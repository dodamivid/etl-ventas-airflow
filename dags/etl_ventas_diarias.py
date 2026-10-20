"""ETL diario de ventas: CSV de las tiendas -> validación -> PostgreSQL.

Se dispara a mano con el parámetro `fecha` (AAAA-MM-DD). Cada corrida debe
reemplazar solo los datos de esa fecha, así que repetirla no puede duplicar filas.

Flujo: extraer -> transformar -> validar_calidad -> cargar -> actualizar_resumen.
validar_calidad no reintenta (retries=0) y cargar corre en una sola transacción.
"""

import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, get_current_context, task

from ventas.reglas import leer_csv, revisar_calidad, separar_ventas

CARPETA_ENTRADA = Path(os.environ.get("VENTAS_CARPETA_ENTRADA", "/opt/airflow/data/entrada"))
CONEXION = "postgres_datapath"

log = logging.getLogger(__name__)

CREAR_TABLAS = """
CREATE TABLE IF NOT EXISTS ventas_limpias (
    id_venta        INTEGER PRIMARY KEY,
    fecha           DATE NOT NULL,
    region          TEXT NOT NULL,
    producto        TEXT NOT NULL,
    cantidad        INTEGER NOT NULL CHECK (cantidad > 0),
    precio_unitario NUMERIC(10, 2) NOT NULL CHECK (precio_unitario > 0),
    total           NUMERIC(12, 2) NOT NULL,
    cargado_en      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ventas_rechazadas (
    fecha_archivo DATE NOT NULL,
    linea         INTEGER NOT NULL,
    id_venta      TEXT,
    motivo        TEXT NOT NULL,
    fila_original TEXT NOT NULL,
    PRIMARY KEY (fecha_archivo, linea)
);
CREATE TABLE IF NOT EXISTS resumen_ventas_diario (
    fecha       DATE NOT NULL,
    region      TEXT NOT NULL,
    num_ventas  INTEGER NOT NULL,
    unidades    INTEGER NOT NULL,
    monto_total NUMERIC(14, 2) NOT NULL,
    PRIMARY KEY (fecha, region)
);
"""


@dag(
    dag_id="etl_ventas_diarias",
    description="Carga diaria de ventas desde CSV a PostgreSQL",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "fecha": Param("2026-10-05", type="string", format="date",
                       description="Fecha del archivo a procesar (AAAA-MM-DD)"),
    },
    default_args={"owner": "equipo-datos", "retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["laboratorio", "ventas", "etl"],
)
def etl_ventas_diarias():

    @task
    def extraer():
        # 1. Toma la fecha de get_current_context()["params"]["fecha"]
        # 2. Arma la ruta CARPETA_ENTRADA / f"ventas_{fecha}.csv" y falla con un
        #    mensaje claro si el archivo no existe
        # 3. Lee las filas con leer_csv y devuelve {"fecha": ..., "filas": [...]}
        fecha = get_current_context()["params"]["fecha"]
        ruta = CARPETA_ENTRADA / f"ventas_{fecha}.csv"
        if not ruta.exists():
            raise FileNotFoundError(f"No existe el archivo de ventas de {fecha}: {ruta}")
        filas = leer_csv(ruta)
        log.info("Archivo %s leído: %d filas", ruta.name, len(filas))
        return {"fecha": fecha, "filas": filas}

    @task
    def transformar(datos):
        # Usa separar_ventas y devuelve un dict con:
        # fecha, leidas (cuántas filas trajo el archivo), limpias y rechazadas
        limpias, rechazadas = separar_ventas(datos["filas"], datos["fecha"])
        log.info("Válidas: %d, rechazadas: %d", len(limpias), len(rechazadas))
        return {
            "fecha": datos["fecha"],
            "leidas": len(datos["filas"]),
            "limpias": limpias,
            "rechazadas": rechazadas,
        }

    # retries=0: si el archivo viene sucio, reintentar da el mismo resultado
    @task(retries=0)
    def validar_calidad(resultado):
        # Llama a revisar_calidad. Si pasa, devuelve el mismo `resultado` para
        # que siga a la carga. ¿Tiene sentido que esta tarea reintente?
        tasa = revisar_calidad(resultado["leidas"], len(resultado["rechazadas"]))
        log.info("Control de calidad aprobado: %.1f%% de filas rechazadas", tasa * 100)
        return resultado

    @task
    def cargar(resultado):
        # Todo en UNA transacción con PostgresHook(postgres_conn_id=CONEXION):
        #   - CREAR_TABLAS
        #   - borrar lo que ya exista de esa fecha en ventas_limpias y ventas_rechazadas
        #   - insertar con parámetros (%s), nunca armando el SQL con f-strings
        #   - comprobar que en la tabla quedó la misma cantidad que transformaste
        # Devuelve {"fecha": ..., "cargadas": n, "rechazadas": m}
        fecha = resultado["fecha"]
        limpias = resultado["limpias"]
        rechazadas = resultado["rechazadas"]

        hook = PostgresHook(postgres_conn_id=CONEXION)
        conexion = hook.get_conn()
        try:
            # "with conexion" = una sola transacción: commit si todo sale bien,
            # rollback si algo lanza una excepción (la fecha queda como estaba).
            with conexion:
                with conexion.cursor() as cur:
                    cur.execute(CREAR_TABLAS)
                    cur.execute("DELETE FROM ventas_limpias WHERE fecha = %s", (fecha,))
                    cur.execute("DELETE FROM ventas_rechazadas WHERE fecha_archivo = %s", (fecha,))

                    cur.executemany(
                        """
                        INSERT INTO ventas_limpias
                            (id_venta, fecha, region, producto, cantidad, precio_unitario, total)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (v["id_venta"], v["fecha"], v["region"], v["producto"],
                             v["cantidad"], v["precio_unitario"], v["total"])
                            for v in limpias
                        ],
                    )
                    cur.executemany(
                        """
                        INSERT INTO ventas_rechazadas
                            (fecha_archivo, linea, id_venta, motivo, fila_original)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        [
                            (fecha, r["linea"], r["id_venta"], r["motivo"], r["fila_original"])
                            for r in rechazadas
                        ],
                    )

                    # Comprobación dentro de la transacción: si no cuadra, rollback
                    cur.execute("SELECT COUNT(*) FROM ventas_limpias WHERE fecha = %s", (fecha,))
                    en_tabla = cur.fetchone()[0]
                    if en_tabla != len(limpias):
                        raise RuntimeError(
                            f"Se esperaban {len(limpias)} ventas de {fecha} y hay {en_tabla}"
                        )
        finally:
            conexion.close()

        log.info("Cargadas %d ventas y %d rechazadas de %s", len(limpias), len(rechazadas), fecha)
        return {"fecha": fecha, "cargadas": len(limpias), "rechazadas": len(rechazadas)}

    @task
    def actualizar_resumen(carga):
        # Recalcula resumen_ventas_diario SOLO para la fecha cargada, agrupando
        # ventas_limpias por región: número de ventas, unidades y monto total
        fecha = carga["fecha"]

        hook = PostgresHook(postgres_conn_id=CONEXION)
        conexion = hook.get_conn()
        try:
            with conexion:
                with conexion.cursor() as cur:
                    cur.execute("DELETE FROM resumen_ventas_diario WHERE fecha = %s", (fecha,))
                    cur.execute(
                        """
                        INSERT INTO resumen_ventas_diario
                            (fecha, region, num_ventas, unidades, monto_total)
                        SELECT fecha, region, COUNT(*), SUM(cantidad), SUM(total)
                        FROM ventas_limpias
                        WHERE fecha = %s
                        GROUP BY fecha, region
                        """,
                        (fecha,),
                    )
                    regiones = cur.rowcount
        finally:
            conexion.close()

        log.info("Resumen de %s actualizado: %d regiones", fecha, regiones)
        return {"fecha": fecha, "regiones": regiones}

    actualizar_resumen(cargar(validar_calidad(transformar(extraer()))))


etl_ventas_diarias()
