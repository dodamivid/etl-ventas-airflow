# ETL de ventas diarias con Airflow

Laboratorio de los módulos 6 y 7: archivos CSV de ventas -> validación -> PostgreSQL, orquestado con Airflow 3.3.2 y con pruebas automáticas en GitHub Actions.

Cada noche la caja deja un CSV con las ventas del día. El pipeline lee el archivo de una fecha, separa las ventas válidas de las rechazadas (guardando el motivo), se detiene si el archivo viene demasiado sucio (más del 20 % de filas rechazadas) y deja los datos en PostgreSQL sin duplicar ventas aunque se repita la fecha.

```
extraer -> transformar -> validar_calidad -> cargar -> actualizar_resumen
```

## Cómo levantarlo

Requisitos: Docker Desktop encendido y Python 3 para el script de preparación.

```
python preparar_entorno.py
docker compose build
docker compose up airflow-init
docker compose up -d --wait
```

1. `preparar_entorno.py` crea el archivo `.env` con las claves.
2. Abre http://localhost:8090 y entra con `airflow` / `airflow`.
3. Si el puerto 8090 está ocupado o bloqueado, agrega `AIRFLOW_PORT=8090` (u otro) en `.env` y vuelve a ejecutar `docker compose up -d --wait`.
4. Activa el DAG `etl_ventas_diarias` y dispáralo con **Trigger**, indicando la fecha (`2026-10-05`, `2026-10-06`, `2026-10-07`).
5. Para revisar la base:
   `docker compose exec -T datapath-db psql -U dp_user_pg -d datapath_pg < sql/verificacion.sql`

`docker compose down` detiene todo y conserva los datos; `docker compose down -v` también borra las bases.

## Cómo correr las pruebas

Dentro del contenedor (entorno idéntico al de Airflow):

```
docker compose exec airflow-scheduler pytest
```

Solo las reglas de validación, sin Airflow (basta con `pip install pytest`):

```
python -m pytest tests/test_reglas.py
```

En GitHub Actions corren automáticamente con Python 3.12 en cada push a `main` y en cada pull request hacia `main`.

## Respuestas

**1. ¿Por qué falló la corrida del 7 de octubre? ¿Qué habría quedado en la base si no existiera `validar_calidad`?**

El archivo original traía los precios con coma decimal entre comillas (`"45,50"` en lugar de `45.50`), que es lo que pasa cuando el CSV se abre y se guarda con Excel en configuración regional en español. La regla de precio solo acepta un punto decimal, así que 22 filas se rechazaron como `precio_invalido`, más una por `cantidad_invalida` y otra por `region_invalida`: 24 de 41 filas, un 59 %, muy por encima del límite del 20 %. Sin `validar_calidad`, `cargar` habría insertado solo las 17 ventas válidas y la corrida habría terminado en verde, con más de la mitad de las ventas del día ausentes y un `resumen_ventas_diario` con totales muy por debajo de los reales, sin que nadie lo notara. Al corregir el archivo, el rechazo bajó a 2 de 41 y la corrida pasó.

**2. ¿Qué hace que repetir una fecha no duplique ventas? ¿Qué pasaría si `cargar` solo hiciera `INSERT`?**

`cargar` trabaja en una sola transacción: primero borra todo lo que ya existe de esa fecha en `ventas_limpias` y `ventas_rechazadas` y después inserta lo nuevo, así que correr la misma fecha dos veces deja la base en el mismo estado (la operación es idempotente). Si algo falla a la mitad, la transacción se revierte y la fecha queda como estaba. Con solo `INSERT`, repetir una fecha chocaría con la llave primaria de `id_venta` y la tarea fallaría; y si la tabla no tuviera esa llave, las ventas quedarían duplicadas y el resumen duplicaría sus montos. Además, la corrida corregida del 7 de octubre no podría reemplazar los datos anteriores.

**3. ¿Por qué las pruebas del CI pueden correr sin PostgreSQL ni el scheduler? Nombra una cosa que esas pruebas no comprueban.**

Porque `reglas.py` es Python puro y no importa nada de Airflow ni de la base: las pruebas le pasan diccionarios y comprueban lo que devuelve, todo en memoria. Las pruebas de integridad del DAG solo cargan los archivos de `dags/` con `DagBag` para revisar que importen, que no haya ciclos y que la configuración sea la esperada; no ejecutan ninguna tarea. Lo que esas pruebas no comprueban es el SQL de `cargar` y `actualizar_resumen` contra una base real (la transacción, el borrado y la reinserción), ni que el scheduler ejecute las tareas de punta a punta; eso solo se ve al correr el DAG en Airflow con PostgreSQL.

**4. ¿Por qué tiene sentido que `validar_calidad` no reintente y que `extraer` sí?**

`validar_calidad` evalúa el mismo archivo con las mismas reglas, así que reintentar daría exactamente el mismo resultado y solo haría perder tiempo antes de avisar del problema; el archivo se arregla en el origen, no esperando. `extraer`, en cambio, puede fallar por causas pasajeras, como que la carpeta compartida no responda un momento o que el archivo todavía se esté escribiendo, y en esos casos un segundo intento sí puede funcionar.

## Pull request

Cambio que rompe a propósito las pruebas (aceptar precio 0), con el CI en rojo y luego en verde tras revertirlo: https://github.com/dodamivid/etl-ventas-airflow/pull/1

## Evidencias

En la carpeta `evidencias/`:

- `corridas_dag.png`: lista de las cinco corridas del DAG (incluida la que falló).
- `verificacion.txt`: salida de `sql/verificacion.sql` (126 ventas sin ids repetidos y 9 filas de resumen).
- `actions.png`: pestaña Actions con ejecuciones en rojo y en verde.
