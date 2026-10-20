import pytest

from ventas.reglas import leer_csv, revisar_calidad, separar_ventas, validar_fila

FECHA = "2026-10-05"


def fila(**cambios):
    """Fila válida de ejemplo; pásale solo lo que quieras cambiar."""
    base = {
        "id_venta": "70001",
        "fecha": FECHA,
        "region": "norte",
        "producto": "Mouse",
        "cantidad": "3",
        "precio_unitario": "45.50",
    }
    base.update(cambios)
    return base


# Estas cuatro pruebas son de ejemplo. Van a fallar hasta que completes reglas.py.

def test_fila_valida_calcula_el_total():
    registro, motivo = validar_fila(fila(), FECHA)
    assert motivo is None
    assert registro["total"] == 136.5


def test_region_desconocida_se_rechaza():
    registro, motivo = validar_fila(fila(region="Lima"), FECHA)
    assert registro is None
    assert motivo == "region_invalida"


def test_duplicado_conserva_la_primera_aparicion():
    limpias, rechazadas = separar_ventas([fila(), fila(cantidad="9")], FECHA)
    assert len(limpias) == 1
    assert limpias[0]["cantidad"] == 3
    assert rechazadas[0]["motivo"] == "id_duplicado"


def test_calidad_rechaza_sobre_el_umbral():
    with pytest.raises(ValueError):
        revisar_calidad(10, 3)


# TODO: agrega tus pruebas. Como mínimo deben cubrir:
#   - región y producto normalizados ("  SUR " -> "Sur", " AURICULARES" -> "Auriculares")
#   - cada motivo de rechazo: id_invalido, fecha_distinta, producto_vacio,
#     cantidad_invalida (vacía, 0, negativa, "dos", "1.5") y precio_invalido
#     (vacío, "0.00", "0.004", con coma decimal "45,50", "1e3")
#   - una línea con columnas de más: 45,50 SIN comillas en el CSV
#     (escribe un archivo con tmp_path y léelo con leer_csv)
#   - un duplicado cuya primera aparición fue rechazada por otro motivo
#   - que se reporte solo el primer problema cuando una fila tiene varios
#   - que separar_ventas no pierda filas: limpias + rechazadas == leídas
#   - que revisar_calidad acepte una tasa exactamente igual al umbral
#   - que revisar_calidad falle con un archivo vacío
# Tip: @pytest.mark.parametrize te ahorra repetir la misma prueba muchas veces.


# ---------- Paso 3.1: normalización y motivos de rechazo ----------

def test_region_y_producto_se_normalizan():
    registro, motivo = validar_fila(fila(region="  SUR ", producto=" AURICULARES"), FECHA)
    assert motivo is None
    assert registro["region"] == "Sur"
    assert registro["producto"] == "Auriculares"


@pytest.mark.parametrize(
    "cambios, motivo_esperado",
    [
        ({"id_venta": "abc"}, "id_invalido"),
        ({"id_venta": "0"}, "id_invalido"),
        ({"id_venta": ""}, "id_invalido"),
        ({"fecha": "2026-10-06"}, "fecha_distinta"),
        ({"producto": "   "}, "producto_vacio"),
        ({"cantidad": ""}, "cantidad_invalida"),
        ({"cantidad": "0"}, "cantidad_invalida"),
        ({"cantidad": "-2"}, "cantidad_invalida"),
        ({"cantidad": "dos"}, "cantidad_invalida"),
        ({"cantidad": "1.5"}, "cantidad_invalida"),
        ({"precio_unitario": ""}, "precio_invalido"),
        ({"precio_unitario": "0.00"}, "precio_invalido"),
        ({"precio_unitario": "0.004"}, "precio_invalido"),
        ({"precio_unitario": "45,50"}, "precio_invalido"),
        ({"precio_unitario": "1e3"}, "precio_invalido"),
    ],
)
def test_motivos_de_rechazo(cambios, motivo_esperado):
    registro, motivo = validar_fila(fila(**cambios), FECHA)
    assert registro is None
    assert motivo == motivo_esperado


# ---------- Paso 3.2: archivo, duplicados, orden y calidad ----------

def test_linea_con_coma_decimal_sin_comillas_tiene_columnas_de_mas(tmp_path):
    ruta = tmp_path / "ventas.csv"
    ruta.write_text(
        "id_venta,fecha,region,producto,cantidad,precio_unitario\n"
        "70001,2026-10-05,norte,Mouse,3,45,50\n",
        encoding="utf-8",
    )
    filas = leer_csv(ruta)
    limpias, rechazadas = separar_ventas(filas, FECHA)
    assert limpias == []
    assert rechazadas[0]["motivo"] == "columnas_incorrectas"
    assert rechazadas[0]["linea"] == 2


def test_duplicado_de_una_fila_rechazada_tambien_se_rechaza():
    filas = [fila(region="Lima"), fila()]
    limpias, rechazadas = separar_ventas(filas, FECHA)
    assert limpias == []
    assert [r["motivo"] for r in rechazadas] == ["region_invalida", "id_duplicado"]


def test_ids_con_ceros_a_la_izquierda_son_el_mismo_id():
    filas = [fila(id_venta="70001"), fila(id_venta="070001")]
    limpias, rechazadas = separar_ventas(filas, FECHA)
    assert len(limpias) == 1
    assert rechazadas[0]["motivo"] == "id_duplicado"


def test_solo_se_reporta_el_primer_problema():
    registro, motivo = validar_fila(
        fila(region="Lima", cantidad="0", precio_unitario="x"), FECHA
    )
    assert registro is None
    assert motivo == "region_invalida"


def test_separar_ventas_no_pierde_filas():
    filas = [
        fila(id_venta="1"),
        fila(id_venta="2", region="Lima"),
        fila(id_venta="1"),
        fila(id_venta="x"),
    ]
    limpias, rechazadas = separar_ventas(filas, FECHA)
    assert len(limpias) + len(rechazadas) == len(filas)


def test_calidad_acepta_una_tasa_igual_al_umbral():
    assert revisar_calidad(10, 2) == pytest.approx(0.2)


def test_calidad_falla_con_un_archivo_vacio():
    with pytest.raises(ValueError):
        revisar_calidad(0, 0)
