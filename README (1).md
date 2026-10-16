# Analisis 3 — Vehículos matriculados: marca KIA y auditoría del dataset

**Dataset:** Vehículos matriculados por clase, modelo y tipo de vehículo
**Registro:** 16.943 filas × 8 columnas · UTF-8 · delimitador coma
**Rango real de fechas:** 01/01/2000 → 10/06/2022 (no 2020–2022 como indica el nombre)
**Tecnología:** Apache Spark 3.5.1 · Python 3.10.12 · Java 17

Este directorio contiene **dos análisis sobre el mismo dataset**, porque el
dataset resultó responder una pregunta y no poder responder otra.

---

# 3a. ¿Cuántos vehículos KIA se matricularon entre 2020 y mayo de 2022?

## Respuesta

```
============================================================ 34 VEHICULOS KIA
============================================================

  Ventana: 01/01/2020 - 31/05/2022

    KIA (exacto)           :      34
    KIAMASTER (variante)   :       1
    ------------------------------------------
    Total si se incluyen   :      35
```

## Contexto

| Concepto | Valor |
|---|---:|
| Total de vehículos en la ventana | 1.421 |
| KIA sobre el total de la ventana | 2,39 % |
| Posición de KIA entre marcas | **#12 de 84** |
| KIA en todo el archivo (2000–2022) | 200 |
| Porcentaje del parque KIA en la ventana | 17,0 % |

**Solo el 17 % de los KIA del archivo caen en la ventana analizada.** La marca
aparece sobre todo en años anteriores.

## Control de variantes de escritura

La marca aparece con dos cadenas distintas en todo el archivo:

| Cadena | Registros (archivo completo) | En la ventana |
|---|---:|---:|
| `KIA` | 200 | 34 |
| `KIAMASTER` | 2 | 1 |

Se reportan **por separado**. Sumarlas sería una decisión del analista, no del
dato: `KIAMASTER` es una denominación distinta en el registro. Si el enunciado
pide estrictamente la marca KIA, la respuesta es **34**; si incluye variantes de
la familia, **35**. Declarar el criterio evita que el número se discuta.

## Desagregados disponibles

En `reporte_kia.txt`:

- **4.1** KIA por clase de vehículo
- **4.2** KIA por año del modelo
- **4.3** KIA matriculados mes a mes (2020-01 a 2022-05)
- **4.4** KIA por año de matrícula
- **4.5** Ranking de las 15 marcas más matriculadas en la ventana
- **4.6** Contexto de KIA en el archivo completo frente a la ventana

## Gráficos

| Archivo | Qué muestra |
|---|---|
| `kia_g1_kia_respuesta.png` | La respuesta central (KIA / KIAMASTER / resto) |
| `kia_g2_kia_por_clase.png` | Qué tipo de vehículo compra Kia |
| `kia_g3_kia_mensual.png` | Evolución mes a mes en la ventana |
| `kia_g4_ranking_marcas.png` | Top 15 marcas, con KIA destacada |
| `kia_g5_kia_ano_modelo.png` | Año del modelo del parque KIA |
| `kia_g6_kia_contexto.png` | KIA fuera de la ventana frente a dentro |

---

# 3b. ¿Cuántos híbridos y eléctricos se matricularon?

## Respuesta

```
NO DETERMINABLE CON ESTE ARCHIVO
```

## Evidencia

El script reproduce la auditoría dentro de la ejecución:

```
1) Columnas disponibles:
   orden, clase, marca, modelo, fecha, anio, mes, dia

2) Columnas que podrían identificar la propulsion: NINGUNA

3) Barrido de las 16,943 filas buscando las palabras
   'hibrid' / 'hibrido' / 'electr' / 'plugin' / 'combustible':
   RESULTADO: 0 coincidencias en todas las columnas
```

## Por qué no se puede calcular

### Razón 1 — la columna `MODELO` no es el nombre del modelo

Sus 84 valores distintos son **todos años de cuatro dígitos**:

```
['1988', '2001', '1998', '1986', '1959', '1971', '2002', '1957']
```

`MODELO` es el **año del modelo**, no el nombre comercial. Por eso no permite
distinguir un Toyota Corolla a gasolina de un Toyota Prius híbrido: en este
archivo ambos serían `MARCA=TOYOTA`, `CLASE=AUTOMOVIL`, `MODELO=2020`.

### Razón 2 — deducirlo por marca tampoco es válido

Toyota, Renault, Kia, Hyundai, BMW y Ford venden versiones a gasolina,
híbridas y eléctricas **bajo la misma marca**. Asignar la etiqueta "híbrido" a
toda marca que lo tenga en su catálogo produciría un número inventado.

## Discrepancia adicional en el nombre del archivo

| | |
|---|---|
| Dice el nombre | enero 2020 → mayo 2022 (28 meses) |
| Rango real | **01/01/2000 → 10/06/2022 (23 años)** |

Es probable que se haya descargado una versión distinta a la del catálogo, o
que la descarga esté incompleta.

## Qué haría falta

La fuente que sí registra el tipo de combustible es **RUNTASK** (Registro Único
Nacional de Tránsito), publicada por la Agencia Nacional de Seguridad Vial.

## Lo que sí se analizo en la ventana

Aunque la pregunta original no es respondible, el dataset sí permite medir el
volumen total de la ventana:

**1.421 vehículos matriculados entre 01/01/2020 y 31/05/2022**

| Clase | n | % |
|---|---:|---:|
| Motocicleta | 453 | 31,88 |
| Automóvil | 425 | 29,91 |
| Camioneta | 181 | 12,74 |
| Campero | 142 | 9,99 |
| Camión | 104 | 7,32 |
| Tracto camión | 64 | 4,50 |
| Volqueta | 13 | 0,91 |
| Motocarro | 13 | 0,91 |
| Microbus | 7 | 0,49 |
| Remolque | 6 | 0,42 |
| Bus | 5 | 0,35 |
| Buseta | 5 | 0,35 |

Ese 1.421 es el **total de la ventana**, no el conteo de híbridos/eléctricos.

## Gráficos de la auditoría

| Archivo | Qué muestra |
|---|---|
| `veh_g1_auditoria_no_respondible.png` | El límite del dato, de forma explícita |
| `veh_g2_clases_ventana.png` | Vehículos por clase en la ventana |
| `veh_g3_marcas_ventana.png` | Top 15 marcas de la ventana |
| `veh_g4_serie_anual.png` | Rango real del archivo completo |
| `veh_g5_mensual_ventana.png` | Detalle mes a mes |
| `veh_g6_antiguedad_modelo.png` | Parque y antigüedad del modelo |

---

# Cómo ejecutar

```bash
pip install -r ../requirements.txt

python3 kia.py         # análisis de la marca KIA
python3 vehiculos.py   # auditoría del dataset
```

**Salidas:**

- `~/Escritorio/BIGDATA 3/fase1_kia/` → `reporte_kia.txt`
- `~/Escritorio/BIGDATA 3/fase1_vehiculos/` → `reporte_vehiculos.txt`

---

# Problemas encontrados

### 1. Precedencia de operadores en PySpark

```python
# MAL: se parsea como  col >= (lit & lit) <= lit
ventana = d.filter(F.col("fecha_dt") >= F.lit(INI) &
                   F.col("fecha_dt") <= F.lit(FIN))

# BIEN: cada comparación entre paréntesis
ventana = d.filter((F.col("fecha_dt") >= F.lit(INI).cast("date")) &
                   (F.col("fecha_dt") <= F.lit(FIN).cast("date")))
```

En Python el operador `&` tiene más precedencia que `>=` y `<=`. Sin los
paréntesis Spark lanza `CANNOT_CONVERT_COLUMN_INTO_BOOL`.

### 2. `FECHA MATRICULA/RADICACION` viene como texto

El formato es `dd/MM/aaaa`, no un timestamp. Se parsea con:

```python
d = d.withColumn("fecha_dt", F.to_date(F.col("fecha"), "dd/MM/yyyy"))
```

### 3. `int` no es subscriptable al iterar `collect()`

```python
# MAL: c es un int, no una Row
for u, c in df.collect():
    c["filas"]

# BIEN: iterar las Row
for r in df.collect():
    r["filas"]
```

---

## Sobre la auditoría

Documentar **por qué una pregunta no es respondible** es parte del análisis, no
una limitación. Demuestra que se revisó el esquema antes de agregar, y evita
presentar un número inventado. El gráfico `veh_g1_auditoria_no_respondible.png`
está pensado para usarse directamente en la presentación.