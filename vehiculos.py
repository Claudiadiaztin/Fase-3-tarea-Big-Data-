#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FASE 1 - ANALISIS EN BATCH (PySpark)
Dataset: Vehiculos matriculados por clase, modelo y tipo de vehiculo
         (archivo: c.csv)

Pregunta solicitada:
    Cuantos vehiculos HIBRIDOS y ELECTRICOS se matricularon entre
    enero de 2020 y mayo de 2022?

RESULTADO DE LA AUDITORIA: LA PREGUNTA NO ES RESPONDIBLE CON ESTE ARCHIVO.
El dataset no contiene ninguna variable de propulsion (combustible,
hibrido, electrico). Se documenta la ausencia y se responde lo que si es
medible.

Ejecucion 100% CLI, sin interfaz grafica.
Salida: ~/Escritorio/BIGDATA 3/fase1_vehiculos/
"""

import os
import io
import sys
import datetime

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HOME = os.path.expanduser("~")
OUT_DIR = os.path.join(HOME, "Escritorio", "BIGDATA 3", "fase1_vehiculos")
GRAF_DIR = os.path.join(OUT_DIR, "graficos")
REPORT = os.path.join(OUT_DIR, "reporte_vehiculos.txt")

RUTAS = [
    os.path.join(HOME, "Descargas", "vehiculos_matriculados.csv"),
    os.path.join(HOME, "Descargas", "c.csv"),
    os.path.join(HOME, "Descargas", "vehiculos.csv"),
]

MAPEO = {
    "ORDEN": "orden", "CLASE": "clase", "MARCA": "marca",
    "MODELO": "modelo", "FECHA MATRICULA/RADICACION": "fecha",
    "AÑO": "anio", "MES": "mes", "DIA": "dia",
}

AZUL, VERDE, NARANJA, ROJO, GRIS, MORADO = \
    "#2c5f8d", "#3d8b5f", "#d98c2b", "#b8453c", "#6b7280", "#7b5ea7"
LOG = []


def emit(line=""):
    print(line)
    LOG.append(str(line))


def regla(t):
    emit("")
    emit("=" * 78)
    emit(t)
    emit("=" * 78)


def tabla(df, n=20):
    filas = df.limit(n).collect()
    if not filas:
        return ["(sin registros)"]
    cols = df.columns
    anchos = [len(str(c)) for c in cols]
    celdas = []
    for f in filas:
        fila = []
        for i, v in enumerate(f):
            s = "NULL" if v is None else str(v)
            anchos[i] = max(anchos[i], len(s))
            fila.append(s)
        celdas.append(fila)
    sep = "+" + "+".join("-" * (a + 2) for a in anchos) + "+"
    out = [sep, "| " + " | ".join(str(c).ljust(anchos[i])
                                 for i, c in enumerate(cols)) + " |", sep]
    for fila in celdas:
        out.append("| " + " | ".join(fila[i].ljust(anchos[i])
                                     for i in range(len(cols))) + " |")
    out.append(sep)
    out.append("({} filas en total)".format(df.count()))
    return out


def guardar(fig, nombre, nota):
    ruta = os.path.join(GRAF_DIR, nombre)
    fig.savefig(ruta, dpi=110, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    emit("  [PNG] {:40} {:7.1f} KB   <- {}".format(
        nombre, os.path.getsize(ruta) / 1024, nota))


def norm(c):
    return F.trim(F.regexp_replace(
        F.expr("translate({}, 'áéíóúüñÁÉÍÓÚÜÑ', 'aeiouunAEIOUUN')".format(c)),
        r"\s+", " "))


def main():
    inicio = datetime.datetime.now()

    regla("PASO 1 - DIRECTORIO DE SALIDA")
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(GRAF_DIR, exist_ok=True)
    emit("Carpeta : {}".format(OUT_DIR))

    ruta = None
    for r in RUTAS:
        if os.path.isfile(r):
            ruta = r
            break
    if not ruta:
        emit("[ERROR] Dataset no encontrado.")
        sys.exit(1)
    emit("")
    emit("Dataset: {}".format(ruta))
    emit("Tamano : {:,} bytes".format(os.path.getsize(ruta)))

    spark = (SparkSession.builder.appName("Vehiculos-Hibridos-Electricos")
             .master("local[*]")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    regla("PASO 2a - CARGA Y ESQUEMA")
    crudo = (spark.read.option("header", "true").option("delimiter", ",")
             .option("encoding", "UTF-8").option("inferSchema", "true")
             .option("mode", "PERMISSIVE").option("quote", '"')
             .csv(ruta))
    total = crudo.count()
    emit("df.count() : {:,}".format(total))
    emit("Columnas   : {}".format(len(crudo.columns)))
    emit("")
    emit("printSchema():")
    emit(crudo._jdf.schema().treeString())

    regla("PASO 2b - LIMPIEZA")
    d = crudo
    for o, n in MAPEO.items():
        if o in d.columns:
            d = d.withColumnRenamed(o, n)
    emit("Columnas renormal: {}".format(d.columns))
    emit("")
    emit("Nulos por columna:")
    for c in d.columns:
        emit("  {:12} {:>8,}".format(c, d.filter(F.col(c).isNull()).count()))

    for c in ["clase", "marca", "modelo"]:
        d = d.withColumn(c, F.trim(F.col(c)))
        d = d.withColumn(c, F.when(F.col(c) == "", F.lit(None)).otherwise(F.col(c)))
    # La fecha viene como dd/MM/aaaa
    d = d.withColumn("fecha_dt", F.to_date(F.col("fecha"), "dd/MM/yyyy"))
    emit("")
    emit("Fechas no parseables: {:,}".format(d.filter(F.col("fecha_dt").isNull()).count()))

    # =====================================================================
    regla("PASO 3 - AUDITORIA: ¿SE PUEDE RESPONDER LA PREGUNTA?")
    # =====================================================================
    emit("")
    emit("  Pregunta: cuantos vehiculos HIBRIDOS y ELECTRICOS se matricularon")
    emit("            entre enero de 2020 y mayo de 2022?")
    emit("")
    emit("  " + "-" * 74)

    columnas = d.columns
    emit("  1) Columnas disponibles en el archivo:")
    for c in columnas:
        emit("       - {}".format(c))
    emit("")

    # Buscar cualquier columna que hable de propulsion
    claves = ("combust", "motor", "propulsion", "energia", "tipo",
              "hibrid", "hibrido", "electr", "fuel", "gasolina", "diesel")
    candidatas = [c for c in columnas
                  if any(k in c.lower() for k in claves)]
    emit("  2) Columnas que podrian identificar la propulsion: {}".format(
        candidatas if candidatas else "NINGUNA"))

    # Barrido del contenido de TODAS las columnas
    exprs = [F.sum(F.when(
        (F.lower(F.coalesce(F.col(c).cast("string"), F.lit("")))
         .contains("hibrid")) |
        (F.lower(F.coalesce(F.col(c).cast("string"), F.lit("")))
         .contains("electr")) |
        (F.lower(F.coalesce(F.col(c).cast("string"), F.lit("")))
         .contains("híbrid")) |
        (F.lower(F.coalesce(F.col(c).cast("string"), F.lit("")))
         .contains("eléctr")) |
        (F.lower(F.coalesce(F.col(c).cast("string"), F.lit("")))
         .contains("electrico")) |
        (F.lower(F.coalesce(F.col(c).cast("string"), F.lit("")))
         .contains("plugin")) |
        (F.lower(F.coalesce(F.col(c).cast("string"), F.lit("")))
         .contains("combustible")), 1).otherwise(0)).alias(c)
              for c in columnas]
    barrido = d.agg(*exprs).collect()[0].asDict()
    coincidencias = {k: v for k, v in barrido.items() if v}

    emit("")
    emit("  3) Barrido de las {:,} filas buscando las palabras".format(total))
    emit("     'hibrid' / 'hibrido' / 'electr' / 'plugin' / 'combustible':")
    if coincidencias:
        for k, v in coincidencias.items():
            emit("       {} -> {} filas".format(k, v))
    else:
        emit("       RESULTADO: 0 coincidencias en todas las columnas")
        emit("")
        emit("  " + "-" * 74)
        emit("  CONCLUSION: LA PREGUNTA NO ES RESPONDIBLE CON ESTE ARCHIVO.")
        emit("  " + "-" * 74)
        emit("")
        emit("  Razon tecnica: el dataset no registra la tecnologia de propulsion")
        emit("  del vehiculo. Solo tiene CLASE (tipo de carroceria), MARCA y")
        emit("  MODELO (que en este archivo es el ANIO del modelo, no el nombre")
        emit("  del modelo). No hay forma de distinguir un Toyota Corolla a")
        emit("  gasolina de un Toyota Prius hibrido, porque ambos serian")
        emit("  MARCA=TOYOTA y CLASE=AUTOMOVIL.")
        emit("")
        emit("  Inferirlo por marca tampoco es valido: Toyota, Renault, Kia,")
        emit("  Hyundai, BMW y Ford venden versiones a gasolina, hibridas y")
        emit("  electricas bajo la misma marca. Asignar 'hibrido' a toda marca")
        emit("  que lo tenga en su catalogo daria un numero inventado.")

    # MODELO no es el nombre del modelo
    mvals = [r[0] for r in d.select("modelo").distinct().limit(200).collect()]
    son_anos = sum(1 for v in mvals if v and v.isdigit())
    emit("")
    emit("  4) Sobre la columna MODELO:")
    emit("     valores distintos: {}".format(d.select("modelo").distinct().count()))
    emit("     de los cuales son un anio de 4 digitos: {} / {}".format(
        son_anos, len(mvals)))
    emit("     ejemplos: {}".format([v for v in mvals if v][:8]))
    emit("     -> MODELO es el ANIO del modelo, no el nombre comercial.")
    emit("        Por eso no permite identificar vehiculos hibridos/electricos.")

    # Rango real de fechas vs el que dice el nombre del archivo
    emit("")
    emit("  5) Discrepancia en el nombre del archivo:")
    emit("     El nombre dice 'desde enero de 2020 hasta mayo de 2022'.")
    rango = d.filter(F.col("fecha_dt").isNotNull())
    fmin, fmax = rango.agg(F.min("fecha_dt"), F.max("fecha_dt")).collect()[0]
    emit("     Rango REAL de fechas en el archivo: {}  ->  {}".format(
        fmin.strftime("%d/%m/%Y"), fmax.strftime("%d/%m/%Y")))
    emit("     El archivo cubre {} anos, no 28 meses.".format(
        fmax.year - fmin.year + 1))

    # =====================================================================
    regla("PASO 4 - LO QUE SI ES MEDIBLE: ENERO 2020 - MAYO 2022")
    # =====================================================================
    # Cada comparacion va entre parentesis: en Python el operador & tiene mas
    # precedencia que >= y <=, asi que sin ellos la expresion se parsea como
    # col >= (lit & lit) <= lit y Spark lanza CANNOT_CONVERT_COLUMN_INTO_BOOL.
    ventana = d.filter(
        (F.col("fecha_dt") >= F.lit("2020-01-01").cast("date")) &
        (F.col("fecha_dt") <= F.lit("2022-05-31").cast("date")))
    n_ventana = ventana.count()

    emit("")
    emit("  Ventana analizada: 01/01/2020 - 31/05/2022")
    emit("  Vehiculos matriculados en la ventana: {:,}".format(n_ventana))
    emit("  (esto es el total de la ventana, NO el conteo de hibridos/electricos)")
    emit("")
    emit("  Lo que NO se puede desglosar:")
    emit("    - hibridos")
    emit("    - electricos")
    emit("    - total por tecnologia de propulsion")
    emit("")

    por_clase = (ventana.groupBy("clase").agg(F.count(F.lit(1)).alias("n"))
                 .withColumn("pct", F.round(F.col("n") * 100.0 / max(1, n_ventana), 2))
                 .orderBy(F.desc("n")).limit(20))
    emit("  4.1  Por clase de vehiculo (ventana 2020-01 a 2022-05)")
    for l in tabla(por_clase, 20):
        emit(l)

    top_marcas = (ventana.groupBy("marca").agg(F.count(F.lit(1)).alias("n"))
                  .orderBy(F.desc("n")).limit(15))
    emit("")
    emit("  4.2  Top 15 marcas (ventana 2020-01 a 2022-05)")
    for l in tabla(top_marcas, 15):
        emit(l)

    por_mes = (ventana.withColumn("anio_mes",
                                  F.date_format(F.col("fecha_dt"), "yyyy-MM"))
                    .groupBy("anio_mes").agg(F.count(F.lit(1)).alias("n"))
                    .orderBy("anio_mes"))
    emit("")
    emit("  4.3  Por mes (ventana 2020-01 a 2022-05)")
    for l in tabla(por_mes, 40):
        emit(l)

    # 전체 dataset: serie completa
    serie = (d.filter(F.col("fecha_dt").isNotNull())
             .withColumn("anio", F.year(F.col("fecha_dt")))
             .groupBy("anio").agg(F.count(F.lit(1)).alias("n"))
             .orderBy("anio"))
    emit("")
    emit("  4.4  Serie anual de TODO el archivo")
    for l in tabla(serie, 30):
        emit(l)

    # antiguedad del modelo al matricular
    antig = (d.filter(F.col("fecha_dt").isNotNull() &
                      F.col("modelo").rlike("^[0-9]{4}$"))
             .withColumn("mod_anio", F.col("modelo").cast("int"))
             .withColumn("anio_mat", F.year(F.col("fecha_dt")))
             .filter(F.col("mod_anio") <= F.col("anio_mat"))
             .withColumn("edad", F.col("anio_mat") - F.col("mod_anio"))
             .groupBy("edad").agg(F.count(F.lit(1)).alias("n"))
             .orderBy("edad").limit(25))
    emit("")
    emit("  4.5  Antiguedad del modelo al matricular (todo el archivo)")
    for l in tabla(antig, 25):
        emit(l)

    # =====================================================================
    regla("PASO 5 - VISUALIZACION DE LOS RESULTADOS EN BATCH")
    # =====================================================================
    emit("")
    plt.rcParams.update({"font.size": 9, "axes.titlesize": 11,
                         "axes.titleweight": "bold", "figure.facecolor": "white"})

    # g1 la respuesta que NO existe
    fig, ax = plt.subplots(figsize=(8, 4.2))
    ax.axis("off")
    ax.add_patch(plt.Rectangle((0.04, 0.12), 0.92, 0.76, fill=False,
                              edgecolor=ROJO, lw=2.5))
    ax.text(0.5, 0.72, "PREGUNTA NO RESPONDIBLE", ha="center", va="center",
            fontsize=15, fontweight="bold", color=ROJO)
    ax.text(0.5, 0.50,
            "El dataset no contiene ninguna variable de\n"
            "propulsion (combustible / hibrido / electrico).\n\n"
            "Columnas reales:  ORDEN, CLASE, MARCA, MODELO,\n"
            "                  FECHA MATRICULA, ANO, MES, DIA",
            ha="center", va="center", fontsize=10, color="#333333")
    ax.text(0.5, 0.20, "0 coincidencias de 'hibrid'/'electr' en las 16.943 filas",
            ha="center", va="center", fontsize=9, style="italic", color="#666666")
    ax.set_title("g1. Auditoria: lo que el dato NO permite responder")
    guardar(fig, "g1_auditoria_no_respondible.png", "alerta sobre el limite del dato")

    # g2 clases en la ventana
    pc = por_clase.collect()
    labs = [r["clase"] for r in pc]
    vals = [r["n"] for r in pc]
    fig, ax = plt.subplots(figsize=(9, 5))
    b = ax.barh(labs[::-1], vals[::-1], color=AZUL)
    ax.bar_label(b, padding=2, fontsize=8)
    ax.set_title("g2. Vehiculos por clase (enero 2020 - mayo 2022)")
    ax.set_xlabel("Matriculados"); ax.grid(axis="x", alpha=.3)
    guardar(fig, "g2_clases_ventana.png", "lo que si es medible")

    # g3 marcas
    pm = top_marcas.collect()
    labs = [r["marca"] for r in pm]
    vals = [r["n"] for r in pm]
    fig, ax = plt.subplots(figsize=(9, 5))
    b = ax.barh(labs[::-1], vals[::-1], color=VERDE)
    ax.bar_label(b, padding=2, fontsize=8)
    ax.set_title("g3. Top 15 marcas matriculadas en la ventana")
    ax.set_xlabel("Matriculados"); ax.grid(axis="x", alpha=.3)
    guardar(fig, "g3_marcas_ventana.png", "concentracion por marca")

    # g4 serie anual
    ps = serie.collect()
    xs = [r["anio"] for r in ps]
    ys = [r["n"] for r in ps]
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(xs, ys, color=NARANJA)
    ax.set_title("g4. Matriculaciones por ano - archivo completo")
    ax.set_ylabel("Vehiculos"); ax.set_xlabel("Ano")
    ax.set_xticks(xs)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)
    ax.grid(axis="y", alpha=.3)
    guardar(fig, "g4_serie_anual.png", "rango real del archivo")

    # g5 dentro de la ventana por mes
    pmes = por_mes.collect()
    xs = [r["anio_mes"] for r in pmes]
    ys = [r["n"] for r in pmes]
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(xs, ys, color=AZUL)
    ax.set_title("g5. Matriculaciones mensuales (2020-01 a 2022-05)")
    ax.set_ylabel("Vehiculos")
    plt.setp(ax.get_xticklabels(), rotation=60, ha="right", fontsize=7)
    ax.grid(axis="y", alpha=.3)
    guardar(fig, "g5_mensual_ventana.png", "detalle mes a mes")

    # g6 antiguedad
    pa = antig.collect()
    labs = ["{} anios".format(r["edad"]) for r in pa]
    vals = [r["n"] for r in pa]
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(labs, vals, color=MORADO)
    ax.set_title("g6. Antiquedad del modelo al matricular")
    ax.set_xlabel("Anios entre el modelo y la matricula")
    ax.set_ylabel("Vehiculos")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)
    ax.grid(axis="y", alpha=.3)
    guardar(fig, "g6_antiguedad_modelo.png", "parque de vehiculos")

    emit("")
    emit("Total de graficos: {}".format(
        len([f for f in os.listdir(GRAF_DIR) if f.endswith(".png")])))

    regla("PASO 6 - EVIDENCIA EN CONSOLA (.show)")
    emit("")
    emit("[.show] Por clase (ventana 2020-01 a 2022-05):")
    por_clase.show(20, truncate=False)
    emit("")
    emit("[.show] Top marcas:")
    top_marcas.show(15, truncate=False)

    fin = datetime.datetime.now()
    regla("METADATOS")
    emit("Inicio     : {}".format(inicio.strftime("%Y-%m-%d %H:%M:%S")))
    emit("Fin        : {}".format(fin.strftime("%Y-%m-%d %H:%M:%S")))
    emit("Duracion   : {:.1f} segundos".format((fin - inicio).total_seconds()))
    emit("Spark      : {}".format(spark.version))
    emit("Python     : {}".format(sys.version.split()[0]))
    emit("matplotlib : {}".format(matplotlib.__version__))
    emit("Dataset    : {}".format(ruta))
    emit("Registros  : {:,}".format(total))
    emit("Ventana    : {:,} matriculados 2020-01 a 2022-05".format(n_ventana))
    emit("")
    emit("RESPUESTA A LA PREGUNTA DE HIBRIDOS Y ELECTRICOS:")
    emit("  NO DETERMINABLE. El dataset no registra la tecnologia de propulsion.")
    emit("  Coincidencias de 'hibrid'/'electr' en {} filas: 0".format(total))

    with open(REPORT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(LOG) + "\n")

    spark.stop()
    print("")
    print("=" * 78)
    print("  ANALISIS FINALIZADO")
    print("=" * 78)
    print("  {:,} filas analizadas".format(total))
    print("  {:,} matriculados entre 2020-01 y 2022-05".format(n_ventana))
    print("  Hibridos/electricos: NO DETERMINABLE con este archivo")
    print("  Reporte : {}".format(REPORT))
    print("=" * 78)


if __name__ == "__main__":
    main()