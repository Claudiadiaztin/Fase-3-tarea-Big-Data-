#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FASE 1 - ANALISIS EN BATCH (PySpark)
Dataset: Vehiculos matriculados por clase, modelo y tipo de vehiculo

Pregunta:  cuantos vehiculos de la marca KIA se matricularon entre
           enero de 2020 y mayo de 2022?

Ventana:  01/01/2020 - 31/05/2022

Ejecucion 100% CLI, sin interfaz grafica.
Salida: ~/Escritorio/BIGDATA 3/fase1_kia/
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
OUT_DIR = os.path.join(HOME, "Escritorio", "BIGDATA 3", "fase1_kia")
GRAF_DIR = os.path.join(OUT_DIR, "graficos")
REPORT = os.path.join(OUT_DIR, "reporte_kia.txt")

RUTAS = [
    os.path.join(HOME, "Descargas", "vehiculos_matriculados.csv"),
    os.path.join(HOME, "Descargas", "c.csv"),
]

MAPEO = {
    "ORDEN": "orden", "CLASE": "clase", "MARCA": "marca",
    "MODELO": "modelo", "FECHA MATRICULA/RADICACION": "fecha",
    "AÑO": "anio", "MES": "mes", "DIA": "dia",
}
INI, FIN = "2020-01-01", "2022-05-31"

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

    spark = (SparkSession.builder.appName("KIA-Matriculaciones-2020-2022")
             .master("local[*]")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # ---------------------------------------------------------------- PASO 2
    regla("PASO 2 - CARGA, ESQUEMA Y LIMPIEZA")
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

    d = crudo
    for o, n in MAPEO.items():
        if o in d.columns:
            d = d.withColumnRenamed(o, n)
    for c in ["clase", "marca", "modelo"]:
        d = d.withColumn(c, F.trim(F.col(c)))
        d = d.withColumn(c, F.when(F.col(c) == "", F.lit(None)).otherwise(F.col(c)))
    for c in ["clase", "marca"]:
        d = d.withColumn(c, norm(c))
    d = d.withColumn("fecha_dt", F.to_date(F.col("fecha"), "dd/MM/yyyy"))
    emit("")
    emit("Filas totales tras limpieza : {:,}".format(d.count()))

    # ---------------------------------------------------------------- PASO 3
    regla("PASO 3 - PREGUNTA CENTRAL: MARCA KIA")
    emit("")
    emit("  Vehiculos de la marca KIA matriculados entre")
    emit("  01/01/2020 y 31/05/2022")
    emit("")

    # Cada comparacion entre parentesis: & tiene mas precedencia que >= en Python
    ventana = d.filter(
        (F.col("fecha_dt") >= F.lit(INI).cast("date")) &
        (F.col("fecha_dt") <= F.lit(FIN).cast("date")))
    n_ventana = ventana.count()

    # Variantes de escritura de la marca
    variantes = (d.filter(F.upper(F.col("marca")).contains("KIA"))
                 .groupBy("marca").agg(F.count(F.lit(1)).alias("n"))
                 .orderBy(F.desc("n")))
    emit("  Control previo: como aparece escrita la marca en TODO el archivo")
    for r in variantes.collect():
        emit("    {:<14}{:>7,}".format(r["marca"], r["n"]))
    emit("")
    emit("  Se reportan por separado KIA y KIAMASTER porque son cadenas")
    emit("  distintas. Sumarlas seria una decision del analista, no del dato.")
    emit("")

    kia = ventana.filter(F.upper(F.col("marca")) == "KIA")
    N_KIA = kia.count()
    kiamaster = ventana.filter(F.upper(F.col("marca")) == "KIAMASTER")
    N_KM = kiamaster.count()

    emit("  " + "=" * 60)
    emit("        {:,} VEHICULOS KIA".format(N_KIA))
    emit("  " + "=" * 60)
    emit("")
    emit("  Ventana 01/01/2020 - 31/05/2022")
    emit("    KIA (exacto)           : {:>7,}".format(N_KIA))
    emit("    KIAMASTER (variante)   : {:>7,}".format(N_KM))
    emit("    ------------------------------------------")
    emit("    Total si se incluyen   : {:>7,}".format(N_KIA + N_KM))
    emit("")
    emit("  Base de comparacion:")
    emit("    Vehiculos en la ventana        : {:>9,}".format(n_ventana))
    emit("    KIA sobre el total de ventana  : {:>8.2f}%".format(
        N_KIA * 100.0 / max(1, n_ventana)))
    emit("    KIA sobre toda la marca (archivo completo): {:>7,}".format(
        d.filter(F.upper(F.col("marca")) == "KIA").count()))

    # posicion en el ranking
    ranking = (ventana.groupBy("marca").agg(F.count(F.lit(1)).alias("n"))
                     .orderBy(F.desc("n")))
    fila_kia = ranking.filter(F.upper(F.col("marca")) == "KIA").first()
    if fila_kia:
        mayores = ranking.filter(F.col("n") > fila_kia["n"]).count()
        emit("    Posicion de KIA entre marcas : #{:>7,} de {:,}".format(
            mayores + 1, ranking.count()))

    # ---------------------------------------------------------------- PASO 4
    regla("PASO 4 - DESAGREGACION DE KIA")

    kia_clase = (kia.groupBy("clase").agg(F.count(F.lit(1)).alias("n"))
                     .withColumn("pct", F.round(F.col("n") * 100.0 / max(1, N_KIA), 2))
                     .orderBy(F.desc("n")))
    emit("")
    emit("4.1  KIA por clase de vehiculo")
    for l in tabla(kia_clase, 20):
        emit(l)

    kia_modelo = (kia.groupBy("modelo").agg(F.count(F.lit(1)).alias("n"))
                      .orderBy(F.desc("n")).limit(20))
    emit("")
    emit("4.2  KIA por anio del modelo")
    for l in tabla(kia_modelo, 20):
        emit(l)

    kia_mes = (kia.withColumn("anio_mes",
                              F.date_format(F.col("fecha_dt"), "yyyy-MM"))
                    .groupBy("anio_mes").agg(F.count(F.lit(1)).alias("n"))
                    .orderBy("anio_mes"))
    emit("")
    emit("4.3  KIA matriculados mes a mes")
    for l in tabla(kia_mes, 40):
        emit(l)

    kia_anio = (kia.withColumn("anio", F.year(F.col("fecha_dt")))
                   .groupBy("anio").agg(F.count(F.lit(1)).alias("n"))
                   .orderBy("anio"))
    emit("")
    emit("4.4  KIA por ano de matricula")
    for l in tabla(kia_anio, 10):
        emit(l)

    emit("")
    emit("4.5  Ranking de las 15 marcas mas matriculadas en la ventana")
    for l in tabla(ranking.limit(15), 15):
        emit(l)

    # KIA fuera de la ventana, para contexto
    kia_total_archivo = d.filter(F.upper(F.col("marca")) == "KIA").count()
    emit("")
    emit("4.6  Contexto de KIA en el archivo completo (2000-2022)")
    emit("     Total KIA matriculados en todo el archivo : {:>7,}".format(
        kia_total_archivo))
    emit("     De esos, dentro de la ventana 2020-05/2022 : {:>7,}".format(N_KIA))
    emit("     Porcentaje del total KIA en la ventana     : {:>7.2f}%".format(
        N_KIA * 100.0 / max(1, kia_total_archivo)))

    # ---------------------------------------------------------------- PASO 5
    regla("PASO 5 - VISUALIZACION DE LOS RESULTADOS EN BATCH")
    emit("")
    plt.rcParams.update({"font.size": 9, "axes.titlesize": 11,
                         "axes.titleweight": "bold", "figure.facecolor": "white"})

    # g1 la respuesta
    fig, ax = plt.subplots(figsize=(7, 4.2))
    labs = ["KIA", "KIAMASTER", "Resto de marcas"]
    vals = [N_KIA, N_KM, n_ventana - N_KIA - N_KM]
    b = ax.bar(labs, vals, color=[AZUL, NARANJA, GRIS])
    ax.bar_label(b, padding=3, fontsize=11, fontweight="bold")
    ax.set_title("g1. Vehiculos matriculados 01/2020 - 05/2022")
    ax.set_ylabel("Vehiculos")
    ax.set_ylim(0, max(vals) * 1.2)
    ax.text(0, N_KIA * 0.55, "{:.2f}% del total".format(
        N_KIA * 100.0 / max(1, n_ventana)), ha="center", va="center",
        fontsize=10, color="white", fontweight="bold")
    ax.grid(axis="y", alpha=.3)
    guardar(fig, "g1_kia_respuesta.png", "la respuesta central")

    # g2 por clase
    pc = kia_clase.collect()
    labs = [r["clase"] for r in pc]
    vals = [r["n"] for r in pc]
    fig, ax = plt.subplots(figsize=(8, 4))
    b = ax.barh(labs[::-1], vals[::-1], color=VERDE)
    ax.bar_label(b, padding=2, fontsize=9)
    ax.set_title("g2. KIA por clase de vehiculo")
    ax.set_xlabel("Matriculados"); ax.grid(axis="x", alpha=.3)
    guardar(fig, "g2_kia_por_clase.png", "que tipo de vehiculo compra Kia")

    # g3 mensual
    pm = kia_mes.collect()
    xs = [r["anio_mes"] for r in pm]
    ys = [r["n"] for r in pm]
    fig, ax = plt.subplots(figsize=(10, 3.8))
    ax.bar(xs, ys, color=AZUL)
    ax.set_title("g3. KIA matriculados mes a mes (2020-01 a 2022-05)")
    ax.set_ylabel("Vehiculos")
    plt.setp(ax.get_xticklabels(), rotation=60, ha="right", fontsize=7)
    ax.grid(axis="y", alpha=.3)
    guardar(fig, "g3_kia_mensual.png", "evolucion mensual")

    # g4 ranking de marcas
    pr = ranking.limit(15).collect()
    labs = [r["marca"] for r in pr]
    vals = [r["n"] for r in pr]
    cols = [ROJO if str(l).upper() == "KIA" else GRIS for l in labs]
    fig, ax = plt.subplots(figsize=(9, 5))
    b = ax.barh(labs[::-1], vals[::-1], color=cols[::-1])
    ax.bar_label(b, padding=2, fontsize=8)
    ax.set_title("g4. Top 15 marcas en la ventana (KIA en rojo)")
    ax.set_xlabel("Matriculados"); ax.grid(axis="x", alpha=.3)
    guardar(fig, "g4_ranking_marcas.png", "posicion de KIA")

    # g5 anio del modelo
    pmo = kia_modelo.limit(15).collect()
    labs = [r["modelo"] for r in pmo]
    vals = [r["n"] for r in pmo]
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(labs, vals, color=MORADO)
    ax.set_title("g5. KIA por anio del modelo")
    ax.set_xlabel("Anio del modelo"); ax.set_ylabel("Vehiculos")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)
    ax.grid(axis="y", alpha=.3)
    guardar(fig, "g5_kia_ano_modelo.png", "antiguedad del parque KIA")

    # g6 KIA en el archivo completo vs ventana
    fig, ax = plt.subplots(figsize=(7, 4))
    labs = ["Fuera de la\nventana", "Dentro de la\nventana 2020-2022"]
    vals = [kia_total_archivo - N_KIA, N_KIA]
    b = ax.bar(labs, vals, color=[GRIS, AZUL])
    ax.bar_label(b, padding=3, fontsize=11, fontweight="bold")
    ax.set_title("g6. KIA en todo el archivo vs la ventana analizada")
    ax.set_ylabel("Vehiculos"); ax.set_ylim(0, max(vals) * 1.2)
    ax.grid(axis="y", alpha=.3)
    guardar(fig, "g6_kia_contexto.png", "cuanto del total KIA cae en la ventana")

    emit("")
    emit("Total de graficos: {}".format(
        len([f for f in os.listdir(GRAF_DIR) if f.endswith(".png")])))

    # ---------------------------------------------------------------- PASO 6
    regla("PASO 6 - EVIDENCIA EN CONSOLA (.show)")
    emit("")
    emit("[.show] KIA por clase:")
    kia_clase.show(20, truncate=False)
    emit("")
    emit("[.show] Ranking de marcas en la ventana:")
    ranking.limit(15).show(15, truncate=False)

    fin = datetime.datetime.now()
    regla("METADATOS")
    emit("Inicio    : {}".format(inicio.strftime("%Y-%m-%d %H:%M:%S")))
    emit("Fin       : {}".format(fin.strftime("%Y-%m-%d %H:%M:%S")))
    emit("Duracion  : {:.1f} segundos".format((fin - inicio).total_seconds()))
    emit("Spark     : {}".format(spark.version))
    emit("Python    : {}".format(sys.version.split()[0]))
    emit("matplotlib: {}".format(matplotlib.__version__))
    emit("Dataset   : {}".format(ruta))
    emit("Ventana   : {} a {}".format(INI, FIN))
    emit("")
    emit("RESPUESTA: {:,} vehiculos KIA matriculados entre 2020-01 y 2022-05".format(N_KIA))
    emit("           (KIAMASTER aparte: {:,})".format(N_KM))

    with open(REPORT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(LOG) + "\n")

    spark.stop()
    print("")
    print("=" * 78)
    print("  ANALISIS FINALIZADO")
    print("=" * 78)
    print("  KIA matriculados 2020-01 a 2022-05: {:,}".format(N_KIA))
    print("  KIAMASTER (variante, por separado) : {:,}".format(N_KM))
    print("  Total de la ventana                : {:,}".format(n_ventana))
    print("  Reporte : {}".format(REPORT))
    print("  Graficos: {}".format(GRAF_DIR))
    print("=" * 78)


if __name__ == "__main__":
    main()