"""
Procesamiento en batch con PySpark
Dataset: Casos positivos de COVID-19 en Colombia (datos.gov.co, id gt2j-8ykr)

Uso:
    spark-submit analisis_batch_covid.py covid_colombia.csv

Genera tablas en consola y graficas PNG en la carpeta ./resultados
"""
import os
import re
import sys
import unicodedata

import matplotlib
matplotlib.use("Agg")  # sin pantalla: guarda las graficas como archivos
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA = sys.argv[1] if len(sys.argv) > 1 else "covid_colombia.csv"
SALIDA = "resultados"
os.makedirs(SALIDA, exist_ok=True)

spark = SparkSession.builder.appName("CovidColombiaBatch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------------
# 1. CARGA
# ---------------------------------------------------------------
def normalizar(nombre):
    """Convierte un nombre de columna a minusculas, sin tildes ni simbolos."""
    s = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


df = spark.read.option("header", True).csv(RUTA)
df = df.toDF(*[normalizar(c) for c in df.columns])
columnas = df.columns

print("\n=== 1. CARGA ===")
print("Columnas encontradas:", columnas)
df.printSchema()


def buscar(*candidatos, obligatoria=True):
    """Devuelve la primera columna que coincida (exacta o parcial) con los candidatos."""
    for c in candidatos:
        if c in columnas:
            return c
    for c in candidatos:
        for col in columnas:
            if c in col:
                return col
    if obligatoria:
        sys.exit(f"No encontre ninguna columna parecida a {candidatos}. "
                 f"Columnas disponibles: {columnas}")
    return None


c_id = buscar("id_de_caso", "id_caso", obligatoria=False)
c_fecha = buscar("fecha_reporte_web", "fecha_de_notificaci", "fecha_diagnostico",
                 "fecha_de_notificacion")
c_dep = buscar("departamento_nom", "nombre_departamento", "departamento")
c_sexo = buscar("sexo")
c_edad = buscar("edad")
c_unidad = buscar("unidad_medida", "unidad_de_medida", obligatoria=False)
c_estado = buscar("estado", obligatoria=False)
c_muerte = buscar("fecha_muerte", "fecha_de_muerte", obligatoria=False)

total_bruto = df.count()
print(f"Registros cargados: {total_bruto:,}")

# ---------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACION
# ---------------------------------------------------------------
print("\n=== 2. LIMPIEZA Y TRANSFORMACION ===")

if c_id:
    df = df.dropDuplicates([c_id])

edad = F.col(c_edad).cast("int")
if c_unidad:
    # unidad 1 = anios; meses o dias se consideran 0 anios
    edad = F.when(F.col(c_unidad).cast("int") == 1, edad).otherwise(F.lit(0))

fecha_txt = F.substring(F.col(c_fecha), 1, 10)
fecha = F.coalesce(F.to_date(fecha_txt, "yyyy-MM-dd"),
                   F.to_date(fecha_txt, "dd/MM/yyyy"))

estado = F.upper(F.trim(F.col(c_estado))) if c_estado else F.lit("SD")
fallecido = (estado == "FALLECIDO")
if c_muerte:
    fallecido = fallecido | F.col(c_muerte).isNotNull()

limpio = df.select(
    F.upper(F.trim(F.col(c_dep))).alias("departamento"),
    F.when(F.upper(F.trim(F.col(c_sexo))).isin("M", "F"),
           F.upper(F.trim(F.col(c_sexo)))).otherwise("SD").alias("sexo"),
    edad.alias("edad"),
    fecha.alias("fecha"),
    F.coalesce(estado, F.lit("SD")).alias("estado"),
    fallecido.cast("int").alias("fallecido"),
)

limpio = (limpio
          .filter(F.col("departamento").isNotNull() & (F.col("departamento") != ""))
          .filter(F.col("fecha").isNotNull())
          .filter(F.col("edad").between(0, 120)))

# Columnas derivadas
lo = F.least(F.floor(F.col("edad") / 10) * 10, F.lit(80))
limpio = (limpio
          .withColumn("grupo_ini", lo)
          .withColumn("grupo_edad",
                      F.when(lo == 80, "80+")
                       .otherwise(F.concat(lo.cast("string"), F.lit("-"),
                                           (lo + 9).cast("string"))))
          .withColumn("mes", F.date_format("fecha", "yyyy-MM")))

limpio.cache()
total = limpio.count()
print(f"Registros despues de limpiar: {total:,} "
      f"(descartados: {total_bruto - total:,})")
limpio.show(5, truncate=False)

# ---------------------------------------------------------------
# 3. ANALISIS EXPLORATORIO (EDA)
# ---------------------------------------------------------------
print("\n=== 3. EDA ===")

print("Estadisticas de la edad:")
limpio.select("edad").describe().show()

# 3.1 Casos por departamento
por_depto = (limpio.groupBy("departamento").count()
             .orderBy(F.desc("count")).limit(10))
print("Top 10 departamentos por casos:")
por_depto.show(truncate=False)

# 3.2 Casos por sexo
por_sexo = limpio.groupBy("sexo").count().orderBy(F.desc("count"))
print("Casos por sexo:")
por_sexo.show()

# 3.3 Casos por grupo de edad
por_edad = (limpio.groupBy("grupo_ini", "grupo_edad").count()
            .orderBy("grupo_ini"))
print("Casos por grupo de edad:")
por_edad.show()

# 3.4 Serie mensual
por_mes = limpio.groupBy("mes").count().orderBy("mes")
print("Casos por mes (primeras filas):")
por_mes.show(12)

# 3.5 Letalidad por departamento (solo departamentos con mas de 1000 casos)
letalidad = (limpio.groupBy("departamento")
             .agg(F.count("*").alias("casos"),
                  F.sum("fallecido").alias("fallecidos"))
             .filter(F.col("casos") > 1000)
             .withColumn("letalidad_pct",
                         F.round(F.col("fallecidos") / F.col("casos") * 100, 2))
             .orderBy(F.desc("letalidad_pct")).limit(10))
print("Top 10 departamentos por letalidad (%):")
letalidad.show(truncate=False)

# 3.6 Ejemplo con RDD (map + reduceByKey + takeOrdered)
print("Ejemplo con RDD: casos por departamento (map + reduceByKey):")
rdd = (limpio.select("departamento").rdd
       .map(lambda fila: (fila[0], 1))
       .reduceByKey(lambda a, b: a + b))
for depto, n in rdd.takeOrdered(5, key=lambda kv: -kv[1]):
    print(f"  {depto}: {n:,}")

# ---------------------------------------------------------------
# 4. VISUALIZACION
# ---------------------------------------------------------------
print("\n=== 4. VISUALIZACION ===")


def barras(pdf, x, y, titulo, archivo, horizontal=True):
    plt.figure(figsize=(9, 5))
    if horizontal:
        plt.barh(pdf[x][::-1], pdf[y][::-1], color="#1f77b4")
        plt.xlabel(y)
    else:
        plt.bar(pdf[x], pdf[y], color="#1f77b4")
        plt.ylabel(y)
    plt.title(titulo)
    plt.tight_layout()
    plt.savefig(os.path.join(SALIDA, archivo), dpi=120)
    plt.close()


barras(por_depto.toPandas(), "departamento", "count",
       "Top 10 departamentos por casos de COVID-19", "01_casos_departamento.png")

barras(por_edad.toPandas(), "grupo_edad", "count",
       "Casos por grupo de edad", "02_casos_grupo_edad.png", horizontal=False)

barras(letalidad.toPandas(), "departamento", "letalidad_pct",
       "Letalidad por departamento (%)", "03_letalidad_departamento.png")

serie = por_mes.toPandas()
plt.figure(figsize=(11, 4.5))
plt.plot(serie["mes"], serie["count"], color="#d62728")
plt.xticks(serie["mes"][::3], rotation=60)
plt.title("Casos de COVID-19 por mes")
plt.ylabel("casos")
plt.tight_layout()
plt.savefig(os.path.join(SALIDA, "04_casos_por_mes.png"), dpi=120)
plt.close()

print(f"Graficas guardadas en ./{SALIDA}/")
spark.stop()
