# Análisis de casos positivos de COVID-19 en Colombia con Apache Spark

Proyecto de la **Tarea 3 – Procesamiento de Datos con Apache Spark**
Curso: Big Data (código 202016911) – Universidad Nacional Abierta y a Distancia (UNAD)

**Autor:** [Luis Alberto Aya Hernandez]
**Grupo:** [202016911_25]

---

## 1. Descripción del problema

La pandemia de COVID-19 generó millones de registros de casos positivos en Colombia. Este proyecto busca responder, mediante el análisis de un conjunto de datos de gran volumen:

- ¿Cómo se distribuyeron los casos por departamento?
- ¿Qué grupos de edad y qué sexo concentraron más casos?
- ¿Cómo evolucionaron los casos mes a mes?
- ¿Qué departamentos presentaron mayor letalidad?

## 2. Conjunto de datos

- **Nombre:** Casos positivos de COVID-19 en Colombia
- **Fuente:** Datos Abiertos Colombia (datos.gov.co), publicado por el Instituto Nacional de Salud
- **Identificador:** `gt2j-8ykr`
- **Enlace:** https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr
- **Contenido:** un registro por caso, con departamento, municipio, edad, sexo, fechas de notificación y de inicio de síntomas, estado del caso y fecha de muerte.
- **Muestra usada en este proyecto:** [ESCRIBE AQUÍ cuántas filas usaste, por ejemplo 1.000.000 de registros]

El archivo de datos **no se incluye en el repositorio** por su tamaño (más de 200 MB). Las instrucciones para descargarlo están más abajo.

## 3. Tecnologías

- Apache Spark 3.5.3 (PySpark, Python 3)
- Apache Kafka (para la parte de tiempo real)
- Ubuntu 22.04 en una máquina virtual de VirtualBox, accedida por PuTTY
- Librerías de Python: `pandas`, `matplotlib`

## 4. Estructura del repositorio

```
.
├── analisis_batch_covid.py   # Procesamiento en batch con PySpark (limpieza, EDA y gráficas)
├── README.md                 # Este archivo
├── evidencias/               # Gráficas, salida del análisis y capturas
├── diagramas/                # Diagramas (arquitectura de Kafka)
```

## 5. Qué hace el código (procesamiento en batch)

El script `analisis_batch_covid.py` sigue cuatro etapas:

1. **Carga:** lee el CSV con Spark y normaliza los nombres de las columnas.
2. **Limpieza y transformación:** elimina duplicados, estandariza texto y sexo, valida edades (0 a 120 años) y fechas, descarta registros incompletos y crea columnas derivadas (grupo de edad, mes y marca de fallecido).
3. **Análisis exploratorio (EDA):** calcula los casos por departamento, sexo, grupo de edad y mes, además de la letalidad por departamento. Incluye un ejemplo con RDD (`map`, `reduceByKey` y `takeOrdered`) y el resto con DataFrames.
4. **Visualización:** genera cuatro gráficas en formato PNG en la carpeta `resultados/`.

## 6. Instrucciones de ejecución

### 6.1 Requisitos previos

- Máquina con Apache Spark 3.5.x y Python 3 instalados
- Acceso a internet para descargar los datos

### 6.2 Clonar el repositorio

```bash
git clone https://github.com/Luisaya/Casos-positivos-de-COVID-19-en-Colombia.git
cd Casos-positivos-de-COVID-19-en-Colombia
```

### 6.3 Instalar las librerías de gráficas

```bash
pip install pandas matplotlib
```

### 6.4 Descargar los datos

Muestra de un millón de registros (las comillas simples son necesarias):

```bash
wget -O covid_colombia.csv 'https://www.datos.gov.co/resource/gt2j-8ykr.csv?$limit=1000000'
```

También se puede descargar el archivo completo con:

```bash
wget -O covid_colombia.csv 'https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD'
```

### 6.5 Ejecutar el análisis

```bash
spark-submit analisis_batch_covid.py covid_colombia.csv
```

Para guardar además la salida en un archivo de texto:

```bash
spark-submit analisis_batch_covid.py covid_colombia.csv 2>&1 | tee salida_batch.txt
```

Al finalizar, el programa imprime `Graficas guardadas en ./resultados/`.

### 6.6 Ver las gráficas

Las imágenes quedan en la carpeta `resultados/`. Si se trabaja en una máquina virtual sin entorno gráfico, se pueden ver desde el navegador con:

```bash
cd resultados
python3 -m http.server 8000
```

y abriendo `http://IP_DE_LA_VM:8000`.

## 7. Resultados

Gráficas generadas por el análisis batch:

![Casos por departamento](evidencias/01_casos_departamento.png)
![Casos por grupo de edad](evidencias/02_casos_grupo_edad.png)
![Letalidad por departamento](evidencias/03_letalidad_departamento.png)
![Casos por mes](evidencias/04_casos_por_mes.png)

La salida completa de las tablas del análisis está en [evidencias/salida_batch.txt](evidencias/salida_batch.txt).

### Hallazgos principales

> Completa esta sección con tus propios resultados, tomados de `salida_batch.txt` y de las gráficas.

- Departamento con más casos: [Bogota]
- Grupo de edad con más casos: [30-39]
- Periodo con mayor número de casos: [Magdalena]
- Departamentos con mayor letalidad: [2021-04]

## 8. Procesamiento en tiempo real (Kafka y Spark Streaming)

Esta parte se desarrolló siguiendo el Anexo 3 de la guía:

1. Se instaló y configuró Apache Kafka con ZooKeeper.
2. Se creó el tema `sensor_data`.
3. Un productor en Python (`kafka_producer.py`) genera datos simulados de sensores (temperatura y humedad) y los envía al tema cada segundo.
4. Un consumidor con Spark Streaming (`spark_streaming_consumer.py`) lee el tema y calcula el promedio de temperatura y humedad por sensor en ventanas de un minuto.

Ejecución del consumidor:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```

Las capturas de esta ejecución están en la carpeta `evidencias/`.

## 9. Video explicativo

[https://youtu.be/kWbUgVD0izU?si=dYFdEPS-4T6HpOnY]

## 10. Referencias

- Instituto Nacional de Salud. (s. f.). *Casos positivos de COVID-19 en Colombia* [Conjunto de datos]. Datos Abiertos Colombia. https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr
- Apache Software Foundation. (s. f.). *Apache Spark documentation*. https://spark.apache.org/docs/latest/
- Apache Software Foundation. (s. f.). *Apache Kafka documentation*. https://kafka.apache.org/documentation/
