# Databricks notebook source
import datetime
import pandas as pd
from pyfcm import FCMNotification #Firebase
import pydocumentdb.document_client as document_client # cosmos
import time as tm
from dateutil.relativedelta import relativedelta as rd
import pydocumentdb
import itertools
import copy
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

# COMMAND ----------

nombreCampania = "CU_20200416_CARDS_1ERENVIO"
df_campanha_r_sp = spark.read.option("header","true").csv("/FileStore/tables/retencion/campanias/resultados/"+nombreCampania+".csv")

# COMMAND ----------

print("=======================================================================================")
print("Campaña:",nombreCampania)
print("Push notifications exitosos:",df_campanha_r_sp.filter("""output_push LIKE "%'success': 1%" """).count())
print("Fallas:",df_campanha_r_sp.filter("""output_push LIKE "%'failure': 1%" """).count())
print("Total de push notifications enviadas:",df_campanha_r_sp.count())

# COMMAND ----------

# MAGIC %md ### Número de notificaciones abiertas

# COMMAND ----------

curr_log_files = [[y[0].split("dbfs:")[-1] for y in dbutils.fs.ls(x[0].split("dbfs:")[-1]) if ".csv" in y[0]][0] for x in dbutils.fs.ls("/mnt/adlseu2bdlbpro01/data/log/paratiapp/") if len(x[0].split("/")[-2])==8]

tracking_log_data = spark.read.option("header","true").csv(curr_log_files[-6:]).filter("path='/tracking/notification' and idCampania LIKE '%CU_20200415_CARDS_1ERENVIO_000%'")

discount_log_data = spark.read.option("header","true").csv(curr_log_files[-6:]).filter("path='/tracking' and idDiscount LIKE '%CU_20200415_CARDS_1ERENVIO_000%'")

# COMMAND ----------

discount_log_data.count()

# COMMAND ----------

display(
  df_campanha_r_sp[["idUser","idDiscount"]]
  .join(
    tracking_log_data[["idUser",'idCampania']].withColumn("FLG_PUSH_ABIERTO",F.lit(1)),
    on="idUser",
    how="left"
  ).join(
    discount_log_data[["idUser","idDiscount"]].withColumn("FLG_OBTENER_DSCTO",F.lit(1)),
    on=["idUser","idDiscount"],
    how="left"
  )
  .fillna(0)
  .groupby("idCampania")
  .agg(
    F.sum("FLG_PUSH_ABIERTO").alias("FLG_PUSH_ABIERTO"),
    F.sum("FLG_OBTENER_DSCTO").alias("FLG_OBTENER_DSCTO")
  ).join(spark.createDataFrame([
  {"idCampania":"CU_20200415_CARDS_1ERENVIO_0000","segmento":"CLASICA"},
  {"idCampania": "CU_20200415_CARDS_1ERENVIO_0001","segmento":"GOLD/ORO"},
  {"idCampania": "CU_20200415_CARDS_1ERENVIO_0002","segmento":"INFINITE"},
  {"idCampania": "CU_20200415_CARDS_1ERENVIO_0003","segmento":"CLASICA LATAM"},
  {"idCampania": "CU_20200415_CARDS_1ERENVIO_0004","segmento":"PLATINUM"},
  {"idCampania": "CU_20200415_CARDS_1ERENVIO_0005","segmento":"SIGNATURE/BLACK"},
]),on="idCampania",how="left")
  [["segmento","FLG_PUSH_ABIERTO","FLG_OBTENER_DSCTO"]]
)

# COMMAND ----------

display(df_campanha_r_sp[["idUser","idDiscount"]]
  .join(
    tracking_log_data[["idUser",'idCampania']].withColumn("FLG_PUSH_ABIERTO",F.lit(1)),
    on="idUser",
    how="inner"
  ).join(
    discount_log_data[["idUser","idDiscount"]].withColumn("FLG_OBTENER_DSCTO",F.lit(1)),
    on=["idUser","idDiscount"],
    how="left"
  ))

# COMMAND ----------

display(discount_log_data.groupby("url").count())
