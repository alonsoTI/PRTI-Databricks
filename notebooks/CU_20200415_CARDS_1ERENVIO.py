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

from pytz import timezone
from datetime import datetime
currentday=datetime.now(timezone('America/Bogota')).strftime('%Y%m%d')

# COMMAND ----------

#NOMBRE DE CAMPANIA
nombreCampania="CU_20200415_CARDS_1ERENVIO"
print(nombreCampania)

# COMMAND ----------

# MAGIC %md #### Push information

# COMMAND ----------

df_push_info = spark.createDataFrame([{
  "title":"Tarjeta de Crédito BCP - Obtenla aquí",
  "body":"Realiza tus compras desde casa con tu Tarjeta BCP",
  "typeEvents":"discountCampaign",
  "idSection":"",
  "keyword":"",
}])
display(df_push_info)

# COMMAND ----------

df_segmented_push_info = spark.createDataFrame([
  {"idGrupoBeneficio":"CU_20200415_CARDS_1ERENVIO_0000",
   "imageUrl":"https://stgeu2patia01.blob.core.windows.net/imageszpatiphotos/descuentos/Parati-destacado-300x200.png"
  },
  {"idGrupoBeneficio":"CU_20200415_CARDS_1ERENVIO_0001",
   "imageUrl":"https://stgeu2patia01.blob.core.windows.net/imageszpatiphotos/descuentos/Parati-destacado-300x200-g.png"
  },
  {"idGrupoBeneficio":"CU_20200415_CARDS_1ERENVIO_0002",
   "imageUrl":"https://stgeu2patia01.blob.core.windows.net/imageszpatiphotos/descuentos/Parati-destacado-300x200-i.png"
  },
  {"idGrupoBeneficio":"CU_20200415_CARDS_1ERENVIO_0003",
   "imageUrl":"https://stgeu2patia01.blob.core.windows.net/imageszpatiphotos/descuentos/Parati-destacado-300x200-l.png"
  },
  {"idGrupoBeneficio":"CU_20200415_CARDS_1ERENVIO_0004",
   "imageUrl":"https://stgeu2patia01.blob.core.windows.net/imageszpatiphotos/descuentos/Parati-destacado-300x200-p.png"
  },
  {"idGrupoBeneficio":"CU_20200415_CARDS_1ERENVIO_0005",
   "imageUrl":"https://stgeu2patia01.blob.core.windows.net/imageszpatiphotos/descuentos/Parati-destacado-300x200-s.png"
  }
])

display(df_segmented_push_info)

# COMMAND ----------

# MAGIC %md ### Usuarios de prueba squad

# COMMAND ----------

# MAGIC %sql
# MAGIC   --19413be5-6ac2-4e9c-9822-2f19c697a107--kat
# MAGIC --5f32819e-ba73-484c-8402-167f5bb54c3c--ser
# MAGIC --b5bb9477-d0b0-4537-9bcf-4035f2b74469--Ro
# MAGIC --50590558-f0a2-4b14-bbc4-e1c1818d2561--Jose
# MAGIC --cd7c88f3-d6a2-4f6c-8129-7dbda892f93c--Alex
# MAGIC --b626f282-fdb5-48fb-a012-5a228448e415--Harvey
# MAGIC --9b4ffb4c-ea0e-4971-9151-da637cf8664e--Dayana
# MAGIC --d7e1485b-42cc-447d-8356-6efc3d00d04f--Willy

# COMMAND ----------

df_test_squad = spark.createDataFrame([{"idUser":"5f32819e-ba73-484c-8402-167f5bb54c3c"}])

# COMMAND ----------

# MAGIC %md ### Usuarios de prueba cliente

# COMMAND ----------

spark.sql("REFRESH TABLE parati_landing_tmp.ud_users_decrypted")
df_test_cliente = spark.sql("select * from parati_landing_tmp.ud_users_decrypted WHERE telefono = '969928064' or codunicocli LIKE '%70433439%'")

# COMMAND ----------

display(df_test_cliente)

# COMMAND ----------

df_push_crossed = df_push_info.crossJoin(df_segmented_push_info.repartition(6)).cache()
display(df_push_crossed)

# COMMAND ----------

# df_test = df_test_squad.cache()
df_test_users = df_test_squad.union(df_test_cliente[["id"]].withColumnRenamed("id","idUser")).cache()
display(df_test_users)

# COMMAND ----------

df_test = df_test_users.crossJoin(df_push_crossed).cache()
display(df_test)

# COMMAND ----------

df_test_processed = (df_test
                            .withColumn("idCampania",F.col("idGrupoBeneficio"))
                            .withColumn("idGroupDiscount",F.col("idGrupoBeneficio"))
                            .withColumn("idDiscount",F.col("idGrupoBeneficio"))
                            .withColumn("showImage",F.lit(1))
                           )

# COMMAND ----------

display(df_test_processed)

# COMMAND ----------

# MAGIC %md #### Carga Firebase tokens

# COMMAND ----------


InitialTime = tm.time()
fmt = '{0.hours} horas {0.minutes} minutos {0.seconds} segundos'


#RUTA DONDE PONER EXCEL DE PIERO
#ruta_inputs = "/mnt/fileszpatidesa/data/Notificaciones/"

#Excel para enviar campañas
#archivoClientes= "CIU00121_Notificacion_Usuario_ 20190416.xlsx"

#Output de envios exitosos
#archivoOutput="Respuesta_CIU00121_Notificacion_Usuario_ 20190416.csv"

## Variables Fijas
config_desa = {
    'ENDPOINT': 'https://acdbeu2patia01.documents.azure.com',
    'MASTERKEY': 'JQm1LYcAPAGm10LvNu99s1zXmdJn7ooC30yXnez7SqtGdnSPa1etHO0u7deEZItAjQh7tFnFGXqqD9VFSrJRjA==',
    'DOCUMENTDB_DATABASE': 'ParaTiDB'
    }

config_prod = {
    'ENDPOINT': 'https://acdbeu2patiproda01.documents.azure.com',
    'MASTERKEY': 'xr6PiRfHqCqAojXvXjhI4gSbmmaLLvIYTF1IWOdPasLm8jnIaO9ogOXS4l8RJYtezmxcnlFsBYUkhk2nbVJH5Q==',
    'DOCUMENTDB_DATABASE': 'ParaTiDB'
}

# Cosmos
client_desa = document_client.DocumentClient(config_desa['ENDPOINT'], {'masterKey': config_desa['MASTERKEY']})
client_prod = document_client.DocumentClient(config_prod['ENDPOINT'], {'masterKey': config_prod['MASTERKEY']})

# Firebase (llave del servicio firebase)
SERVER_KEY = "AAAA_hNF2-0:APA91bGBVzMk1AUspUIx2Z7iY6euFBf-QP6DJxD_ZiFgKlSoasUdFSjG5a03QqwAdazEhxOhI-HRMXSayf25ro1gl_90jesYEzRT4NuJoBdSdb91yQMuz8LS6SL0UCCB5G7LI5nutsJd"
push_service = FCMNotification(api_key=SERVER_KEY)



# COMMAND ----------

## Definicion de funciones a utilizar (Cosmos)
def get_collection_link(collection_name, config):
    client = document_client.DocumentClient(config['ENDPOINT'], {'masterKey': config['MASTERKEY']})
    db_query = "select * from r where r.id = '{0}'".format(config['DOCUMENTDB_DATABASE'])
    db_link = list(client.QueryDatabases(db_query))[0]['_self']
    coll_id = collection_name
    coll_query = "select * from r where r.id = '{0}'".format(coll_id)
    coll = list(client.QueryCollections(db_link, coll_query))[0]
    coll_link = coll['_self']
    return coll_link


# # c-Logica


# Usuarios en cada dispositivo - tabla cosmos FireBaseAccess con iddispositivos
# tokens_desa = client_desa.ReadDocuments(get_collection_link("FireBaseAccess", config_desa)) # Lectura de la colección
tokens_prod = client_prod.ReadDocuments(get_collection_link("FireBaseAccess", config_prod)) # Lectura de la colección
# df_tokens = pd.DataFrame(list(tokens_desa) + list(tokens_prod))
df_tokens = pd.DataFrame(list(tokens_prod))

# COMMAND ----------

# MAGIC %md #### FCM Wrapper

# COMMAND ----------

def send_message(row):
    try:
        if row["device"] == "Android":
            return push_service.notify_single_device(registration_id=row["firebaseToken"],
                                                     data_message = {"title": row["title"],
                                                                   "body": row["body"],
                                                                   "keyWord": row["keyword"],
                                                                   "typeEvents": row["typeEvents"],
                                                                   "typePush": "RT",
                                                                   "idSection": row["idSection"],
                                                                   "idCampania": row["idCampania"],
                                                                   "device": "Android",
                                                                   "idGroupDiscount": row["idGroupDiscount"],
                                                                   "idDiscount": row["idGroupDiscount"],
                                                                   "idc": row["idUser"],
                                                                   "imageDiscountUrl": row["imageUrl"],
                                                                   "sound": "default"
                                                                  }
                                                        )
        elif row["device"] == "Iphone":
            return push_service.notify_single_device(registration_id=row["firebaseToken"],
                                                  message_title=row["title"],
                                                  message_body=row["body"],
                                                  sound="default",
                                                  extra_notification_kwargs = {
                                                      "idGroupDiscount": row["idGroupDiscount"], 
                                                      "idDiscount": row["idGroupDiscount"],
                                                      "keyWord": row["keyword"],
                                                      "typeEvents": row["typeEvents"],
                                                      "typePush": "RT",
                                                      "idSection": row["idSection"],
                                                      "idCampania": row["idCampania"],
                                                      "idc": row["idUser"],
                                                      "imageDiscountUrl": row["imageUrl"],
                                                      "device": "Iphone",
                                                      "badge":1
                                                  },
                                                  extra_kwargs = {"mutable_content": row["showImage"], "data":{"urlImageString": row["imageUrl"]}}
                                                 )
        else:
            return None
    except Exception as e:
        return {"message": "Problemas de Conexion Firebase", "success": 0, "error_exception":str(e)}

# COMMAND ----------

#Dataframe con todos los usuarios
# df_campaign = df_total_leads_processed 
df_campaign = df_test_processed # .limit(1)

df_campaign = df_campaign.withColumnRenamed("id","idUser").toPandas()

df = df_campaign
df.showImage = df.showImage.astype(bool)

#df_tokens = pd.DataFrame(list(tokens_prod))
df_tokens = df_tokens[['device', 'deviceToken', 'firebaseToken', 'idUser', 'numeroDocumento', 'timeStamp']]
df_tokens = df_tokens.loc[(df_tokens["device"] != "web") & (pd.notnull(df_tokens["device"])), :].reset_index(drop=True)

# df_tokens["date"] = df_tokens["timeStamp"].apply(lambda t: datetime.datetime.fromtimestamp(t / 1000).strftime('%Y-%m-%d %H:%M:%S'))
df_tokens.sort_values(by=["timeStamp"], ascending=[False], inplace=True)
df_tokens["index"] = df_tokens.groupby(["idUser"]).cumcount()



df_tokens = df_tokens.loc[df_tokens["index"] == 0].reset_index(drop=True)
del df_tokens["index"]
# df_tokens = df_tokens.loc[pd.notnull(df_tokens["numeroDocumento"])].reset_index(drop=True)

# Cruce de tabla de campañas y de tokens
df_campanha = df.merge(df_tokens, how="left", left_on="idUser", right_on="idUser")

df_campanha["status"] = ""
df_campanha.loc[~pd.notnull(df_campanha["firebaseToken"]) & ~pd.notnull(df_campanha["firebaseToken"]), "status"] = "SIN TOKEN REGISTRADO"
df_campanha.fillna("", inplace=True)
# break
df_campanha_r = df_campanha
df_campanha_r["output_push"] = "" 

# COMMAND ----------

df_campanha_r.shape

# COMMAND ----------

df_campanha_r[df_campanha_r.idGrupoBeneficio=="CU_20200415_CARDS_1ERENVIO_0000"]

# COMMAND ----------

cod_enviar

# COMMAND ----------

print("Enviar Push...")
cod_enviar = "CU_20200415_CARDS_1ERENVIO_0004"
results = df_campanha_r.loc[df_campanha_r.idGrupoBeneficio==cod_enviar,:].apply(lambda row: send_message(row), axis=1)
print("Fin del envio Push Campania")
results

# COMMAND ----------

print("Enviar Push...")
df_campanha_r.loc[df_campanha_r["status"] != "SIN TOKEN REGISTRADO", "output_push"] = df_campanha_r.loc[df_campanha_r["status"] != "SIN TOKEN REGISTRADO", :].apply(lambda row: send_message(row), axis=1)


print("Fin del envio Push Campania")
  # log_list.append(df_campanha_r)

# COMMAND ----------

df_campanha_r[["output_push"]]

# COMMAND ----------


