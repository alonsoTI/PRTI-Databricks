# Databricks notebook source
import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents
import time as tm
from dateutil.relativedelta import relativedelta as rd
from pyspark.sql import functions as F
import json
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,ArrayType,FloatType,DoubleType
from pyspark.sql.functions import lit
from pyspark.sql.functions import from_json

# COMMAND ----------


print("Produccion")
#PARAMETROS PRODUCCION
ENDPOINT = "https://acdbeu2patiproda01.documents.azure.com:443/"
MASTERKEY = "BCTm1WG14wDxBzJvjVEGbIb22fopncgTD0SC44sZ5wRUzZAyQXBbx1Xy7y6uMAMWusW6vvPpGxyocjkRhIsNkw===="
DATABASE = 'ParaTiDB'
DBLINK = 'dbs/' + DATABASE


# COMMAND ----------

"""
print("Desarrollo")
#PARAMETROS DESARROLLO
ENDPOINT = "https://acdbeu2patia01.documents.azure.com:443/"
MASTERKEY = "3sRK91D3Cj8pViLgyqCTeg0OSJud3E7TKmBsAOau4WguWDghxyHzyjdOqcoFjXMp2wqzNlosAWonJ9Ld5bbrbg=="
DATABASE = 'ParaTiDB'
DBLINK = 'dbs/' + DATABASE
"""

# COMMAND ----------

connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery
connectionPolicy.PreferredLocations = ["Central US", "East US 2", "Southeast Asia", "Western Europe","Canada Central"]
client = document_client.DocumentClient(ENDPOINT, {'masterKey': MASTERKEY}, connectionPolicy)

# COMMAND ----------

def create_collection(collection_name):
  print("Creating collection "+collection_name)
  coll = {
              "id": collection_name,
              "indexingPolicy": {
              "indexingMode": "consistent",
              "automatic": True,
              "includedPaths": [
                                {
                                    "path": "/*",
                                    "indexes": [
                                        {
                                            "kind": "Range",
                                            "dataType": "Number",
                                            "precision": -1
                                        },
                                        {
                                            "kind": "Range",
                                            "dataType": "String",
                                            "precision": -1
                                        }
                                    ]
                                }
                            ],
              "excludedPaths": []
              }
             }

  collection_options = {
          'offerThroughput': 5000
        }
  try:
       client.CreateCollection(DBLINK , coll, collection_options)
  except:
       print("Can't create_collection "+collection_name)
  else:
       print("Created Collection successful  "+collection_name)


# COMMAND ----------

def delete_collection(collection_name):
  print("Deleting collection "+collection_name)
  
  collection_link = DBLINK + '/colls/{0}'.format(collection_name)
  try:
     client.DeleteCollection(collection_link)
  except:
     print("Can't delete_collection "+collection_name) 
  else:
    print("Deleted Collection successful "+collection_name) 

# COMMAND ----------

#Funcion ingestar documentos
def create_document(collection_name, df_document):
  print("Ingestando data "+collection_name)
  
  writeConfig = {
   "Endpoint" : ENDPOINT,
   "Masterkey" : MASTERKEY,
   "Database" : DATABASE,
   "Collection" : collection_name,
   "Upsert" : "true"
  }
  print(ENDPOINT)
  print(MASTERKEY)
  print(DATABASE)
  print(collection_name)
  #try:
  df_document.write.mode("overwrite").format("com.microsoft.azure.cosmosdb.spark").options(**writeConfig).save()

# COMMAND ----------

delete_collection('')

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestar

# COMMAND ----------


spark.catalog.refreshTable("parati_landing.ud_users_campaign")
dfIngestar=sqlContext.sql("""select distinct idUser from parati_landing.ud_users_campaign where flg_activo=1""")

# COMMAND ----------

create_document('TablaActivos',dfIngestar)

# COMMAND ----------


