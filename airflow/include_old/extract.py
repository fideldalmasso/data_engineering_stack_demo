import os
import requests
from pyspark.sql import SparkSession
import getpass
print("Current user:", getpass.getuser())

spark = SparkSession.builder.appName("CSV Ingestion Example").getOrCreate()


url = "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/PurchasesFINAL12312016csv.zip"
local_path = "/home/data2/PurchasesFINAL12312016csv.zip"

path = "/home"
files = sorted(os.listdir(path))
print('Files in directory:', path)
for f in files:
    print(f)

response = requests.get(url)
response.raise_for_status() 
with open(local_path, "wb+") as f:
    f.write(response.content)

df = spark.read.option("header", "true").csv(local_path)
df.show()
spark.stop()
