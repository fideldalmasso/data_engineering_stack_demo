import os
import requests
from pyspark.sql import SparkSession
import getpass
print("Current user:", getpass.getuser())

spark = SparkSession.builder.appName("CSV Ingestion Example").getOrCreate()


url = "https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv"
local_path = "/home/data2/airtravel.csv"

path = "/home"
files = sorted(os.listdir(path))
print('Files in directory:', path)
for f in files:
    print(f)

# Ensure the directory exists
# os.makedirs(os.path.dirname(local_path), exist_ok=True)

response = requests.get(url)
response.raise_for_status()  # Raise an error if download failed

with open(local_path, "wb+") as f:
    f.write(response.content)


df = spark.read.option("header", "true").csv(local_path)
df.show()
spark.stop()


# import os
# import getpass
# print("Current user:", getpass.getuser())
# import stat

# Print current user

# # Get file status
# path = "/home"
# os.makedirs(path, exist_ok=True)
# st = os.stat(path)

# # Print UID, GID, and permissions
# print(f"Owner UID: {st.st_uid}, GID: {st.st_gid}")
# print("Permissions:", oct(st.st_mode)[-3:])

# # Print readable, writable, executable by current user
# print("Readable:", os.access(path, os.R_OK))
# print("Writable:", os.access(path, os.W_OK))
# print("Executable:", os.access(path, os.X_OK))