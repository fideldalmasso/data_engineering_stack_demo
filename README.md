#  Entity Relationship Diagram (ERD)
[![ERD diagram](ERD.png)](https://viewer.diagrams.net/?tags=%7B%7D&lightbox=1&highlight=0000ff&edit=_blank&layers=1&nav=1&title=DER%20inventory_analysis&dark=auto#Uhttps%3A%2F%2Fdrive.google.com%2Fuc%3Fid%3D1_B1yiPsM6hpgth14rVbpCK10uO4eSjG3%26export%3Ddownload)
# Setup

## 1. Notebooks files: ETL
To automatically download, unzip, normalize tables and save them into a postgreSQL database.
Requires [Docker](https://www.docker.com/get-started/)
```bash
docker-compose build
docker-compose --profile notebook up 
```
- Run jupyter notebooks files from `notebooks/` via VSCode or by using http://localhost:8888/ + token
- Use Ctrl+C to Stop or `docker-compose down`
## 2. Metabase: Visualization
```bash
docker-compose --profile visualization up
```
- Access Metabase dashboard from http://localhost:3000/
- Use Ctrl+C to Stop or docker-compose down

## 3. Airflow demo
Requires [Astronomer](https://www.astronomer.io/docs/astro/cli/install-cli/?tab=windowswithwinget#install-the-astro-cli)
```
cd airflow
astro dev start
```
- Acces Airflow dashboard from http://localhost:8080/
---
# TODO List
- [x] Set up multi-container stack with Jupyter notebook + Spark for local development
- [x] Download .csv files and unzip them 
- [x] Normalize tables using PySpark
- [x] Configure PosgreSQL DB and write table outputs
- [ ] Use psycopg2 to add PK and FK constraints into Database
- [x] Finish basic Airflow (Astro) configuration for master and worker setup
- [x] Set up basic DAG example for data ingestion
- [ ] Fix spark-worker without write permissions
- [ ] Migrate notebooks files to new DAG in Airflow environment
- [ ] Configure daily scheduler and conditional download based on filename (DAG)
- [x] Set up Metabase app for easy-to-use data visualization
- [ ] Create meaningful visualizations in Metabase 
- [ ] Migrate parquet to delta lake to allow for efficient storage of historical records
- [ ] Deploy Airflow (Astro) setup Astronomer Cloud
- [ ] Databricks integration: Migrate SparkSubmitOperator to DatabricksSubmitRunOperator
- [ ] Storage migration: use AWS S3 buckets instead
- [ ] Database server migration: use AWS RDS service

# Dataset source
https://www.pwc.com/us/en/careers/university-relations/data-and-analytics-case-studies-files.html