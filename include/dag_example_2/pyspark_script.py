from pyspark.sql import SparkSession

def drop_columns_from_dataset(input_path, output_path):
    spark = SparkSession.builder.appName('DropColumnsApp').getOrCreate()
    df=spark.read.csv(input_path,header=True,inferSchema=True)
    columns_to_drop=['date','product']
    df_transformed = df.drop(*columns_to_drop)
    df_transformed.write.csv(output_path,header=True)
    spark.stop()


if __name__ == "__main__":
    input_dataset_path='./include/data.csv'
    output_dataset_path='./include/data_out.csv'
    drop_columns_from_dataset(input_dataset_path, output_dataset_path)