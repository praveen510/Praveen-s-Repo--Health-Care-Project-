from pyspark.sql import SparkSession
from pyspark.sql.functions import when, input_file_name

spark = SparkSession.builder.appName("CPT Codes Ingestion") \
                            .getOrCreate()

BUCKET_NAME = "healthcare-bucket-07032026"
CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"
BQ_TABLE = "project-839560af-c436-4801-9dc.bronze_dataset.cptcodes"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# read from cpt source
cpt_df = spark.read.csv(CPT_BUCKET_PATH, header=True)

for col in cpt_df.columns:
    new_col = col.replace(" ","_").lower()
    cpt_df=cpt_df.withColumnRenamed(col,new_col)
    

# write to bigquery
(cpt_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())