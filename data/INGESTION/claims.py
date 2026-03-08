from pyspark.sql import SparkSession
from pyspark.sql.functions import when, input_file_name

spark = SparkSession.builder.appName("Healthcare Claims Ingestion") \
                            .getOrCreate()

BUCKET_NAME = "healthcare-bucket-07032026"
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"
BQ_TABLE = "project-839560af-c436-4801-9dc.bronze_dataset.claims"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# read from claims source
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

# adding hospital source for future reference
claims_df = (claims_df
                .withColumn("datasource", 
                              when(input_file_name().contains("hospital2"), "hosb")
                             .when(input_file_name().contains("hospital1"), "hosa").otherwise("None")))

# dropping dupplicates if any
claims_df = claims_df.dropDuplicates()

# write to bigquery
(claims_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())