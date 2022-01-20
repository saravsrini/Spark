from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import col


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("SparkDemoApp") \
        .config("spark.jars","file:///home/sarav/jar/mysql-connector-java-8.0.22.jar").getOrCreate()

    url = "jdbc:mysql://localhost:3306/nation?user=root&password=sarav"

    db_target_properties = {"driver":"com.mysql.cj.jdbc.Driver"}

    customer_db_df = spark.read.jdbc(url,"countries",properties=db_target_properties)
    customer_df = customer_db_df.select(col("country_id"),col("name"),col("area"),col("country_code2"),col("country_code3"),col("region_id"))

#    customer_df.write.csv(path = "hdfs:///user/hive/warehouse/outbound/hdfs/",header = "true",sep = ",",encoding = "UTF-8",mode = "OVERWRITE")

    customer_trans_schema= StructType([
        StructField("Customer_id",IntegerType()),
        StructField("transaction_amt",StringType()),
        StructField("transaction_rating",StringType())
    ])

    customer_transaction = spark.readStream.option("header",True).schema(customer_trans_schema). \
            csv(path="file:///home/sarav/streaming_dataset/input")

    customer_transaction_join = customer_df.join(customer_transaction,customer_df.country_id ==customer_transaction.Customer_id,"inner")

    def process_row(df, epoch_id):
        df.write.csv(path = "hdfs:///user/hive/warehouse/outbound/hdfs/",header = "true",sep = ",",encoding = "UTF-8",mode = "OVERWRITE")
        pass

    query = customer_transaction_join.writeStream.foreachBatch(process_row).start()

    query.awaitTermination()
