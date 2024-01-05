from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Define your schema
my_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
])

# Create an empty DataFrame with the specified schema
empty_data = []
final_df_to_process = spark.createDataFrame(empty_data, schema=my_schema)

# Show the empty DataFrame
final_df_to_process.show()
