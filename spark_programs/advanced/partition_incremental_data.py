'''
write a PySpark code that overwrites only those partitions which have data in the incremental load while 
preserving the existing partition data? The code should ensure that the new data is merged with the existing
data without losing any current partition data.

Explanation
Initialize Spark Session: Start by initializing a Spark session.
Load Existing Data: Read the existing partitioned data from S3.
Load Incremental Data: Read the incremental data from S3.
Identify Partitions: Extract the distinct partition values from the incremental data.
Filter Existing Data: Filter out the partitions in the existing data that will be overwritten by the incremental data.
Combine Data: Union the filtered existing data with the incremental data.
Write Combined Data: Write the combined data back to S3, partitioned by the partition column.
Key Points
Preserve Existing Data: By filtering out the partitions that will be overwritten, you ensure that the existing data in other partitions is preserved.
Efficient Overwrite: Only the partitions that have data in the incremental load are overwritten, making the process efficient.
This approach ensures that you maintain the integrity of your existing data while updating only the necessary partitions with the new incremental data. If you have any specific questions or need further assistance, feel free to ask!

'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Incremental Load") \
    .getOrCreate()

# Load existing data from the partitioned table
existing_data = spark.read.parquet("s3://your-bucket/existing-data/")

# Load incremental data
incremental_data = spark.read.parquet("s3://your-bucket/incremental-data/")

# Identify partitions in the incremental data
incremental_partitions = incremental_data.select("partition_column").distinct().collect()
incremental_partition_values = [row["partition_column"] for row in incremental_partitions]

# Filter existing data to exclude partitions that will be overwritten
filtered_existing_data = existing_data.filter(~col("partition_column").isin(incremental_partition_values))

# Combine filtered existing data with incremental data
combined_data = filtered_existing_data.union(incremental_data)

# Write the combined data back to S3, partitioned by the partition column
combined_data.write.mode("overwrite").partitionBy("partition_column").parquet("s3://your-bucket/existing-data/")
