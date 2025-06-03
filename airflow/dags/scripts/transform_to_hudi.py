from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, current_timestamp
from pyspark.sql.types import DoubleType

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("HudiETLJob") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# 2. Load CSV from S3
input_path = "s3://varun-demo-temp-s3-vilva/datasets/fatal_collisions.csv"
df = spark.read.option("header", True).csv(input_path)

# 3. Rename columns using snake_case
rename_cols = {
    'VIN (1-10)': 'vehical_number',
    'Postal Code': 'postal_code',
    'Electric Vehicle Type': 'vehical_type',
    'Clean Alternative Fuel Vehicle (CAFV) Eligibility': 'cavf_eligibility',
    'Electric Range': 'electric_range',
    'Base MSRP': 'base_msrp',
    'Legislative District': 'legislative_district',
    'Vehicle Location': 'vehicle_location',
    'Electric Utility': 'electric_utility',
    '2020 Cencus Tract': 'cencus_tract',
    'Model Year': 'model_year'
}

for old_name, new_name in rename_cols.items():
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)

# Lowercase all column names
for column in df.columns:
    df = df.withColumnRenamed(column, column.lower())

# Fill nulls in selected columns
df_filled = df.fillna({
    'city': 'Unknown',
    'county': 'Unknown',
    'postal_code': 0,
    'electric_range': 0,
    'electric_utility': 'Unknown',
    'model_year': 0,
    'vehicle_location': 'Unknown',
    'legislative_district': 0,
})

# Drop unnecessary columns
columns_to_drop = ['2020 census tract', 'dol vehicle id']
for col_name in columns_to_drop:
    if col_name in df_filled.columns:
        df_filled = df_filled.drop(col_name)

# Extract longitude and latitude from 'vehicle_location'
new_df = df_filled.withColumn(
    'longitude', regexp_extract(col("vehicle_location"), r'POINT \((-?\d+\.\d+)', 1).cast(DoubleType())
).withColumn(
    'latitude', regexp_extract(col("vehicle_location"), r'POINT \(-?\d+\.\d+ (-?\d+\.\d+)\)', 1).cast(DoubleType())
)

# Drop the raw location column
if 'vehicle_location' in new_df.columns:
    new_df = new_df.drop('vehicle_location')

# Fill remaining nulls
new_df = new_df.fillna({
    'latitude': 0,
    'longitude': 0,
    'base_msrp': 0
})

# Add event_time column
new_df = new_df.withColumn('event_time', current_timestamp())

# 4. Define Hudi write options
hudi_options = {
    'hoodie.table.name': 'ev_data_cleaned',
    'hoodie.datasource.write.storage.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.recordkey.field': 'vehical_number',
    'hoodie.datasource.write.partitionpath.field': 'state',
    'hoodie.datasource.write.precombine.field': 'event_time',
    'hoodie.upsert.shuffle.parallelism': '2',
    'hoodie.insert.shuffle.parallelism': '2',
    # Hive Sync Settings
    'hoodie.datasource.hive_sync.enable': 'false'
}

# 5. Output path for Hudi table
output_path = "s3://varun-demo-temp-s3-vilva/hudi_tables/ev_data_cleaned"

# 6. Write the data to Hudi
new_df.write.format("hudi") \
    .options(**hudi_options) \
    .mode("append") \
    .save(output_path)
