import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, avg

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PySpark Transformations") \
    .master("local[*]") \
    .getOrCreate()

salesDataCSVPath = "/tmp/bigdata_nov_2024/hitesh/sample_data.csv"
data = spark.read.csv(salesDataCSVPath, header=True, inferSchema=True)
# Show the original data
print("Original Data:")
data.show()

# 1. Filter Data Based on Condition 
filtered_data = data.filter(col("Category") == "Furniture")
print("*******************************************")
print("1> Filtered Data (Category = Furniture):")
filtered_data.show()

# 2. Add a New Column 
data_with_new_column = data.withColumn("Sales", col("Price") * col("Quantity"))
print("2> Data with New Column (Sales):")
data_with_new_column.show()

# 3. Drop a Column 
data_without_column = data_with_new_column.drop("Category")
print("3> Data without Column (Category dropped):")
data_without_column.show()

# 4. Change the column Name
renamed_data = data.withColumnRenamed("Category", "Prod.Category")
print("4> Data with Renamed Column (Category -> Prod.Category):")
renamed_data.show()

# 5. Group By and Aggregate 
grouped_data = data.groupBy("Category").agg(avg("Quantity").alias("Number of Product in Category"))
print("5> Grouped Data (Average Number of Products):")
grouped_data.show()

# 6. Sort Data by a Column 
sorted_data = data.orderBy(col("Category").desc())
print("6> Sorted Data (by Category descending):")
sorted_data.show()

# 7. Replace Values in a Column 
replaced_values_data = data.fillna({"Bookshelf": "Book_Shelf"})
print("7> Data with Replaced Values (Bookshelf -> Book_Shelf):")
replaced_values_data.show()

# 8. Change Data Type of a Column 
data_with_changed_type = data.withColumn("Founded", col("Founded").cast("string"))
print("Data with Changed Data Type (Founded as string):")
data_with_changed_type.show()

# from pyspark.sql.functions import max as spark_max

# max_age = data.select(spark_max("Number of employees")).collect()[0][0]  # Get the max age
# standardized_data = data.withColumn("Number of employees_standardized", col("Number of employees") / max_age)
# print("Data with Standardized Column (Number of employees_standardized):")
# standardized_data.show()