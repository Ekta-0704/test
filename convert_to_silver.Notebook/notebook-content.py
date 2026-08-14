# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Read raw data from Bronze
# Read raw data from Bronze
df = spark.read.table("Bronze_LH.dbo.bronze_posts")

# Clean it up: remove exact duplicate rows, rename columns to be clearer
silver_df = (
    df.dropDuplicates(["id"])
      .withColumnRenamed("userId", "user_id")
      .withColumnRenamed("id", "post_id")
)

# Write to Silver as a Delta table
silver_df.write.format("delta").mode("overwrite").saveAsTable("Silver_LH.dbo.silver_posts")

print(f"Rows written to Silver: {silver_df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold_LH.dbo.gold_user_post_counts LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Read cleaned data from Silver
silver_df = spark.read.table("Silver_LH.dbo.silver_posts")

# Aggregate: count how many posts each user wrote
gold_df = (
    silver_df.groupBy("user_id")
             .agg(F.count("post_id").alias("total_posts"))
             .orderBy(F.desc("total_posts"))
)

# Write to Gold as a Delta table
gold_df.write.format("delta").mode("overwrite").saveAsTable("Gold_LH.dbo.gold_user_post_counts")

gold_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
