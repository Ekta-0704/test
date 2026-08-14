# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3766f896-9d75-4b48-9f26-994c639c800c",
# META       "default_lakehouse_name": "UnicoreV2_Bronze",
# META       "default_lakehouse_workspace_id": "f83276eb-b536-4e1e-8fdf-0da89f47a574",
# META       "known_lakehouses": [
# META         {
# META           "id": "3766f896-9d75-4b48-9f26-994c639c800c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd

wrangler_sample_df = pd.read_csv("https://aka.ms/wrangler/titanic.csv")
display(wrangler_sample_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "editable": true
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
#-----------------------------------------uniphi_cus_prg_user to Bronze Layer--------------------------------------------------------------------------

# Query the Customer Program list Delta table to retrieve 'id' values
id_df = spark.sql("SELECT id FROM UnicoreV2GoldLayer.uniphi_cus_program")

# Extract 'id' values from the Spark DataFrame and collect them into a list
id_list = [row.id for row in id_df.collect()]
print(id_list)

# Define your Delta table path
delta_table_path = "Tables/uniphi_cus_prg_user_raw"
delta_table_name = "uniphi_cus_prg_user_raw"

# Drop Existing
spark.sql(f"DROP TABLE if exists {delta_table_name}")

# Define the schema for raw storage
raw_schema = StructType([
    StructField("customerProgramId", IntegerType(), True),
    StructField("raw_response", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("InsertDate", TimestampType(), True)
])

rows = []

# Loop through each id value and retrieve raw data
for id_value in id_list:
    api_endpoint = f'{url}/rest/customerPrograms/{id_value}/programUsers/data'
    try:
        response = requests.get(api_endpoint, headers=headers)

        current_time_uk = datetime.now(uk_time_zone)

        rows.append((
            id_value,
            response.text,
            response.status_code,
            current_time_uk
        ))

        print(f"Fetched raw data for id = {id_value}. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred for id = {id_value}: {e}")
        rows.append((id_value, None, None, datetime.now(uk_time_zone)))

# Write raw responses to Bronze Delta table
raw_df = spark.createDataFrame(rows, schema=raw_schema)
raw_df.write.mode("append").format("delta").saveAsTable(delta_table_name)

print(f"{delta_table_name} -> Total Records : ", spark.sql(f"SELECT * FROM {delta_table_name}").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
