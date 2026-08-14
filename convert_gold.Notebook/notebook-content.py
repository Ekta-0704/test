# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0d00097c-5187-4c79-a993-3e41ed3be55e",
# META       "default_lakehouse_name": "UnicoreV2_Gold",
# META       "default_lakehouse_workspace_id": "f83276eb-b536-4e1e-8fdf-0da89f47a574",
# META       "known_lakehouses": [
# META         {
# META           "id": "0d00097c-5187-4c79-a993-3e41ed3be55e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
#-----------------------------------------uniphi_cus_prg_user to Gold Layer--------------------------------------------------------------------------

# Define the Delta table name
delta_table_name = "uniphi_cus_prg_user"

# Read from Silver Layer
df_final = spark.sql(f"""
SELECT * 
FROM UnicoreV2SilverLayer.{delta_table_name}
""")

# Writing to Delta table
df_final.write.format("delta").mode("overwrite").save(f"{prod_path}/{delta_table_name}")

# Total Records into Gold Table
dff = spark.sql(f"SELECT * FROM UnicoreV2GoldLayer.{delta_table_name}")
print(f"{delta_table_name} -> Total Records : ", dff.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
