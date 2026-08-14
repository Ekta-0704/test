# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3ab0a0c6-f159-41fa-b76b-1e72cd2a06ec",
# META       "default_lakehouse_name": "UnicoreV2_Silver",
# META       "default_lakehouse_workspace_id": "f83276eb-b536-4e1e-8fdf-0da89f47a574",
# META       "known_lakehouses": [
# META         {
# META           "id": "3ab0a0c6-f159-41fa-b76b-1e72cd2a06ec"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
#-----------------------------------------uniphi_cus_prg_user to Silver Layer (Upsert)--------------------------------------------------------

from delta.tables import DeltaTable

# Define the Delta table name
delta_table_name = "uniphi_cus_prg_user"

# Define the schema to match your Delta table schema
custom_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("inviteTimestampMillis", TimestampType(), True),
    StructField("inviteUserRole", StringType(), True),
    StructField("inviteEmail", StringType(), True),
    StructField("customerProgramId", IntegerType(), True),
    StructField("m49Code", StringType(), True),
    StructField("inviteInstitutionName", StringType(), True),
    StructField("spentTimestampMillis", TimestampType(), True),
    StructField("invitedAccount_id", IntegerType(), True),
    StructField("invitedAccount_email", StringType(), True),
    StructField("invitedAccount_creationTimestampMillis", TimestampType(), True),
    StructField("invitedAccount_institutionReferenceId", StringType(), True),
    StructField("invitedAccount_physicianReferenceId", StringType(), True),
    StructField("invitedAccount_pharmacistReferenceId", StringType(), True),
    StructField("inviteInstitutionReferenceId", StringType(), True),
    StructField("InsertDate", TimestampType(), True)
])

# ---------------------------------------------------------------------------
# STEP 1: Create Silver table once if it doesn't exist (no more DROP TABLE)
# ---------------------------------------------------------------------------
if not spark._jsparkSession.catalog().tableExists(delta_table_name):
    empty_df = spark.createDataFrame([], custom_schema)
    empty_df.write.format("delta").saveAsTable(delta_table_name)
    print(f"Created new empty table: {delta_table_name}")

# ---------------------------------------------------------------------------
# STEP 2: Read Bronze (Bronze is always a full refresh, so we read it all)
# ---------------------------------------------------------------------------
bronze_df = spark.sql("SELECT customerProgramId, raw_response FROM UnicoreV2BronzeLayer.uniphi_cus_prg_user_raw")
bronze_rows = bronze_df.collect()
print(f"Rows to process this run: {len(bronze_rows)}")

all_pandas_dfs = []

for row in bronze_rows:
    id_value = row.customerProgramId
    raw_text = row.raw_response

    if not raw_text:
        print(f"No raw data for id = {id_value}. Skipping.")
        continue

    data = json.loads(raw_text)

    if not data:
        print(f"JSON data is blank for id = {id_value}. Skipping processing for this id.")
        continue

    pandas_df = json_normalize(data)
    pandas_df = pandas_df.fillna('')
    pandas_df.columns = pandas_df.columns.str.replace('.', '_')

    if 'invitedAccount_id' in pandas_df:
        try:
            pandas_df['invitedAccount_id'] = pd.to_numeric(pandas_df['invitedAccount_id'], errors='coerce')
            pandas_df['invitedAccount_id'].fillna(-999, inplace=True)
            pandas_df['invitedAccount_id'] = pandas_df['invitedAccount_id'].astype('Int64')
        except ValueError as e:
            print(f"Error: {e}")
            print("Non-numeric values detected in 'invitedAccount_id' column. Check and handle them.")
    else:
        pandas_df['invitedAccount_id'] = [None] * len(pandas_df)

    if 'invitedAccount_institutionReferenceId' not in pandas_df:
        pandas_df['invitedAccount_institutionReferenceId'] = ''
    else:
        pandas_df['invitedAccount_institutionReferenceId'] = pandas_df['invitedAccount_institutionReferenceId'].apply(lambda x: '|'.join(x))

    current_time_uk = datetime.now(uk_time_zone)
    pandas_df['InsertDate'] = current_time_uk

    date_columns = ["inviteTimestampMillis", "spentTimestampMillis", "invitedAccount_creationTimestampMillis"]

    for field in custom_schema.fields:
        field_name = field.name
        if field_name not in pandas_df:
            pandas_df[field_name] = ''

    for column in date_columns:
        if column in pandas_df:
            pandas_df[column] = pd.to_datetime(pandas_df[column], unit='ms')

    pandas_df = pandas_df[custom_schema.fieldNames()]
    pandas_df['customerProgramId'] = pandas_df['customerProgramId'].astype('Int64')

    all_pandas_dfs.append(pandas_df)

    print(f"Transformed data for id = {id_value}")

# ---------------------------------------------------------------------------
# STEP 3: Combine all transformed rows into a single Spark DataFrame
# ---------------------------------------------------------------------------
if len(all_pandas_dfs) == 0:
    print("No valid records to load after filtering blanks/empties.")
else:
    combined_pandas_df = pd.concat(all_pandas_dfs, ignore_index=True)
    spark_df = spark.createDataFrame(combined_pandas_df, schema=custom_schema)

    spark_df = spark_df.withColumn(
        "invitedAccount_id",
        when(col("invitedAccount_id") == -999, None).otherwise(col("invitedAccount_id"))
    )

    # -----------------------------------------------------------------
    # STEP 4: UPSERT into Silver -- update matching rows, insert new ones
    # Matched on id + customerProgramId
    # -----------------------------------------------------------------
    delta_tbl = DeltaTable.forName(spark, delta_table_name)

    (delta_tbl.alias("t")
        .merge(
            spark_df.alias("s"),
            "t.id = s.id AND t.customerProgramId = s.customerProgramId"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Upserted {combined_pandas_df.shape[0]} rows into {delta_table_name}")

# ---------------------------------------------------------------------------
# STEP 5: Final record count
# ---------------------------------------------------------------------------
print(f"{delta_table_name} -> Total Records : ", spark.sql(f"SELECT COUNT(*) FROM {delta_table_name}").collect()[0][0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
