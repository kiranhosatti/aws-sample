#pylint: disable-logging-too-many-args
import os
import datetime
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
import sys
import time
import random

import boto3
import watchtower
import psycopg2
import pyspark.sql.types as T
import requests
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, concat_ws, from_json, lit, sha2, when, to_json, schema_of_json
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, TimestampType, StructType, MapType
from awsglue.utils import getResolvedOptions
import traceback
from picklist import process_picklists

#Initialize logging
logger = logging.getLogger() 
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("% (asctime) s - %(name)s - %(levelname)s - %(message) s") 
handler.setFormatter(formatter)
logger.addHandler(handler)

#Initialize AWS clients
S3_client = boto3.client ("s3")
secrets_manager_client = boto3.client ("secretsmanager")
glue_client = boto3.client("glue")
LAMBDA_CLIENT = boto3.client("lambda")

errors = {}
secret_errors = []

def fetch_secret (secret_name):
    """
        Fetches a secret from AWS Secrets Manager.

        Args:
        secret_name (str): The name of the secret to fetch.

        Returns:
            dict: The secret value as a dictionary.

        Raises:
         ClientError: If there is an error fetching the secret from AWS Secrets Manager."
    """

    try:
        response = secrets_manager_client.get_secret_value(SecretId=secret_name) 
        secret = json. loads (response["SecretString"])
        return secret
    except Exception as e:
        logger.error("Error fetching secret: %s", e)
        error_trace = traceback.format_exc ()
        secret_errors.append(str(error_trace))
        raise e

def raise_incident(all_configs, err_resp):
    try:
        lc = boto3.client("lambda")
        audit_params=all_configs["audit_params"] 
        snow_params= all_configs["snow_params"] 
        #Update audit_params with error message 
        audit_params.update({"error_message": err_resp})
        audit= logger.info (f"updated audit_params: {audit_params}")
         #Merge payloads
        snow_payload = {**snow_params, **audit_params}      

        invoke_response = lc.invoke(FunctionName=all_configs["snowlambdaname"], InvocationType="RequestResponse", Payload=json.dumps (snow_payload)) 
        payload_response = invoke_response["Payload"].read().decode()
        try:
            inc = json.loads(payload_response) 
        except json.JSONDecodeError:
            inc = payload_response # If not JSON, return raw string 
        logger.info(f"Incident#: {inc}")
        logger.info(f"Full Response: {invoke_response}")
        return inc
        
    except Exception as err:
        errors.append(str(err)) #Ensure errors are stored as strings
        logger.error("Error invoking Snow Lambda: %s", err) 
        return None
        
# Function to extract the "data" field from JSON columns using getItem()
def extract_json_data (df, json_columns, object_name):
     """  
        Extracts the data feild from json columns.

        Args: df created from parquet file and json_columns


        Returns:
        df: with cleaned jsonb columns.
    """
     try:
        for col_name in json_columns:
            df = df.withColumn (col_name, col(col_name)["data"])
        return df
     except Exception as e:
        logger.error("Error extracting data from nested json: %s", e) 
        error_trace = traceback.format_exc()
        errors[object_name] = str(error_trace)
        raise e


def log_error_details (step_function_id, glue_job_name, failures):
    """
    Inserts error details into the database."

    Args:
        step_function_id (str): Step function ID. 
        glue_job_name (str): Glue job name.
        error_details (list): List of dictionaries with error details.

    Returns:
        None
    """
    try:
        logger.debug("START Error details insert")
        db_secret_dict = fetch_secret(all_configs["db"]["secret_name"]) 
        conn = connect_to_db(db_secret_dict)
    
        query = (all_configs["db"]["error_table_insert_query"]).format(vals="%s, %s, %s , %s )") 
    
        cur = conn.cursor()
        for object_name, error_details in failures.items():
            error_details_json  = json.dumps(error_details, default=str) # Convert error details to JSON
            values = (step_function_id, glue_job_name, object_name, error_details_json) 
            cur.execute (query, values)

        conn.commit()
        cur.close()
        conn.close()
        logger.info("Error details inserted successfully into error table")

    except Exception as e:
        logger.error("Error logging error details: %s", e)
        raise err


def persist_data_as_hudi(spark: Any, db_config: dict, s3_bucket_name: str, s3_bucket_basepath: str, s3_bucket_hudipath: str, object_name: str, ods_object_name, db_secret) -> None:
    print(f"Persisting data as Hudi for (object_name)")
   
    try:
        dbtable = f"{db_config['schema_name']}.{ods_object_name}" 
        connection = connect_to_db(db_secret)
        cursor = connection.cursor()
        cursor.execute(db_config["batch config query"], (object_name,))
        result =  cursor.fetchone()
        last_modified_date, limit, offset,parquet_schema, hudi_incremental_run_values_str = result[0], result[1], result[2], result[3], result[4]
        logger.info("%s: Last Modified Date: %s, Limit: %s, Offset: %s, incremental_run_values: %s", object_name, last_modified_date, limit, offset,hudi_incremental_run_values_str)
        last_modified_date = last_modified_date.strftime ("%Y-%m-%dT%H:%M:%S.000Z") 
        cursor.close()
        hudi_incremental_run_values = json.loads (hudi_incremental_run_values_str) 
        s3_base_path = f"s3a://{s3_bucket_name}/{s3_bucket_basepath}/{object_name}/" + f"dl_year={hudi_incremental_run_values['year_val']}/" + f"dl_month={hudi_incremental_run_values['month_val']}/" + f"d1_day{hudi_incremental_run_values['day_val']}/" + f"dl_hour={hudi_incremental_run_values['hour_val']}/"
        s3_hudi_path = f"s3a://{s3_bucket_name}/{s3_bucket_hudipath}/{object_name}/"
        
        last_commit_time = "0"

        # Check if Hudi path exists to determine full or incremental load
        try:
            existing_hudi_df = spark.read.format("hudi").load(s3_hudi_path) 
            commit_timeline = existing_hudi_df.select("_hoodie_commit_time").distinct ().orderBy(col("_hoodie_commit_time").desc()).limit(1)
            last_commit_time = commit_timeline.first()[0] if commit_timeline.count() > 0 else "0"
            is_first_run = False
            logger.info("%s: Hudi table exists. Last commit: %s", object_name, last_commit_time)
        except Exception as e:
            if "FileNotFoundException" is str(e):
                is_first_run = True      
                logger.info("Hudi table does not exist. Performing full load.")
            else:
                logger.info("Unexpected error checking Hudi table: %s", e)
                error_trace = traceback.format_exc()
                errors[object_name] = str(error_trace)
                raise e

        inferred_schema = T._parse_datatype_string(parquet_schema)

        logger.info("Reading Parquet files from {s3_base_path}")
        s3_df = spark.read.schema(inferred_schema).option("recursiveFileLookup", "true").option("multiline", "true").parquet(s3_base_path)

        # Identify columns that should be converted to JSON
        json_columns = [field.name for field in s3_df.schema.fields if isinstance(field.dataType, StructType)]
        if json_columns:
            s3_df = extract_json_data(s3_df, json_columns, object_name)
        logger.info(f"StructType columns to be converted to json - {json_columns}")
                    
        if not is_first_run:
            hash_columns = [c for c in s3_df.columns if c != "id"]
            hash_columns_exprs = [col(c).cast("string") if dict(s3_df.dtypes)[c] not in ("string", "int", "bigint", "double", "float", "boolean", "timestamp", "date") else col(c) for c in hash_columns]
            s3_df = s3_df.withColumn("record_hash", sha2(concat_ws("|", *hash_columns_exprs), 256))
            existing_hudi_columns = [c for c in hash_columns if c in dict(existing_hudi_df.dtypes)]
            hash_columns_exprs_hudi = [col(c).cast("string") if dict(existing_hudi_df.dtypes).get(c, None) not in ("string", "int", "bigint","double", "float", "boolean", "timestamp", "date") else col(c) for c in existing_hudi_columns]
            existing_hudi_df = existing_hudi_df.select("id", "ods_created_dtm", "ods_last_updated_dtm", "ods_elt_process_flag", *existing_hudi_columns).withColumn("record_hash", sha2(concat_ws("|",  *hash_columns_exprs_hudi), 256))

            existing_hudi_df = broadcast(existing_hudi_df)

            final_df = s3_df.alias("new").join(existing_hudi_df.alias("old"), s3_df.id == existing_hudi_df.id, "left_outer")

            final_df = final_df.withColumn(
                "ods_elt_process_flag", when(col("old.id").isNull(),  lit("I")).when(col("new.record_hash") != col("old.record_hash"),  lit("U")).otherwise(col("old.ods_elt_process_flag"))
            ).withColumn("ods_last_updated_dtm", lit(datetime.now(timezone.utc)))

            selected_columns = [col("new.id")] + [col(f"new.{c}") for c in hash_columns] + [col("ods_created_dtm"), col("ods_last_updated_dtm"), col("ods_elt_process_flag")]
            
            final_df = final_df.select(*selected_columns).drop("record__hash")

        else:
            final_df = s3_df.withColumn("ods_created_dtm", lit(datetime.now(timezone.utc))).withColumn("ods_last_updated_dtm",lit(datetime.now(timezone.utc))).withColumn("ods_elt_process_flag", lit("I"))

        hudi_options = {
            "hoodie.table.name": object_name,
            "hoodie.datasource.write.recordkey.field": "id",
            "hoodie.datasource.write.precombine.field": "ods_last_updated_dtm",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.table.type": "MERGE_ON_READ",
            # Glue Data Catalog sync
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.mode": "hms",
            "hoodie.datasource.write.retry.max.retries": "5",
            "hoodie.datasource.write.retry.interval.ms": "2000",
            "hoodie.datasource.hive_sync.database": db_config['schema_name'],
            "hoodie.datasource.hive_sync.table": ods_object_name,
            "hoodie.datasource.hive_sync.skip_ro_suffix": "true",
            "hoodie.datasource.hive_sync.table_strategy": "SINGLE",
            "hoodie.datasource.hive_sync.use_jdbc": "false",
            "hoodie.datasource.hive_sync.support_timestamp": "true"
        }

        write_mode = "overwrite" if is_first_run else "append"
        final_df.write.format("hudi").options(**hudi_options).mode(write_mode).save(s3_hudi_path)
        logger.info(f"{object_name}: Hudi data persisted with {final_df.count()} records")

        jdbc_url = f"jdbc:postgresql://{db_secret['host']}:{db_secret['port']}/{db_secret['dbname']}"
        db_properties = {"user": db_secret["username"], "password": db_secret["password"], "driver": "org.postgresql.Driver"}

        business_columns = [c for c in final_df.columns if not c.startswith("_hoodie")]
        clean_df = final_df.select(business_columns)

        if is_first_run:
            hudi_options.update({"hoodie.datasource.write.operation": "bulk_insert",  "hoodie.datasource.write.bulk.insert.sort_mode": "NONE"})
            with connect_to_db(db_secret) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {dbtable}")
                    conn.commit()
            logger.info("First run")
                # Convert StructType and MapType columns to JSON strings
            if json_columns:
                for col_name in json_columns:
                    clean_df = clean_df.withColumn(col_name, to_json(col(col_name)))
            clean_df.write.format("jdbc").option("url", jdbc_url).option("dbtable", dbtable).option("batchsize", 10000).option("stringtype", "unspecified").options(**db_properties).mode("append").save()
            with connect_to_db(db_secret) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(db_config["update_hudi_status_query"], ('True', object_name))
                    conn.commit()    
        else:
            incremental_df = spark.read.format("hudi").option("hoodie.datasource.query.type", "incremental").option("hoodie.datasource.read.begin.instanttime", last_commit_time).load(s3_hudi_path)
            clean_incremental_df = incremental_df.select(business_columns)

            temp_table = f"{db_config['schema_name']}.{object_name}_temp_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
            with connect_to_db(db_secret) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM {dbtable} LIMIT 0;")
                    conn.commit()
                ## Convert StructType and MapType columns to JSON strings
            if json_columns:
                for col_name in json_columns:
                    clean_incremental_df = clean_incremental_df.withColumn(col_name, to_json(col(col_name)))
            logger.info("json converted")
            clean_incremental_df.write.format("jdbc").option("url", jdbc_url).option("dbtable", temp_table).option("batchsize", 10000).option("stringtype", "unspecified").option("properties", db_properties).mode("append").save()

            merge_sql = f"""
                INSERT INTO {dbtable} SELECT * FROM {temp_table}
                ON CONFLICT (id) DO UPDATE SET {', '.join([f'{c} = EXCLUDED.{c}::JSONB' if c in json_columns else f'{c} = EXCLUDED.{c}' for c in business_columns if c != 'id'])}
            """

            with connect_to_db(db_secret) as conn:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute(merge_sql)
                        conn.commit()
                        cursor.execute(f"DROP TABLE {temp_table}")
                        conn.commit()
                        cursor.execute(db_config["update_hudi_status_query"], ('True', object_name))
                        conn.commit()
                    except Exception as e:
                        error_trace = traceback.format_exc()
                        logger.info(f"error occured - {error_trace}")           

        
        
        logger.info(f"{object_name}: Data successfully persisted to RDS with {clean_df.count()} records")
    except Exception as e:
        logger.error(f"Unexpected error in persist_data_as_hudi: {e}")
        error_trace = traceback.format_exc()
        errors[object_name] = str(error_trace)
        raise e
    

def spark_to_postgres_type(spark_data_type,object_name):
    """
    Converts Spark data types to PostgreSQL data types.
    Adjust or extend this mapping as needed.
    """
    try:
        if isinstance(spark_data_type, StringType):
            return "TEXT"
        elif isinstance(spark_data_type, IntegerType):
            return "INTEGER"
        elif isinstance(spark_data_type, LongType):
            return "BIGINT"
        elif isinstance(spark_data_type, DoubleType):
            return "DOUBLE PRECISION"
        elif isinstance(spark_data_type, BooleanType):
            return "BOOLEAN"
        elif isinstance(spark_data_type, TimestampType):
            return "TIMESTAMP"
        elif isinstance(spark_data_type, ArrayType):
            #assume array of simple types; this create a postgres array type.
            element_type = spark_to_postgres_type(spark_data_type.elementType, object_name)
            return f"{element_type}[]"
        elif isinstance(spark_data_type, StructType):
            return "JSONB" 
        #Fallback to Text for unsupported types
        return "TEXT"
    except Exception as e:
        error_trace = traceback.format_exc()
        errors[object_name] = str(error_trace)
        logger.error(f"Error converting Spark data type to PostgreSQL type for {object_name}: {e}")
        raise e


def generate_alter_table_statement(new_fields, table_name, object_name):
    """
    Generates ALTER TABLE statements to add new columns from inferred_schema_str to the
    database.
    """
    try:
        alter_queries = []
        for field in new_fields:
            pg_type = spark_to_postgres_type(field.dataType, object_name)  # Convert Spark type to PostgreSQL type
            alter_queries.append(f"ALTER TABLE {table_name} ADD COLUMN {field.name} {pg_type} NULL;")
        return alter_queries
    except Exception as e:
        error_trace = traceback.format_exc()
        errors[object_name] = str(error_trace)
        logger.error("Error in generating Alter query: %s", e)
        raise e

def generate_create_table_statement(schema, table_name, object_name):
    """
    Generates a PostgreSQL CREATE TABLE statement from a Spark StructType schema.
    Assumes 'id' column always exists and is always a BIGINT PRIMARY KEY.
    """
    try:
        columns = []
        for field in schema.fields:
            column_name = field.name
            pg_type = spark_to_postgres_type(field.dataType, object_name)

            if column_name == "id":
                columns.append(f" id{pg_type} PRIMARY KEY")
            else:
                not_null = "NOT NULL" if not field.nullable else ""
                columns.append(f"{column_name} {pg_type} {not_null}")  

        #add additional columns for ETL metadata
        columns.append("ods_elt_process_flag CHAR(1)")
        columns.append("ods_created_dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        columns.append("ods_last_updated_dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

        columns_statement = ",\n".join(columns)
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{columns_statement}\n);"

    except Exception as e:
        error_trace = traceback.format_exc()
        errors[object_name] = str(error_trace)
        logger.error("Error in generating Create table query: %s", e)
        raise e

def fetch_object_records(spark, session_id, veeva_config, s3_config, db_config,object_name, object_fields, vql_query, ods_object_name, incremental_load_column,db_secret, object_type, all_configs):
    """
    Fetches records of a specified object from Veeva Vault, processes them, and store them in S3 as Parquet files.

    Args:
        spark (SparkSession): The Spark session object.
        session_id (str): The session ID for Veeva Vault API authentication.
        veeva_config (dict): Configuration dictionary for Veeva Vault, containing   'vault_url' and 'api_version'.
        s3_config (dict): Configuration dictionary for S3, containing 'bucket' and 'prefix'.
        db_config (dict): Configuration dictionary for the database, containing 'secret_name', 'last_modified_date_query', and 'update_batch_table_query'.
        object_name (str): The name of the object to fetch records for.
        object_fields (str): The fields of the object to fetch.

    Returns:
        None
    """
    try:
        query_url = f"{veeva_config['vault_url']}/api/{veeva_config['api_version']}/query"
        headers = {"Authorization":f"Bearer {session_id}", "Accept": "application/json" ,"Content-Type" : "application/x-www-form-urlencoded"}
        hudi_last_commit_status = "True"
        incremental_run_values = {"year_val": None, "month_val": None, "day_val": None, "hour_val": None}
        incremental_run_values_str = json.dumps(incremental_run_values)

        #Get database connection details from secrets manager
        connection = connect_to_db(db_secret)

        #Fetch last modified date ,limit and offset from database
        cursor = connection.cursor()
        cursor.execute(db_config["batch_config_query"], (object_name,))
        result = cursor.fetchone() #returns first row of the result set
        logger.info(f"logger:{result}")
        parquet_schema = None
        incremental_clause = ""
        where_clause = db_config["where_clause"]
        if result:
            last_modified_date, limit, offset, parquet_schema = result[0], result[1], result[2], result[3]
            if last_modified_date.strftime("%Y-%m-%dT%H:%M:%S.000") == "1900-01-01 00:00:00.000":
                sleep_duration = random.uniform(1, 5)
            else:
                sleep_duration = random.uniform(0.1 , 0.5 )
            last_modified_date_forvql = datetime.strftime(result[0], "%Y-%m-%dT%H:%M:%S.000Z")
            incremental_clause = f" WHERE {incremental_load_column} > '{{last_modified_date_for_vql}}'"
            if object_type == 'documents':
                incremental_clause = f"{incremental_clause} AND {where_clause})"
        cursor.close()
           
        if object_fields:
            vql_query = f"SELECT {object_fields} FROM {object_name}"
            logger.info("VQL Query object_fields %s:", vql_query)
        vql_query = f"({vql_query}) {incremental_clause}"
        logger.info("VQL Query: %s", vql_query)
        has_more = True
        first_attempt = True # schema inference is performed only once

        while has_more:
            data = {"q": vql_query, "limit": limit, "offset": offset}
            logger.info("data:{data}")
            try:
                response = requests.post(query_url, headers=headers, data=data, timeout=10)
                status = response.raise_for_status() # for non-200 status an exception is raised.
                logger.info("status: %s", status)
                data = response.json()
                if data.get("error", []):
                    errors[object_name] = str(f"invalid syntax in vql query - {data.get('errors', [])}")
                    return incremental_run_values, hudi_last_commit_status
                logger.info("Fetched %d records for object %s", len(data.get("data", [])), object_name)
                records = data.get("data", []) # if data exists, returns data values or []
            except requests.exceptions.RequestException as e:
                logger.error("Error fetching records: %s", e)
                errors[object_name] = str(f"Error fetching records: {e}")
                return incremental_run_values, hudi_last_commit_status
                raise 
        
            if records:
                json_strings = [json.dumps(record) for record in records]
                raw_df = spark.createDataFrame(json_strings, "string").toDF("json_string")
                logger.info(f"raw df count = {raw_df.count()}")
                if first_attempt:
                    sample_json = raw_df.select("json_string").first()[0]
                    schema_json_df = spark.range(1).selectExpr(f"from_json(lit('{sample_json}'), 'schema') as schema")
                    schema_str = schema_json_df.first()["schema"]
                    inferred_schema = T._parse_datatype_string(inferred_schema_str)
                    logger.info("schema extracted from the raw df")
                    
                    if object_fields:
                        original_field_order = object_fields.split(",")
                        inferred_schema = [field for name in original_field_order for field in inferred_schema.fields if field.name == name]
                        inferred_schema = StructType(inferred_schema)
                    create_table_ddl = generate_create_table_statement(inferred_schema, f"{db_config['schema']}.{ods_object_name}", object_name)
                    
                    logger.info("PostgreSQL CREATE TABLE Statement:\n" + create_table_ddl)
                    logger.info(create_table_ddl)
                    cursor = connection.cursor()
                    cursor.execute(create_table_ddl)    
                    connection.commit()
                    cursor.close()
                    first_attempt = False
                
                    if parquet_schema:
                        # Compare parquet_schema (DB) and inferred_schema_str (veeva)
                        db_parquet_schema = T._parse_datatype_string(parquet_schema)
                        # Assume inferred_schema and db_parquet_schema are StructType
                        new_fields = [field for field in inferred_schema.fields if field not in db_parquet_schema.fields]
                        new_columns = [field.name for field in new_fields]
                        if new_fields:
                            logger.info(f"schemas are different, new columns to be added {new_columns}")
                            alter_table_ddl = generate_alter_table_statement(new_fields, f"{db_config['schema_name']}.{ods_object_name}", object_name)
                            cursor = connection.cursor()
                            for query in alter_table_ddl:
                                logger.info(f"Executing: {query}")
                                cursor.execute(query)
                                connection.commit()
                            cursor.close()
                            err_details = f"New columns {new_columns} has been added to {ods_object_name} in {db_config['schema_name']} schema, please update Metadata Manager "
                            INC_NUMBER = raise_incident(all_configs, err_details)
                            if INC_NUMBER != "":
                                logger.info(f"Incident created in Service Now for metadata manager update. Incident number is {INC_NUMBER}")
                                # logger.info("Schemas are different. Ticket raised to support to update Metadata.")
                    parquet_schema = inferred_schema_str

                # Parse JSON strings using inferred schema
                batch_df = raw_df.withColumn("data", from_json(col("json_string"), inferred_schema)).select("data.*")

                # Extract year, month, and day from last_modified_date
                incremental_run_values["year_val"] = last_modified_date.year
                incremental_run_values["month_val"] = last_modified_date.month
                incremental_run_values["day_val"] = last_modified_date.day
                incremental_run_values["hour_val"] = last_modified_date.hour

                incremental_run_values_str = json.dumps(incremental_run_values)

                # Generate a unique file name using timestamp + batch offset
                timestamp_str = datetime.now(timezone.utc).strftime("%H%M%S")  # Format: HHMMSS
                s3_path = f"s3a://{s3_config['bucket']}/{s3_config['prefix']}/{object_name}/dl_year={incremental_run_values['year_val']}/"f"dl_month={incremental_run_values['month_val']}/dl_day={incremental_run_values['day_val']}/"f"dl_hour={incremental_run_values['hour_val']}/batch_{offset}={timestamp_str}.parquet"

                # Write Parquet file in Append mode
                batch_df = batch_df.withColumn("dl_year", lit(incremental_run_values["year_val"])) 
                batch_df = batch_df.withColumn("dl_month", lit(incremental_run_values["month_val"])) 
                batch_df = batch_df.withColumn("dl_day", lit(incremental_run_values["day_val"])) 
                batch_df = batch_df.withColumn("dl_hour", lit(incremental_run_values["hour_val"])) 
                time.sleep(sleep_duration)  # Sleep for a random duration between 1 and 5 seconds       
                batch_df.write.mode("append").partitionBy("dl_year", "dl_month", "dl_day", "dl_hour").parquet(s3_path)
                logger.info(f"file written:- {s3_path}")

               
            response_details = data.get("responseDetails", {})
            has_more = response_details.get("next_page") is not None
            if has_more:
                query_url = f"{veeva_config['vault_url']}/{response_details['next_page']}"
            offset += limit

            # Update the batch_table with the current date, limit, and offset

            cursor = connection.cursor()
            cursor.execute(db_config["update_batch_table_query"],(last_modified_date, limit, offset, parquet_schema, incremental_run_values_str,  hudi_last_commit_status, object_name))
            connection.commit()
            cursor.close()

        
        # Update the batch_table after all records are uploaded to S3
        logger.info(f"parquet schema = {parquet_schema}")
        cursor = connection.cursor()
        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        limit = 1000
        offset = 0
        if incremental_run_values["year_val"]:
            hudi_last_commit_status = "False"
        cursor.execute(db_config["update_batch_table_query"], (current_date, limit,offset, parquet_schema, incremental_run_values_str, hudi_last_commit_status,object_name))
        connection.commit()
        cursor.close()
        connection.close()

        return incremental_run_values, hudi_last_commit_status
    
    except Exception as e:
        logger.error("Unexcepted error in fetching records from veeva and store in s3(parque): %s" , e )
        error_trace = traceback.format_exc()
        errors[object_name] = str(error_trace)
        raise e


def get_object_metadata(object_name , object_type, veeva_config, session_id):
    """
    Fetches metadata for a specified object from Veeva Vault.

    Args:
        object_name (str): The name of the object to fetch metadata for.
        object_type (str): The type of the object (e.g., 'documents', 'objects').
        veeva_config (dict): Configuration dictionary for Veeva Vault, containing 'vault_url' and 'api_version'.
        session_id (str): The session ID for Veeva Vault API authentication.

    Returns:
        dict: A dictionary containing the metadata fields and their types.

    Raises:
        requests.exceptions.RequestException: If an errors occurs while making the HTTP Request   KeyError: If the expected keys are not found in the response.
    """     
    try:
        logger.info(f"Fetching metadata for object_name")
        headers = {"Authorization": f"Bearer {session_id}", "Accept": "application/json"}
        if object_type == 'object':
            metadata_url = f"{veeva_config['vault_url']}/api/{veeva_config['api_version']}/metadata/vobjects/{object_name}"
            metadata_response = requests.get(metadata_url, headers=headers, timeout=10)
            metadata = metadata_response.json()
            fields = metadata["object"]["fields"]
            object_fields = [field["name"] for field in fields]
            for field in object_fields:
                if '.' in field:
                    if field.split('.')[0] not in object_fields:
                        object_fields.append(field.split('.')[0])
            object_fields = ".".join(field for field in object_fields)

        elif object_type == 'documents':
            metadata_url = f"{veeva_config['vault_url']}/api/{veeva_config['api_version']}/metadata/objects/{object_name}/properties"
            logger.info(f"url for metadata {metadata_url}")
            metadata_response = requests.get(metadata_url, headers=headers, timeout=10)
            metadata = metadata_response.json()
            if metadata.get("responseStatus") == "SUCCESS" and "properties" in metadata:
                queryable_fields = [prop["name"] for prop in metadata["properties"] if prop.get("queryable")]
                for field in queryable_fields:
                    if '.' in field:
                        if field.split('.')[0] not in queryable_fields:
                            queryable_fields.append(field.split('.')[0])
                object_fields = ".".join(field for field in queryable_fields)
                logger.info("Queryable fields: %s", queryable_fields)
            else:
                logger.error("Unexpected response structure: %s", metadata)

        return object_fields
    
    except Exception as e:
        error_trace = traceback.format_exc()
        errors[object_name] = str(error_trace)
        logger.error("Error in Fetching Object Metadata: %s", e)
        raise e


def process_batch(spark, session_id, batch_object_name, veeva_config, s3_config, db_config, all_configs, db_secret):
    """
    Processes a batch of entries by fetching object records from Veeva and storing them.

    Args:
        spark (SparkSession): The Spark session object.
        session_id (str): The session ID for the current process.
        batch_config (list): A list of dictionaries containing batch configuration. Each dictionary should have an "object_name" key.
        veeva_config (dict): Configuration dictionary for Veeva.
        s3_config (dict): Configuration dictionary for S3.
        db_config (dict): Configuration dictionary for the database.

    Raises:
        KeyError: If there is an error processing the batch due to a missing key.
    """

    try:
        logger.info("Processing batch: %s", batch_object_name)
        connection = connect_to_db(db_secret)
        cursor = connection.cursor()
        cursor.execute(db_config["batch_config_query"], (batch_object_name,))
        result = cursor.fetchone()  # returns first row of the query result set
        if result is None:
            logger.error(f"No record found in batch_table_new for object_name: {batch_object_name}")
            errors[batch_object_name] = str(error_trace)
            return errors[batch_object_name]
        else:
              object_type, ods_object_name, incremental_load_column, vql_query, hudi_last_commit_status = result[5], result[6], result[7], result[8], result[9]

        if object_type == "picklist":
                process_picklists(logger, batch_object_name, session_id, db_config, veeva_config, f"{db_config['schema_name']}.{ods_object_name}", all_configs["ssm_parameter_name"])
      
        elif hudi_last_commit_status == "True":
            if vql_query:
                incremental_run_values, hudi_last_commit_status = fetch_object_records(spark, session_id, veeva_config, s3_config, db_config, batch_object_name,  None, vql_query, ods_object_name, incremental_load_column, db_secret, object_type, all_configs)
                logger.info(f"Fetched incremental_run_values: {incremental_run_values}")
            else:        
                object_fields = get_object_metadata(batch_object_name, object_type, session_id, veeva_config)
                logger.info("Processing batch_entry: %s  object_fields: %s", ods_object_name, object_fields)
                incremental_run_values, hudi_last_commit_status = fetch_object_records(spark, session_id,veeva_config, s3_config, db_config, batch_object_name, object_fields, None, ods_object_name, incremental_load_column, db_secret, object_type, all_configs)

        if object_type != "picklist" and (hudi_last_commit_status == "False"):
            persist_data_as_hudi(spark, db_config, s3_config["bucket"], s3_config["prefix"], s3_config["s3_bucket_hudipath"], batch_object_name, ods_object_name, db_secret)

    except Exception as e:
        error_trace = traceback.format_exc()
        errors[batch_object_name] = str(error_trace)
        logger.error("Error processing batch: %s", e)
        return errors[batch_object_name]        


def process_batches(spark, session_id, batch_details, veeva_config, s3_config, db_config, all_configs, db_secret):
    """
    Processes multiple batches concurrently using threading.

    Args:
        spark (SparkSession): The Spark session object.
        session_id (str): The session identifier.
        batch_config (dict): Configuration for the batches to be processed.
        veeva_config (dict): Configuration for Veeva.
        s3_config (dict): Configuration for S3.
        db_config (dict): Configuration for the database.

    Returns:
        None

    Raises:
        ValueError: If batch_config is missing or invalid.

    Logs:
        Error: If batch configuration is missing.
    """
    try:
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(process_batch, spark, session_id, batch, veeva_config, s3_config, db_config, all_configs, db_secret): batch for batch in batch_details}
            for future in as_completed(futures):
                result = future.result()
                logger.info("Completed load: %s", result)
                logger.info("Completed load %s", futures[future])

    except Exception as e:
        error_trace = traceback.format_exc()
        errors["Batch_failure"] = str(error_trace)
        logger.error("Error in Batch configuration: %s", e)
        raise e    


def get_veeva_session_id(vault_url, api_version, username, secret_name):
    """
    Authenticate with the Veeva Vault API and retrieve a session ID.

    Args:
        vault_url (str): The base URL of the Veeva Vault instance.
        api_version (str): The version of the Veeva Vault API to use.
        username (str): The username for authentication.
        secret_name (str): The name of the secret containing the password.

    Returns:
        str: The session ID obtained from the Veeva Vault API.

    Raises:
        requests.exceptions.RequestException: If there is an issue with the HTTP request.
        KeyError: If the response does not contain the expected session ID.
    """
    try:
        auth_url = f"{vault_url}/api/{api_version}/auth"
        password = fetch_secret(secret_name)["password"]
        auth_response = requests.post(auth_url, data={"username": username, "password": password}, timeout=10)
        auth_data = auth_response.json()
        session_id = auth_data["sessionId"]

        return session_id

    except Exception as e:
        error_trace = traceback.format_exc()
        errors["Batch_failure"] = str(error_trace)
        logger.error("Error in Fetching Veeva SessionID: %s", e)
        raise e
    

def connect_to_db(sm_value):
    """
    Establishes a connection to a PostgreSQL database using the provided connection parameters.

    Args:
        sm_value (dict): A dictionary containing the database connection parameters:
            - host (str): The hostname of the database server.
            - port (int): The port number on which the database server is listening.
            - dbname (str): The name of the database.
            - username (str): The username to connect to the database.
            - password (str): The password to connect to the database.

    Returns:
        connection: A connection object to the PostgreSQL database.

    Raises:
        Exception: If there is an error connecting to the database, an exception is raised and logged.
    """
    
    try:
        connection = psycopg2.connect( host=sm_value["host"], port=sm_value["port"], dbname=sm_value["dbname"],user=sm_value["username"], password=sm_value["password"])
        return connection
    except Exception as exp_msg:
        #erros.append(e)
        logger.error("Error connecting to the database: %s", exp_msg)
        error_trace = traceback.format_exc()
        secret_errors.append(str(error_trace))
        raise exp_msg


def audit_batch_exec_id(batch):
    """
    Function to audit new batch job
    Parameters:
        batch (string): Batch name
    Return:
        batch_execution_id (int): New batch execution id
    """
   
    batch_payload = {"operation_type": "insert", "status": "R", "batch_name": batch}

    batch_response = invoke_lambda("edb_abc_log_batch_status", batch_payload)
    
    try:
        batch_execution_id = json.loads(batch_response["Payload"].read().decode("utf-8"))["batch_execution_id"]
    except KeyError as batch_error:
        err = f"Couldn't create batch_execution_id for batch: {batch}"
        logger.error(err)
        error_trace = traceback.format_exc()
        errors["Batch_failure"] = str(error_trace)
        raise batch_error
    return batch_execution_id


def audit_job_exec_id(batch_exec_id, job):
    """
    Function to audit new job
    Parameters:
        batch_exec_id (string): Batch execution id
        job (string): Job name
    Return:
        process_execution_id (int): New job execution id
    """
  
    job_payload = { "operation_type": "insert", "status": "R", "batch_execution_id": batch_exec_id, "job_name": job}

    job_response = invoke_lambda("edb_abc_log_process_status", job_payload)

    try:
        process_execution_id = json.loads(    job_response["Payload"].read().decode("utf-8"))["process_execution_id"]
    except KeyError as audit_error:
        msg = f"Couldn't create batch_execution_id for job {job}"
        logger.error(msg)
        error_trace = traceback.format_exc()
        errors["Batch_failure"] = str(error_trace)
        raise audit_error

    return process_execution_id


def audit_batch_status(batch_exec_id, status):
    """
    Function to audit batch status
    Parameters:
        batch_exec_id (string): Batch execution id
        status (string): Job status
    Return:
        None
    """

    batch_audit_payload = {"operation_type": "update", "status": status, "batch_execution_id": batch_exec_id}

    invoke_lambda("edb_abc_log_batch_status", batch_audit_payload)


def audit_job_status(batch_exec_id, process_exec_id, status, job_error=None):
    """
    Function to audit job status
    Parameters:
        batch_exec_id (string): Batch execution id
        process_execution_id (int): Job execution id
        status (string): Job status
        job_error (string): Job error details
    Return:
        None
    """

    job_audit_payload = { "operation_type": "update", "status": status, "batch_execution_id": batch_exec_id, "process_execution_id": process_exec_id }

    if status == "S":
        invoke_lambda("edb_abc_log_process_status", job_audit_payload)
    elif status == "F":
        job_audit_payload["exception_message"] = job_error
        invoke_lambda("edb_abc_log_process_status", job_audit_payload)


def invoke_lambda(func_name, func_payload):
    """
    Function to invoke lambda function
    Parameters:
        func_name (string): Lambda function name
        func_payload (dict): Lambda function payload
    Return:
        invoke_response (dict): Lambda function response
    """

    try:
        invoke_response = LAMBDA_CLIENT.invoke(  FunctionName=func_name, InvocationType="RequestResponse", Payload=json.dumps(func_payload).encode("utf-8"))
    except Exception as invoke_error:
        error_trace = traceback.format_exc()
        errors["Batch_failure"] = str(error_trace)
        raise invoke_error

    return invoke_response


def main(all_configs):
    """
    Main function to initialize Spark session and process batches.

    This function performs the following steps:
    1. Initializes a Spark session with specific configurations for S3.
    2. Loads configuration settings from a JSON file.
    3. Retrieves a Veeva session ID.
    4. Processes batches using the Spark session and configuration settings.
    5. Stops the Spark session.

    Note:
        The Veeva session ID is currently hardcoded for testing purposes.

    Raises:
        FileNotFoundError: If the configuration file is not found.
        json.JSONDecodeError: If there is an error decoding the JSON configuration file.
    """
    
    logger.info("Main block – Start")
    job_name = args["JOB_NAME"]
    try:
        spark = (
            SparkSession.builder.appName("VeevaVaultHudi")
            .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.0,""org.apache.hadoop:hadoop-aws:3.3.3,"     "org.postgresql:postgresql:42.5.1")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("datanucleaus.schema.autoCreateTables" , "True") # Enable auto creation
            .config("hoodie.datasource.write.recordkey.field", "id") # Primary Key
            # Fix S3E Issues
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.committer.name", "directory")
            .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
            .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
            .config("spark.hadoop.fs.s3a.connection.maximum", "2000")
            #Hudi Hive Sync to AWS Glue
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.executor.log.level", "DEBUG")
                     # Fix Java Instrumentation
            .config("spark.driver.extraJavaOptions", "-Djdk.attach.allowAttachSelf=true")
            .config("spark.executor.extraJavaOptions", "-Djdk.attach.allowAttachSelf=true")
            .getOrCreate() 
        )

        session_id = get_veeva_session_id(   all_configs["veeva"]["vault_url"],  all_configs["veeva"]["api_version"],   all_configs["veeva"]["username"],all_configs["veeva"]["secret_name"]    )
        logger.info("Session id: %s", session_id)

        db_secret = fetch_secret(all_configs["db"]["secret_name"])

        process_batches( spark, session_id, batch_list, all_configs["veeva"], all_configs["s3"], all_configs["db"], all_configs, db_secret )
        spark.stop()
        audit_job_status(batch_exec_id, process_exec_id, "S", job_error=None)
        audit_batch_status(batch_exec_id, "S")

    except Exception as exp_msg:
        error_trace = traceback.format_exc()
        errors["Batch_failure"] = str(error_trace)
        logger.info("errors: %s", errors)
        logger.error("Error in Main function: %s", error_trace)

    finally:
        now = datetime.now()
        date_time_str = now.strftime("%Y_%m_%d_%H_%M_%S")
        if len(secret_errors) > 0:
            err_resp =   f"MDIDS Glue job execution failed.\n" f"Glue Job Name: {job_name}\n"f"Job Run Id: {job_run_id}\nError \
               Details:\n{str(secret_errors[0])}\nPlease see the CloudWatch logs for more details."
            INC_NUMBER = raise_incident(all_configs, err_resp)
            audit_job_status(batch_exec_id, process_exec_id, "F", job_error=secret_errors)
            audit_batch_status(batch_exec_id, "F")
        if errors and len(errors) > 0:
            logger.info(f"error variable = {errors}")
            log_error_details(step_function_id, job_name, errors)
            audit_job_status(batch_exec_id, process_exec_id, "F", job_error=errors)
            audit_batch_status(batch_exec_id, "F")

def get_jr_id(job_name):
    try:
        glue_client = boto3.client("glue")
        response = glue_client.get_job_runs(JobName=job_name)
        return response["JobRuns"][0]["Id"]

    except Exception as exp_msg:
        error_trace = traceback.format_exc()
        errors["Batch_failure"] = str(error_trace)
        logger.error(error_trace)
        raise exp_msg
    
if __name__ == "__main__":
    try:
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "sourceConfigFile", "batch_details", "step_function_id", "bucket"])
        print("sys.argv:", sys.argv)

        batch_details = args["batch_details"]
        batch_list = json.loads(batch_details)
        bucket = args["bucket"]
        step_function_id = args["step_function_id"]
        print(f"batch_list = {type(batch_list)}, {batch_list}")

        job_name = args["JOB_NAME"]
        job_run_id = get_jr_id(job_name)
        os.environ["log_stream"] = f"{job_name}_{job_run_id}"

        LOG_MSG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
        LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

        logging.basicConfig(format=LOG_MSG_FORMAT, datefmt=LOG_DATE_FORMAT)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        logger.addHandler(
            watchtower.CloudWatchLogHandler(
                log_group="/aws-mdids-ihub-loggroup/veeva_rim_cc/glue/output",
                log_stream_name=os.environ["log_stream"]
            )
        )

        all_configs = {}
        config_file_path = args["sourceConfigFile"]
        bucket_name = config_file_path.split("/")[2]
        file_name = config_file_path[config_file_path.find(bucket_name) + len(bucket_name) + 1:]
        logger.info("file_name: %s", file_name)
        encoding = "utf-8"
        obj = S3_client.get_object(Bucket=bucket_name, Key=file_name)
        json_content = obj["Body"].read().decode(encoding)
        all_configs = json.loads(json_content)
        all_configs["s3"]["bucket"] = bucket
        batch_exec_id = audit_batch_exec_id(all_configs["batch_name"])
        process_exec_id = audit_job_exec_id(batch_exec_id, all_configs["job_name"])
        logger.info("all_configs: %s", all_configs)
        main(all_configs)

    except Exception as exp_msg:
        error_trace = traceback.format_exc()
        logger.error("An Exception occured :\n%s", error_trace)
        raise exp_msg

                
