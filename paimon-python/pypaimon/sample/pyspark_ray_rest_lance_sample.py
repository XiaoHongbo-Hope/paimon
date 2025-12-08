################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

from pyspark.sql import SparkSession
import ray
from pypaimon import CatalogFactory

# Configuration constants
CATALOG_NAME = "paimon"
REST_CATALOG_URI = "http://cn-hangzhou-vpc.dlf.aliyuncs.com"
WAREHOUSE = "ray_poc"
DLF_REGION = "cn-hangzhou"
DLF_ACCESS_KEY_ID = "xxxx"
DLF_ACCESS_KEY_SECRET = "xxx"

# JAR file paths
PAIMON_SPARK_JAR = "paimon-spark-3.5_2.12-1.4-SNAPSHOT.jar"
PAIMON_OSS_JAR = "paimon-oss-1.4-SNAPSHOT.jar"
PAIMON_LANCE_JAR = "paimon-lance-1.4-SNAPSHOT.jar"
PAIMON_ARROW_JAR = "paimon-arrow-1.4-SNAPSHOT.jar"
LANCE_CORE_JAR = "lance-core-0.39.0.jar"
JAR_JNI_JAR = "1.1.1/jar-jni-1.1.1.jar"
ARROW_C_DATA_JAR = "arrow-c-data-15.0.0.jar"


def create_table_with_lance_format(spark: SparkSession, catalog_name: str = CATALOG_NAME):
    spark.sql(f"USE {catalog_name}.default")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.default.lance_table (
        id INT,
        name STRING,
        value DOUBLE,
        category STRING
    ) USING PAIMON
    TBLPROPERTIES (
        'bucket' = '2',
        'primary-key' = 'id',
        'file.format' = 'lance'
    )
    """
    spark.sql(create_table_sql)

    insert_sql = f"""
    INSERT INTO {catalog_name}.default.lance_table
    VALUES
        (1, 'Alice', 10.5, 'A'),
        (2, 'Bob', 20.3, 'B'),
        (3, 'Charlie', 30.7, 'A'),
        (4, 'David', 40.1, 'B'),
        (5, 'Eve', 50.9, 'A')
    """
    spark.sql(insert_sql)
    spark.sql(f"SELECT * FROM {catalog_name}.default.lance_table").show()


def read_with_ray(catalog_name: str = CATALOG_NAME):
    ray.init(ignore_reinit_error=True)

    catalog = CatalogFactory.create({
        'metastore': 'rest',
        'uri': REST_CATALOG_URI,
        'warehouse': WAREHOUSE,
        'dlf.region': DLF_REGION,
        "token.provider": "dlf",
        'dlf.access-key-id': DLF_ACCESS_KEY_ID,
        'dlf.access-key-secret': DLF_ACCESS_KEY_SECRET,
    })
    
    table = catalog.get_table('default.lance_table')
    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    table_read = read_builder.new_read()
    splits = table_scan.plan().splits()
    
    ray_dataset = table_read.to_ray(splits)
    print(f"Ray Dataset count: {ray_dataset.count()}")
    
    filtered_dataset = ray_dataset.filter(lambda row: row['value'] > 30.0)
    print(f"Filtered count (value > 30): {filtered_dataset.count()}")
    
    result_pandas = ray_dataset.to_pandas()
    print(result_pandas)
    
    ray.shutdown()


def main():
    jars = [
        PAIMON_SPARK_JAR,
        PAIMON_OSS_JAR,
        PAIMON_LANCE_JAR,
        PAIMON_ARROW_JAR,
        LANCE_CORE_JAR,
        JAR_JNI_JAR,
        ARROW_C_DATA_JAR,
    ]

    builder = (
        SparkSession.builder.appName("PaimonPySparkRayLanceExample")
        .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.paimon.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.metastore", "rest")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", REST_CATALOG_URI)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.token.provider", "dlf")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.dlf.region", DLF_REGION)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.dlf.access-key-id", DLF_ACCESS_KEY_ID)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.dlf.access-key-secret", DLF_ACCESS_KEY_SECRET)
    )
    builder = builder.config("spark.jars", ",".join(jars))
    spark = builder.getOrCreate()
    
    create_table_with_lance_format(spark)
    read_with_ray()


if __name__ == "__main__":
    main()

