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


def example_read_write_operations(spark: SparkSession, catalog_name: str = "paimon"):
    spark.sql(f"USE {catalog_name}.default")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.default.example_table (
        id INT,
        name STRING,
        value DOUBLE,
        dt STRING
    ) USING PAIMON
    PARTITIONED BY (dt)
    TBLPROPERTIES (
        'bucket' = '2',
        'primary-key' = 'id,dt'
    )
    """
    spark.sql(create_table_sql)

    insert_sql = f"""
    INSERT INTO {catalog_name}.default.example_table
    VALUES
        (1, 'Alice', 10.5, '2024-01-01'),
        (2, 'Bob', 20.3, '2024-01-01'),
        (3, 'Charlie', 30.7, '2024-01-02'),
        (4, 'David', 40.1, '2024-01-02')
    """
    spark.sql(insert_sql)

    query_sql = f"""
    SELECT * FROM {catalog_name}.default.example_table
    ORDER BY id
    """
    result = spark.sql(query_sql)
    result.show()

    filter_query = f"""
    SELECT * FROM {catalog_name}.default.example_table
    WHERE dt = '2024-01-01' AND value > 15.0
    """
    spark.sql(filter_query).show()

    upsert_sql = f"""
    INSERT INTO {catalog_name}.default.example_table
    VALUES
        (1, 'Alice Updated', 11.5, '2024-01-01'),
        (5, 'Eve', 50.9, '2024-01-03')
    """
    spark.sql(upsert_sql)

    spark.sql(f"SELECT * FROM {catalog_name}.default.example_table ORDER BY id").show()


def example_dataframe_operations(spark: SparkSession, catalog_name: str = "paimon"):
    df = spark.table(f"{catalog_name}.default.example_table")
    df.show()

    filtered_df = df.filter(df.value > 20.0).select("id", "name", "value")
    filtered_df.show()

    filtered_df.write.format("paimon").mode("overwrite").option(
        "primary-key", "id"
    ).saveAsTable(f"{catalog_name}.default.filtered_table")


def main():
    paimon_spark_jar = "paimon-spark-3.5_2.12-1.4-SNAPSHOT.jar"
    paimon_oss_jar = "paimon-oss-1.4-SNAPSHOT.jar"

    builder = (
        SparkSession.builder.appName("PaimonRESTCatalogExample")
        .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
        .config(f"spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
        .config(f"spark.sql.catalog.paimon.metastore", "rest")
        .config(f"spark.sql.catalog.paimon.uri", "http://cn-hangzhou-vpc.dlf.aliyuncs.com")  # Replace with actual server URL)
        .config(f"spark.sql.catalog.paimon.warehouse", "ray_poc")  # Replace with actual catalog name)
        .config(f"spark.sql.catalog.paimon.token.provider", "dlf")
        .config(f"spark.sql.catalog.paimon.dlf.region", "cn-hangzhou")  # Replace with actual region
        .config(f"spark.sql.catalog.paimon.dlf.access-key-id", "xxx")  # Replace with actual access key ID
        .config(f"spark.sql.catalog.paimon.dlf.access-key-secret", "xxx")  # Replace with actual access key secret
    )
    builder = builder.config("spark.jars", paimon_spark_jar + "," + paimon_oss_jar)
    spark = builder.getOrCreate()
    example_read_write_operations(spark)
    example_dataframe_operations(spark)


if __name__ == "__main__":
    main()
