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
"""
Example: REST Catalog + Ray Sink + OSS JSON Data Source

Demonstrates:
1. Reading JSON data from OSS using Ray Data
2. Writing data to Paimon table using Ray Sink (write_ray)
3. REST catalog integration for production scenarios
"""

import json
import tempfile
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.tests.rest.rest_server import RESTCatalogServer
from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider


def create_sample_json_file(local_path: str, num_records: int = 100):
    data = []
    for i in range(1, num_records + 1):
        data.append({
            'id': i,
            'name': f'Item_{i}',
            'category': ['A', 'B', 'C'][i % 3],
            'value': 10.5 + i * 2.3,
            'score': 50 + i * 5,
            'timestamp': f'2024-01-{i % 28 + 1:02d}T10:00:00Z'
        })
    
    with open(local_path, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')  # JSONL format
    
    print(f"Created sample JSON file with {num_records} records: {local_path}")


def create_oss_filesystem_for_ray(
    endpoint: str,
    access_key_id: str,
    access_key_secret: str,
    bucket: str = None,
    region: str = None,
    session_token: str = None
) -> pafs.S3FileSystem:
    import pyarrow as pyarrow_module
    from packaging import version
    
    client_kwargs = {
        "access_key": access_key_id,
        "secret_key": access_key_secret,
    }
    
    if session_token:
        client_kwargs["session_token"] = session_token
    
    if region:
        client_kwargs["region"] = region
    
    if version.parse(pyarrow_module.__version__) >= version.parse("7.0.0"):
        client_kwargs['force_virtual_addressing'] = True
        client_kwargs['endpoint_override'] = endpoint
    else:
        if bucket:
            client_kwargs['endpoint_override'] = f"{bucket}.{endpoint}"
        else:
            client_kwargs['endpoint_override'] = endpoint
    
    return pafs.S3FileSystem(**client_kwargs)


def main():
    ray.init(ignore_reinit_error=True, num_cpus=2)
    print("Ray initialized successfully")
    
    temp_dir = tempfile.mkdtemp()
    token = str(uuid.uuid4())
    server = RESTCatalogServer(
        data_path=temp_dir,
        auth_provider=BearTokenAuthProvider(token),
        config=ConfigResponse(defaults={"prefix": "mock-test"}),
        warehouse="warehouse"
    )
    server.start()
    print(f"REST server started at: {server.get_url()}")
    
    temp_json_dir = tempfile.mkdtemp()
    json_file_path = f"{temp_json_dir}/data.jsonl"
    create_sample_json_file(json_file_path, num_records=100)
    
    try:
        catalog = CatalogFactory.create({
            'metastore': 'rest',
            'uri': f"http://localhost:{server.port}",
            'warehouse': "warehouse",
            'token.provider': 'bear',
            'token': token,
        })
        catalog.create_database("default", True)
        
        from pypaimon.common.options.core_options import CoreOptions
        schema = Schema.from_pyarrow_schema(pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('category', pa.string()),
            ('value', pa.float64()),
            ('score', pa.int64()),
            ('timestamp', pa.timestamp('s')),
        ]), primary_keys=['id'], options={
            CoreOptions.BUCKET.key(): '4'
        })
        
        table_name = 'default.oss_json_import_table'
        catalog.create_table(table_name, schema, True)
        table = catalog.get_table(table_name)
        
        print(f"\nTable created: {table_name}")
        print(f"Table path: {table.table_path}")
        
        print("\n" + "="*60)
        print("Step 1: Reading JSON data from OSS using Ray Data")
        print("="*60)
        
        # If using actual OSS, create filesystem and read
        # Uncomment and configure with your OSS credentials:
        # oss_fs = create_oss_filesystem_for_ray(
        #     endpoint=OSS_ENDPOINT,
        #     access_key_id=OSS_ACCESS_KEY_ID,
        #     access_key_secret=OSS_ACCESS_KEY_SECRET,
        #     bucket=OSS_BUCKET,
        #     region="cn-hangzhou",  # Optional: OSS region
        # )
        # ray_dataset = ray.data.read_json(
        #     OSS_JSON_PATH,
        #     filesystem=oss_fs,
        #     concurrency=2,
        # )

        print(f"Reading JSON from: {json_file_path}")
        ray_dataset = ray.data.read_json(
            json_file_path,
            concurrency=2,
        )
        
        print(f"✓ Ray Dataset created successfully")
        print(f"  - Total rows: {ray_dataset.count()}")
        print(f"  - Schema: {ray_dataset.schema()}")
        
        sample_data = ray_dataset.take(3)
        print("\nSample data (first 3 rows):")
        for i, row in enumerate(sample_data, 1):
            print(f"  Row {i}: {row}")
        
        # Step 2: Write to Paimon table using Ray Sink
        print("\n" + "="*60)
        print("Step 2: Writing data to Paimon table using Ray Sink")
        print("="*60)
        
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        
        print("Writing Ray Dataset to Paimon table...")
        table_write.write_ray(
            ray_dataset,
            overwrite=False,
            concurrency=2,
            ray_remote_args={"num_cpus": 1}
        )
        
        # Commit the write
        table_commit = write_builder.new_commit()
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()
        
        print(f"✓ Successfully wrote {ray_dataset.count()} rows to table")
        
        # Step 3: Verify data by reading back
        print("\n" + "="*60)
        print("Step 3: Verifying data by reading back from table")
        print("="*60)
        
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()
        
        # Read to Pandas for verification
        result_df = table_read.to_pandas(splits)
        print(f"✓ Read back {len(result_df)} rows from table")
        print("\nFirst 5 rows from table:")
        print(result_df.head().to_string())
        
        ray_df = ray_dataset.to_pandas()
        ray_df_sorted = ray_df.sort_values(by='id').reset_index(drop=True)
        result_df_sorted = result_df.sort_values(by='id').reset_index(drop=True)
        
        pd.testing.assert_frame_equal(
            ray_df_sorted[['id', 'name', 'category', 'value', 'score']],
            result_df_sorted[['id', 'name', 'category', 'value', 'score']],
            check_dtype=False  # Allow type differences (e.g., int64 vs int32)
        )
        print("✓ Data verification passed: written data matches source data")
        
        # Step 4: Demonstrate additional Ray Data operations before writing
        print("\n" + "="*60)
        print("Step 4: Demonstrating data transformation with Ray Data")
        print("="*60)
        
        def add_computed_field(row):
            """Add a computed field based on existing data."""
            row['value_score_ratio'] = row['value'] / row['score'] if row['score'] > 0 else 0.0
            return row
        
        transformed_dataset = ray_dataset.map(add_computed_field)
        print(f"✓ Transformed dataset: {transformed_dataset.count()} rows")
        
        # Filter data
        filtered_dataset = ray_dataset.filter(lambda row: row['value'] > 50.0)
        print(f"✓ Filtered dataset (value > 50): {filtered_dataset.count()} rows")
        
        print("\n" + "="*60)
        print("Summary")
        print("="*60)
        print("✓ Successfully demonstrated Ray Sink with OSS JSON data source")

    finally:
        server.shutdown()
        if ray.is_initialized():
            ray.shutdown()
        print("\n✓ Server stopped and Ray shutdown")


if __name__ == '__main__':
    main()

