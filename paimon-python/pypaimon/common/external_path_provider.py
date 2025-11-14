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
import random
from typing import List, Optional
from urllib.parse import urlparse

from urlpath import URL

class ExternalPathProvider:
    def __init__(self, external_table_paths: List[URL], relative_bucket_path: URL):
        self.external_table_paths = external_table_paths
        self.relative_bucket_path = relative_bucket_path
        # Start from a random position for load balancing
        self.position = random.randint(0, len(external_table_paths) - 1) if external_table_paths else 0

    def get_next_external_data_path(self, file_name: str) -> URL:
        """
        Get the next external data path using round-robin strategy.
        """
        if not self.external_table_paths:
            raise ValueError("No external paths available")

        # Round-robin: move to next path
        self.position = (self.position + 1) % len(self.external_table_paths)

        external_base = self.external_table_paths[self.position]

        if str(self.relative_bucket_path):
            full_path = external_base / self.relative_bucket_path / file_name
        else:
            full_path = external_base / file_name

        return full_path


def create_external_path_provider(
    external_paths: List[URL],
    partition: tuple,
    bucket: int,
    partition_keys: List[str]
) -> Optional[ExternalPathProvider]:
    """
    Create ExternalPathProvider for a specific partition and bucket.

    Args:
        external_paths: List of external paths (URL objects) from table._create_external_paths()
        partition: Partition tuple
        bucket: Bucket number
        partition_keys: List of partition key names

    Returns:
        ExternalPathProvider instance or None if external paths not configured
    """
    if not external_paths:
        return None

    # Build relative bucket path (same logic as _generate_file_path)
    relative_parts = []
    for i, field_name in enumerate(partition_keys):
        relative_parts.append(f"{field_name}={partition[i]}")
    relative_parts.append(f"bucket-{bucket}")

    # Use URL for relative path (URL supports relative paths without scheme)
    relative_bucket_path = URL("/".join(relative_parts))

    return ExternalPathProvider(external_paths, relative_bucket_path)

