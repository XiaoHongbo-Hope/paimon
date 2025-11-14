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


class ExternalPathStrategy:
    """Strategy for selecting external paths."""
    NONE = "none"
    ROUND_ROBIN = "round-robin"
    SPECIFIC_FS = "specific-fs"


class ExternalPathProvider:
    def __init__(self, external_table_paths: List[URL], relative_bucket_path: URL):
        """
        Args:
            external_table_paths: List of external table paths (base paths with scheme)
            relative_bucket_path: Relative path from table root to bucket directory
        """
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


def create_external_paths(
    external_paths_str: Optional[str],
    strategy: str,
    specific_fs: Optional[str]
) -> Optional[List[URL]]:
    """
    Create list of external paths based on configuration.

    Args:
        external_paths_str: Comma-separated external paths string
        strategy: Strategy for selecting paths (none, round-robin, specific-fs)
        specific_fs: Specific filesystem scheme when strategy is specific-fs

    Returns:
        List of URL objects for external paths, or None if not configured
    """
    if not external_paths_str or not external_paths_str.strip() or strategy == ExternalPathStrategy.NONE:
        return None

    paths = []
    for path_string in external_paths_str.split(","):
        path_string = path_string.strip()
        if not path_string:
            continue

        # Parse and validate path
        parsed = urlparse(path_string)
        scheme = parsed.scheme
        if not scheme:
            raise ValueError(f"External path must have a scheme (e.g., oss://, s3://, file://): {path_string}")

        # Filter by specific filesystem if strategy is specific-fs
        if strategy == ExternalPathStrategy.SPECIFIC_FS:
            if not specific_fs:
                raise ValueError(
                    f"data-file.external-paths.specific-fs must be set when "
                    f"strategy is {ExternalPathStrategy.SPECIFIC_FS}"
                )
            if scheme.lower() != specific_fs.lower():
                continue  # Skip paths that don't match the specific filesystem

        paths.append(URL(path_string))

    if not paths:
        raise ValueError("No valid external paths found after filtering")

    return paths


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

