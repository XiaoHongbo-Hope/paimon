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
from typing import List, Optional, Tuple
from urlpath import URL

from pypaimon.common.external_path_provider import ExternalPathProvider
from pypaimon.table.bucket_mode import BucketMode


class FileStorePathFactory:
    """
    Factory which produces paths for manifest files and data files.
    This class corresponds to org.apache.paimon.utils.FileStorePathFactory in Java.
    """

    MANIFEST_PATH = "manifest"
    MANIFEST_PREFIX = "manifest-"
    MANIFEST_LIST_PREFIX = "manifest-list-"
    INDEX_MANIFEST_PREFIX = "index-manifest-"

    INDEX_PATH = "index"
    INDEX_PREFIX = "index-"

    STATISTICS_PATH = "statistics"
    STATISTICS_PREFIX = "stat-"

    BUCKET_PATH_PREFIX = "bucket-"

    def __init__(
        self,
        root: URL,
        partition_keys: List[str],
        format_identifier: str,
        data_file_prefix: str,
        changelog_file_prefix: str,
        file_suffix_include_compression: bool,
        file_compression: str,
        data_file_path_directory: Optional[str] = None,
        external_paths: Optional[List[URL]] = None,
        index_file_in_data_file_dir: bool = False,
    ):
        """
        Initialize FileStorePathFactory.

        Args:
            root: Table schema root path
            partition_keys: List of partition key names
            format_identifier: File format identifier (e.g., "parquet", "orc")
            data_file_prefix: Prefix for data files
            changelog_file_prefix: Prefix for changelog files
            file_suffix_include_compression: Whether file suffix includes compression
            file_compression: File compression type
            data_file_path_directory: Optional directory for data files
            external_paths: Optional list of external paths
            index_file_in_data_file_dir: Whether index files are in data file directory
        """
        self.root = root
        self.partition_keys = partition_keys
        self.format_identifier = format_identifier
        self.data_file_prefix = data_file_prefix
        self.changelog_file_prefix = changelog_file_prefix
        self.file_suffix_include_compression = file_suffix_include_compression
        self.file_compression = file_compression
        self.data_file_path_directory = data_file_path_directory
        self.external_paths = external_paths or []
        self.index_file_in_data_file_dir = index_file_in_data_file_dir

    def root_path(self) -> URL:
        """Get the root path."""
        return self.root

    def manifest_path(self) -> URL:
        """Get the manifest path."""
        return self.root / self.MANIFEST_PATH

    def index_path(self) -> URL:
        """Get the index path."""
        return self.root / self.INDEX_PATH

    def statistics_path(self) -> URL:
        """Get the statistics path."""
        return self.root / self.STATISTICS_PATH

    def data_file_path(self) -> URL:
        """Get the data file path."""
        if self.data_file_path_directory:
            return self.root / self.data_file_path_directory
        return self.root

    def relative_bucket_path(self, partition: Tuple, bucket: int) -> URL:
        """
        Get relative bucket path for a partition and bucket.
        This corresponds to FileStorePathFactory.relativeBucketPath() in Java.

        Args:
            partition: Partition tuple
            bucket: Bucket number

        Returns:
            Relative bucket path as URL
        """
        bucket_name = str(bucket)
        if bucket == BucketMode.POSTPONE_BUCKET.value:
            bucket_name = "postpone"

        relative_parts = [f"{self.BUCKET_PATH_PREFIX}{bucket_name}"]

        # Add partition path
        if partition:
            partition_parts = []
            for i, field_name in enumerate(self.partition_keys):
                partition_parts.append(f"{field_name}={partition[i]}")
            if partition_parts:
                relative_parts = partition_parts + relative_parts

        # Add data file path directory if specified
        if self.data_file_path_directory:
            relative_parts = [self.data_file_path_directory] + relative_parts

        return URL("/".join(relative_parts))

    def bucket_path(self, partition: Tuple, bucket: int) -> URL:
        """
        Get full bucket path for a partition and bucket.
        This corresponds to FileStorePathFactory.bucketPath() in Java.

        Args:
            partition: Partition tuple
            bucket: Bucket number

        Returns:
            Full bucket path as URL
        """
        return self.root / self.relative_bucket_path(partition, bucket)

    def create_external_path_provider(
        self, partition: Tuple, bucket: int
    ) -> Optional[ExternalPathProvider]:
        """
        Create ExternalPathProvider for a partition and bucket.
        This method corresponds to FileStorePathFactory.createExternalPathProvider() in Java.

        Args:
            partition: Partition tuple
            bucket: Bucket number

        Returns:
            ExternalPathProvider instance or None if external paths not configured
        """
        if not self.external_paths:
            return None

        relative_bucket_path = self.relative_bucket_path(partition, bucket)
        return ExternalPathProvider(self.external_paths, relative_bucket_path)

