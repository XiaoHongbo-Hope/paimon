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
# limitations under the License.
#################################################################################

from pathlib import Path
from typing import List, Optional, Union
from urllib.parse import urlparse

try:
    from urlpath import URL
    URLPATH_AVAILABLE = True
except ImportError:
    URLPATH_AVAILABLE = False
    URL = None

from pypaimon.catalog.catalog import Catalog
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.catalog_exception import (DatabaseAlreadyExistException,
                                                DatabaseNotExistException,
                                                TableAlreadyExistException,
                                                TableNotExistException)
from pypaimon.catalog.database import Database
from pypaimon.common.config import CatalogOptions
from pypaimon.common.core_options import CoreOptions
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.table.table import Table


class FileSystemCatalog(Catalog):
    def __init__(self, catalog_options: dict):
        if CatalogOptions.WAREHOUSE not in catalog_options:
            raise ValueError(f"Paimon '{CatalogOptions.WAREHOUSE}' path must be set")
        self.warehouse = catalog_options.get(CatalogOptions.WAREHOUSE)
        self.catalog_options = catalog_options
        self.file_io = FileIO(self.warehouse, self.catalog_options)

    def get_database(self, name: str) -> Database:
        import logging
        logger = logging.getLogger(__name__)
        
        db_path = self.get_database_path(name)
        logger.debug(f"get_database: name={name}, db_path={db_path}, type={type(db_path)}")
        
        if self.file_io.exists(db_path):
            logger.debug(f"get_database: database exists: {name}")
            return Database(name, {})
        else:
            logger.debug(f"get_database: database not found: {name}, path={db_path}")
            raise DatabaseNotExistException(name)

    def create_database(self, name: str, ignore_if_exists: bool, properties: Optional[dict] = None):
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            self.get_database(name)
            if not ignore_if_exists:
                raise DatabaseAlreadyExistException(name)
        except DatabaseNotExistException:
            if properties and Catalog.DB_LOCATION_PROP in properties:
                raise ValueError("Cannot specify location for a database when using fileSystem catalog.")
            path = self.get_database_path(name)
            logger.debug(f"create_database: name={name}, path={path}, type={type(path)}")
            logger.debug(f"create_database: calling mkdirs with path={path}")
            result = self.file_io.mkdirs(path)
            logger.debug(f"create_database: mkdirs result={result}")
            # Verify creation
            exists_after = self.file_io.exists(path)
            logger.debug(f"create_database: exists after mkdirs={exists_after}, path={path}")

    def get_table(self, identifier: Union[str, Identifier]) -> Table:
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        if CoreOptions.SCAN_FALLBACK_BRANCH in self.catalog_options:
            raise ValueError(f"Unsupported CoreOption {CoreOptions.SCAN_FALLBACK_BRANCH}")
        table_path = self.get_table_path(identifier)
        table_schema = self.get_table_schema(identifier)

        # Create catalog environment for filesystem catalog
        # Filesystem catalog doesn't support version management by default
        catalog_environment = CatalogEnvironment(
            identifier=identifier,
            uuid=None,  # Filesystem catalog doesn't track table UUIDs
            catalog_loader=None,  # No catalog loader for filesystem
            supports_version_management=False
        )

        return FileStoreTable(self.file_io, identifier, table_path, table_schema, catalog_environment)

    def create_table(self, identifier: Union[str, Identifier], schema: 'Schema', ignore_if_exists: bool):
        if schema.options and schema.options.get(CoreOptions.AUTO_CREATE):
            raise ValueError(f"The value of {CoreOptions.AUTO_CREATE} property should be False.")

        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        self.get_database(identifier.get_database_name())
        try:
            self.get_table(identifier)
            if not ignore_if_exists:
                raise TableAlreadyExistException(identifier)
        except TableNotExistException:
            if schema.options and CoreOptions.TYPE in schema.options and schema.options.get(
                    CoreOptions.TYPE) != "table":
                raise ValueError(f"Table Type {schema.options.get(CoreOptions.TYPE)}")
            table_path = self.get_table_path(identifier)
            schema_manager = SchemaManager(self.file_io, table_path)
            schema_manager.create_table(schema)

    def get_table_schema(self, identifier: Identifier):
        table_path = self.get_table_path(identifier)
        table_schema = SchemaManager(self.file_io, table_path).latest()
        if table_schema is None:
            raise TableNotExistException(identifier)
        return table_schema

    def get_database_path(self, name) -> Union[Path, 'URL']:
        """Get database path, returning URL if urlpath is available, otherwise Path."""
        import logging
        logger = logging.getLogger(__name__)
        
        if URLPATH_AVAILABLE:
            # Use URL to preserve scheme
            warehouse_url = URL(self.warehouse)
            logger.debug(f"get_database_path: warehouse={self.warehouse}, warehouse_url={warehouse_url}, "
                        f"path={warehouse_url.path}, name={name}")
            
            # Handle trailing slash: if path is '/' or empty, remove it before joining
            # This prevents double slashes like oss://bucket//db.db
            if warehouse_url.path == '/' or warehouse_url.path == '':
                # Reconstruct URL without trailing slash
                from urllib.parse import urlparse
                parsed = urlparse(str(warehouse_url))
                # Remove trailing slash from path
                clean_path = parsed.path.rstrip('/')
                if clean_path:
                    warehouse_url = URL(f"{parsed.scheme}://{parsed.netloc}{clean_path}")
                else:
                    # No path, just scheme://netloc
                    warehouse_url = URL(f"{parsed.scheme}://{parsed.netloc}")
                logger.debug(f"get_database_path: cleaned warehouse_url={warehouse_url}, path={warehouse_url.path}")
            
            # Join database name (URL handles path joining correctly)
            db_path = warehouse_url / f"{name}{Catalog.DB_SUFFIX}"
            logger.debug(f"get_database_path: db_path={db_path}, str(db_path)={str(db_path)}")
            return db_path
        else:
            # Fallback to Path (will lose scheme)
            return self._trim_schema(self.warehouse) / f"{name}{Catalog.DB_SUFFIX}"

    def get_table_path(self, identifier: Identifier) -> Union[Path, 'URL']:
        """Get table path, returning URL if urlpath is available, otherwise Path."""
        db_path = self.get_database_path(identifier.get_database_name())
        return db_path / identifier.get_table_name()

    @staticmethod
    def _trim_schema(warehouse_url: str) -> Path:
        """Fallback method when urlpath is not available."""
        parsed = urlparse(warehouse_url)
        bucket = parsed.netloc
        warehouse_dir = parsed.path.lstrip('/')
        return Path(f"{bucket}/{warehouse_dir}" if warehouse_dir else bucket)

    def commit_snapshot(
            self,
            identifier: Identifier,
            table_uuid: Optional[str],
            snapshot: Snapshot,
            statistics: List[PartitionStatistics]
    ) -> bool:
        raise NotImplementedError("This catalog does not support commit catalog")
