"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import logging
import threading
import time
from pathlib import Path
from typing import Optional

import pyarrow
import pyarrow.fs
from cachetools import LRUCache
from pyarrow._fs import FileSystem

from pypaimon.api.rest_api import RESTApi
from pypaimon.catalog.rest.rest_token import RESTToken
from pypaimon.common.config import CatalogOptions, OssOptions
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier


class RESTTokenFileIO(FileIO):
    _FILESYSTEM_CACHE: LRUCache = LRUCache(maxsize=1000)
    _CACHE_LOCK = threading.Lock()
    _TOKEN_CACHE: dict = {}
    _TOKEN_LOCKS: dict = {}
    _TOKEN_LOCKS_LOCK = threading.Lock()

    def __init__(self, identifier: Identifier, path: str,
                 catalog_options: Optional[dict] = None):
        self.identifier = identifier
        self.path = path
        self.token: Optional[RESTToken] = None
        self.api_instance: Optional[RESTApi] = None
        self.lock = threading.Lock()
        self.log = logging.getLogger(__name__)
        super().__init__(path, catalog_options)

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove non-serializable objects
        state.pop('lock', None)
        state.pop('api_instance', None)
        # token can be serialized, but we'll refresh it on deserialization
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Recreate lock after deserialization
        self.lock = threading.Lock()
        # api_instance will be recreated when needed
        self.api_instance = None

    def _initialize_oss_fs(self, path) -> FileSystem:
        self.try_to_refresh_token()
        if self.token is None:
            raise RuntimeError("Token is None after refresh, cannot initialize OSS filesystem")
        self.properties.update(self.token.token)
        return super()._initialize_oss_fs(path)

    def _initialize_s3_fs(self) -> FileSystem:
        self.try_to_refresh_token()
        if self.token is None:
            raise RuntimeError("Token is None after refresh, cannot initialize S3 filesystem")
        self.properties.update(self.token.token)
        return super()._initialize_s3_fs()

    def _merge_token_with_catalog_options(self, token: dict) -> dict:
        """Merge token with catalog options, DLF OSS endpoint should override the standard OSS endpoint."""
        merged_token = dict(token)
        dlf_oss_endpoint = self.properties.get(CatalogOptions.DLF_OSS_ENDPOINT)
        if dlf_oss_endpoint and dlf_oss_endpoint.strip():
            merged_token[OssOptions.OSS_ENDPOINT] = dlf_oss_endpoint
        return merged_token

    def _get_filesystem(self) -> FileSystem:
        self.try_to_refresh_token()

        if self.token is None:
            return self.filesystem

        filesystem = self._FILESYSTEM_CACHE.get(self.token)
        if filesystem is not None:
            return filesystem

        with self._CACHE_LOCK:
            filesystem = self._FILESYSTEM_CACHE.get(self.token)
            if filesystem is not None:
                return filesystem

            self.properties.update(self.token.token)
            scheme, netloc, _ = self.parse_location(self.path)
            if scheme in {"oss"}:
                filesystem = self._initialize_oss_fs(self.path)
            elif scheme in {"s3", "s3a", "s3n"}:
                filesystem = self._initialize_s3_fs()
            else:
                filesystem = self.filesystem

            self._FILESYSTEM_CACHE[self.token] = filesystem
            return filesystem

    def new_input_stream(self, path: str):
        filesystem = self._get_filesystem()
        path_str = self.to_filesystem_path(path)
        return filesystem.open_input_file(path_str)

    def new_output_stream(self, path: str):
        filesystem = self._get_filesystem()
        path_str = self.to_filesystem_path(path)
        parent_dir = Path(path_str).parent
        if str(parent_dir) and not self.exists(str(parent_dir)):
            self.mkdirs(str(parent_dir))
        return filesystem.open_output_stream(path_str)

    def get_file_status(self, path: str):
        filesystem = self._get_filesystem()
        path_str = self.to_filesystem_path(path)
        file_infos = filesystem.get_file_info([path_str])
        return file_infos[0]

    def list_status(self, path: str):
        filesystem = self._get_filesystem()
        path_str = self.to_filesystem_path(path)
        selector = pyarrow.fs.FileSelector(path_str, recursive=False, allow_not_found=True)
        return filesystem.get_file_info(selector)

    def exists(self, path: str) -> bool:
        try:
            filesystem = self._get_filesystem()
            path_str = self.to_filesystem_path(path)
            file_info = filesystem.get_file_info([path_str])[0]
            result = file_info.type != pyarrow.fs.FileType.NotFound
            return result
        except Exception:
            return False

    def delete(self, path: str, recursive: bool = False) -> bool:
        try:
            filesystem = self._get_filesystem()
            path_str = self.to_filesystem_path(path)
            file_info = filesystem.get_file_info([path_str])[0]
            if file_info.type == pyarrow.fs.FileType.Directory:
                if recursive:
                    filesystem.delete_dir_contents(path_str)
                else:
                    filesystem.delete_dir(path_str)
            else:
                filesystem.delete_file(path_str)
            return True
        except Exception as e:
            self.logger.warning(f"Failed to delete {path}: {e}")
            return False

    def mkdirs(self, path: str) -> bool:
        try:
            filesystem = self._get_filesystem()
            path_str = self.to_filesystem_path(path)
            filesystem.create_dir(path_str, recursive=True)
            return True
        except Exception as e:
            self.logger.warning(f"Failed to create directory {path}: {e}")
            return False

    def rename(self, src: str, dst: str) -> bool:
        try:
            filesystem = self._get_filesystem()
            dst_str = self.to_filesystem_path(dst)
            dst_parent = Path(dst_str).parent
            if str(dst_parent) and not self.exists(str(dst_parent)):
                self.mkdirs(str(dst_parent))

            src_str = self.to_filesystem_path(src)
            filesystem.move(src_str, dst_str)
            return True
        except Exception as e:
            self.logger.warning(f"Failed to rename {src} to {dst}: {e}")
            return False

    def copy_file(self, source_path: str, target_path: str, overwrite: bool = False):
        if not overwrite and self.exists(target_path):
            raise FileExistsError(f"Target file {target_path} already exists and overwrite=False")

        filesystem = self._get_filesystem()
        source_str = self.to_filesystem_path(source_path)
        target_str = self.to_filesystem_path(target_path)
        filesystem.copy_file(source_str, target_str)

    def try_to_refresh_token(self):
        identifier_str = str(self.identifier)
        
        if self.token is not None and not self._is_token_expired(self.token):
            self.log.debug(f"Using instance token (fast path), identifier: {identifier_str}")
            return
        
        cached_token = self._get_cached_token(identifier_str)
        if cached_token and not self._is_token_expired(cached_token):
            self.token = cached_token
            self.log.debug(f"Using cached token (fast path), identifier: {identifier_str}")
            return
        
        self.log.debug(
            f"Cache miss, acquiring lock for token refresh, identifier: {identifier_str}, "
            f"cache size: {len(self._TOKEN_CACHE)}, instance token: {self.token is not None}"
        )
        global_lock = self._get_global_token_lock()
        
        with global_lock:
            cached_token = self._get_cached_token(identifier_str)
            if cached_token and not self._is_token_expired(cached_token):
                self.token = cached_token
                self.log.debug(f"Using cached token after acquiring lock, identifier: {identifier_str}")
                return
            
            token_to_check = cached_token if cached_token else self.token
            if token_to_check is None or self._is_token_expired(token_to_check):
                import traceback
                import os
                import threading
                self.log.warning(
                    f"REFRESHING TOKEN for identifier [{identifier_str}]. "
                    f"This should only happen once per worker per token expiration period. "
                    f"Cache size: {len(self._TOKEN_CACHE)}, "
                    f"Process ID: {os.getpid()}, "
                    f"Thread ID: {threading.get_ident()}, "
                    f"Call stack: {''.join(traceback.format_stack()[-5:-1])}"
                )
                self.refresh_token()
                self._set_cached_token(identifier_str, self.token)
                self.log.debug(f"Token refreshed and cached, identifier: {identifier_str}, cache size: {len(self._TOKEN_CACHE)}")
            else:
                self.token = cached_token if cached_token else self.token
                self.log.debug(f"Token refresh not needed after acquiring lock, identifier: {identifier_str}")
    
    def _get_cached_token(self, identifier_str: str):
        with self._TOKEN_LOCKS_LOCK:
            return self._TOKEN_CACHE.get(identifier_str)
    
    def _set_cached_token(self, identifier_str: str, token):
        with self._TOKEN_LOCKS_LOCK:
            self._TOKEN_CACHE[identifier_str] = token
    
    def _is_token_expired(self, token) -> bool:
        if token is None:
            return True
        current_time = int(time.time() * 1000)
        return (token.expire_at_millis - current_time) < RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS
    
    def _get_global_token_lock(self):
        identifier_str = str(self.identifier)
        with self._TOKEN_LOCKS_LOCK:
            if identifier_str not in self._TOKEN_LOCKS:
                self._TOKEN_LOCKS[identifier_str] = threading.Lock()
            return self._TOKEN_LOCKS[identifier_str]

    def _should_refresh_considering_cache(self):
        identifier_str = str(self.identifier)
        cached_token = self._get_cached_token(identifier_str)
        if cached_token and not self._is_token_expired(cached_token):
            return False
        
        token_to_check = cached_token if cached_token else self.token
        if token_to_check is None:
            return True
        current_time = int(time.time() * 1000)
        return (token_to_check.expire_at_millis - current_time) < RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS
    
    def should_refresh(self):
        if self.token is None:
            return True
        current_time = int(time.time() * 1000)
        return (self.token.expire_at_millis - current_time) < RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS

    def refresh_token(self):
        import traceback
        self.log.warning(
            f"REFRESHING TOKEN for identifier [{self.identifier}] - "
            f"This should only happen once per worker per token expiration period. "
            f"Call stack: {''.join(traceback.format_stack()[-5:-1])}"
        )
        self.log.info(f"begin refresh data token for identifier [{self.identifier}]")
        if self.api_instance is None:
            if not self.properties:
                raise RuntimeError(
                    f"Cannot refresh token: properties is None or empty. "
                    f"This may indicate a serialization issue when passing RESTTokenFileIO to Ray workers."
                )
            
            if CatalogOptions.URI not in self.properties:
                available_keys = list(self.properties.keys()) if self.properties else []
                raise RuntimeError(
                    f"Cannot refresh token: missing required configuration '{CatalogOptions.URI}' in properties. "
                    f"This is required to create RESTApi for token refresh. "
                    f"Available configuration keys: {available_keys}. "
                    f"This may indicate that catalog options were not properly serialized when passing to Ray workers."
                )
            
            uri = self.properties.get(CatalogOptions.URI)
            if not uri or not uri.strip():
                raise RuntimeError(
                    f"Cannot refresh token: '{CatalogOptions.URI}' is empty or whitespace. "
                    f"Value: '{uri}'. Please ensure the REST catalog URI is properly configured."
                )
            
            try:
                self.api_instance = RESTApi(self.properties, False)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to create RESTApi for token refresh: {e}. "
                    f"Please check that all required catalog options are properly configured. "
                    f"Identifier: {self.identifier}"
                ) from e

        try:
            response = self.api_instance.load_table_token(self.identifier)
        except Exception as e:
            raise RuntimeError(
                f"Failed to load table token from REST API: {e}. "
                f"Identifier: {self.identifier}, URI: {self.properties.get(CatalogOptions.URI)}"
            ) from e
        
        self.log.info(
            f"end refresh data token for identifier [{self.identifier}] expiresAtMillis [{response.expires_at_millis}]"
        )
        merged_token = self._merge_token_with_catalog_options(response.token)
        self.token = RESTToken(merged_token, response.expires_at_millis)

    def valid_token(self):
        self.try_to_refresh_token()
        return self.token
