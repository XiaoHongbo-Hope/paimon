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

import os
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

from pypaimon.common.config import OssOptions
from pypaimon.common.file_io import FileIO


def to_lance_specified(file_io: FileIO, file_path: str) -> Tuple[str, Optional[Dict[str, str]]]:
    """Convert path and extract storage options for Lance format."""
    scheme, _, _ = file_io.parse_location(file_path)
    storage_options = None
    file_path_for_lance = file_io.to_filesystem_path(file_path)

    if scheme in {'file', None} or not scheme:
        if not os.path.isabs(file_path_for_lance):
            file_path_for_lance = os.path.abspath(file_path_for_lance)
    else:
        file_path_for_lance = file_path

    if scheme == 'oss':
        storage_options = {}
        if hasattr(file_io, 'properties'):
            # Extract bucket from file_path (same as Java: uri.getHost())
            # Use FileIO's _extract_oss_bucket method for reliable bucket extraction
            try:
                bucket = file_io._extract_oss_bucket(file_path)
            except (ValueError, AttributeError):
                # Fallback: extract from URI
                uri = urlparse(file_path)
                bucket = uri.netloc or uri.hostname
                if not bucket:
                    # Extract from path (oss://bucket/path format)
                    path_parts = uri.path.lstrip('/').split('/', 1)
                    if path_parts:
                        bucket = path_parts[0]
                if bucket:
                    # Remove endpoint suffix if present
                    bucket = bucket.split('.', 1)[0]

            endpoint = file_io.properties.get(OssOptions.OSS_ENDPOINT)
            if endpoint and bucket:
                # Construct endpoint as "https://{bucket}.{endpoint}" (same as Java implementation)
                # Java: "https://" + uri.getHost() + "." + originOptions.get("fs.oss.endpoint")
                if not endpoint.startswith('http://') and not endpoint.startswith('https://'):
                    storage_options['endpoint'] = f"https://{bucket}.{endpoint}"
                else:
                    # If endpoint already has protocol, extract the host part
                    endpoint_host = endpoint.split('://', 1)[1] if '://' in endpoint else endpoint
                    storage_options['endpoint'] = f"https://{bucket}.{endpoint_host}"

            if OssOptions.OSS_ACCESS_KEY_ID in file_io.properties:
                storage_options['access_key_id'] = file_io.properties[OssOptions.OSS_ACCESS_KEY_ID]
            if OssOptions.OSS_ACCESS_KEY_SECRET in file_io.properties:
                storage_options['secret_access_key'] = file_io.properties[OssOptions.OSS_ACCESS_KEY_SECRET]
            if OssOptions.OSS_SECURITY_TOKEN in file_io.properties:
                storage_options['session_token'] = file_io.properties[OssOptions.OSS_SECURITY_TOKEN]
            storage_options['virtual_hosted_style_request'] = 'true'

        file_path_for_lance = file_path.replace('oss://', 's3://')

    return file_path_for_lance, storage_options
