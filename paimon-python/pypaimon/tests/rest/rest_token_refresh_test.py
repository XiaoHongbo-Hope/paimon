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
import time
import unittest
import uuid

from pypaimon import Schema
from pypaimon.catalog.rest.rest_token import RESTToken
from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.common.identifier import Identifier
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


class RESTTokenRefreshTest(RESTBaseTest):

    def setUp(self):
        super().setUp()
        self.options['data-token.enabled'] = 'true'
        from pypaimon import CatalogFactory
        self.rest_catalog = CatalogFactory.create(self.options)

    def test_refresh_file_io(self):
        identifiers = [
            Identifier.from_string("test_db_a.test_table_a"),
            Identifier.from_string("test_db_b.test_table_b"),
            Identifier.from_string("test_db_c.test_table_c")
        ]

        for identifier in identifiers:
            schema = Schema(
                fields=[DataField(0, "col1", AtomicType("STRING"), "field1")],
                partition_keys=[],
                primary_keys=[],
                options={},
                comment=""
            )
            self.rest_catalog.create_database(identifier.get_database_name(), True)
            self.rest_catalog.create_table(identifier.get_full_name(), schema, False)

            table = self.rest_catalog.get_table(identifier.get_full_name())
            
            self.assertIsInstance(table.file_io, RESTTokenFileIO)
            file_io = table.file_io
            
            file_data_token = file_io.valid_token()
            self.assertIsNotNone(file_data_token, "Token should be retrieved from file_io")
            
            server_data_token = self.server.get_table_token(identifier)
            if server_data_token:
                self.assertEqual(server_data_token, file_data_token)

    def test_valid_token(self):
        from pypaimon.common.options.config import CatalogOptions
        self.options[CatalogOptions.DLF_OSS_ENDPOINT.key()] = 'test-endpoint'
        from pypaimon import CatalogFactory
        self.rest_catalog = CatalogFactory.create(self.options)
        self.rest_catalog.create_database("test_data_token", False)

        identifier = Identifier.from_string("test_data_token.table_for_testing_valid_token")
        schema = Schema(
            fields=[DataField(0, "col1", AtomicType("STRING"), "field1")],
            partition_keys=[],
            primary_keys=[],
            options={},
            comment=""
        )

        expired_data_token = RESTToken(
            token={"akId": "akId", "akSecret": str(uuid.uuid4())},
            expire_at_millis=int(time.time() * 1000) + 3600_000  # 1 hour from now
        )
        self.server.set_table_token(identifier, expired_data_token)
        self.rest_catalog.create_database(identifier.get_database_name(), True)
        self.rest_catalog.create_table(identifier.get_full_name(), schema, False)

        table = self.rest_catalog.get_table(identifier.get_full_name())
        file_io = table.file_io
        self.assertIsInstance(file_io, RESTTokenFileIO)

        file_data_token = file_io.valid_token()
        self.assertEqual("test-endpoint", file_data_token.token.get("fs.oss.endpoint"))

    def test_refresh_file_io_when_expired(self):
        identifier = Identifier.from_string("test_data_token.table_for_testing_date_token")
        schema = Schema(
            fields=[DataField(0, "col1", AtomicType("STRING"), "field1")],
            partition_keys=[],
            primary_keys=[],
            options={},
            comment=""
        )

        expired_data_token = RESTToken(
            token={"akId": "akId", "akSecret": str(uuid.uuid4())},
            expire_at_millis=int(time.time() * 1000) + 3600_000  # 1 hour from now
        )
        self.server.set_table_token(identifier, expired_data_token)
        self.rest_catalog.create_database(identifier.get_database_name(), True)
        self.rest_catalog.create_table(identifier.get_full_name(), schema, False)

        table = self.rest_catalog.get_table(identifier.get_full_name())
        file_io = table.file_io
        self.assertIsInstance(file_io, RESTTokenFileIO)

        file_data_token = file_io.valid_token()
        self.assertEqual(expired_data_token, file_data_token)

        new_data_token = RESTToken(
            token={"akId": "akId", "akSecret": str(uuid.uuid4())},
            expire_at_millis=int(time.time() * 1000) + 4000_000  # ~1.1 hours from now
        )
        self.server.set_table_token(identifier, new_data_token)

        next_file_data_token = file_io.valid_token()
        self.assertEqual(new_data_token, next_file_data_token)
        self.assertGreater(
            next_file_data_token.expire_at_millis,
            file_data_token.expire_at_millis
        )


if __name__ == '__main__':
    unittest.main()

