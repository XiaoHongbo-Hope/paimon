#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from typing import Dict, List


class Types:
    FILE = "file"
    SQL = "sql"
    LAMBDA = "lambda"


class FunctionFileResource:
    """Represents a file resource for a function."""

    def __init__(self, resource_type: str, uri: str):
        self.resource_type = resource_type
        self.uri = uri

    def to_dict(self) -> Dict:
        return {
            "resourceType": self.resource_type,
            "uri": self.uri,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "FunctionFileResource":
        return cls(
            resource_type=data.get("resourceType"),
            uri=data.get("uri"),
        )

    def __eq__(self, other):
        if not isinstance(other, FunctionFileResource):
            return False
        return self.resource_type == other.resource_type and self.uri == other.uri

    def __hash__(self):
        return hash((self.resource_type, self.uri))


class FunctionDefinition:
    """Represents a function definition."""

    def __init__(self, definition_type: str, **kwargs):
        self._type = definition_type
        self._data = kwargs

    @staticmethod
    def file(
        file_resources: List[FunctionFileResource],
        language: str,
        class_name: str,
        function_name: str,
    ) -> "FunctionDefinition":
        defn = FunctionDefinition(Types.FILE)
        defn._data = {
            "fileResources": file_resources,
            "language": language,
            "className": class_name,
            "functionName": function_name,
        }
        return defn

    @staticmethod
    def sql(body: str) -> "FunctionDefinition":
        defn = FunctionDefinition(Types.SQL)
        defn._data = {"definition": body}
        return defn

    @staticmethod
    def lambda_def(definition: str, language: str) -> "FunctionDefinition":
        defn = FunctionDefinition(Types.LAMBDA)
        defn._data = {"definition": definition, "language": language}
        return defn

    def to_dict(self) -> Dict:
        result = {"type": self._type}
        for key, value in self._data.items():
            if key == "fileResources" and isinstance(value, list):
                result[key] = [r.to_dict() if isinstance(r, FunctionFileResource) else r for r in value]
            else:
                result[key] = value
        return result

    @classmethod
    def from_dict(cls, data: Dict) -> "FunctionDefinition":
        if data is None:
            return None
        defn_type = data.get("type")
        defn = cls(defn_type)
        defn._data = {}
        for key, value in data.items():
            if key == "type":
                continue
            if key == "fileResources" and isinstance(value, list):
                defn._data[key] = [
                    FunctionFileResource.from_dict(r) if isinstance(r, dict) else r
                    for r in value
                ]
            else:
                defn._data[key] = value
        return defn

    def __eq__(self, other):
        if not isinstance(other, FunctionDefinition):
            return False
        return self._type == other._type and self._data == other._data

    def __hash__(self):
        return hash(self._type)
