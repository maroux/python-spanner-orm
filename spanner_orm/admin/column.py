# python3
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Model for interacting with Spanner column schema table."""

import re
from typing import Type, Union

from spanner_orm import error
from spanner_orm import field
from spanner_orm.admin import schema

_string_pattern = re.compile(r"^STRING\([0-9]+\)+$")
_string_array_pattern = re.compile(r"^ARRAY<STRING\([0-9]+\)>+$")


class ColumnSchema(schema.InformationSchema):
    """Model for interacting with Spanner column schema table."""

    __table__ = "information_schema.columns"
    table_catalog = field.Field(field.String, primary_key=True)
    table_schema = field.Field(field.String, primary_key=True)
    table_name = field.Field(field.String, primary_key=True)
    column_name = field.Field(field.String, primary_key=True)
    ordinal_position = field.Field(field.Integer)
    is_nullable = field.Field(field.String)
    spanner_type = field.Field(field.String)

    @property
    def nullable(self) -> bool:
        return self.is_nullable == "YES"

    @property
    def field_type(self) -> Type[field.FieldType]:
        for field_type in field.ALL_TYPES:
            if self.spanner_type == field_type.ddl():
                return field_type

        if bool(_string_pattern.match(self.spanner_type)):
            return field.String
        elif bool(_string_array_pattern.match(self.spanner_type)):
            return field.StringArray

        raise error.SpannerError(
            "No corresponding Type for {}".format(self.spanner_type)
        )

    @property
    def size(self) -> Union[None, int]:
        if bool(_string_pattern.match(self.spanner_type)) or bool(
            _string_array_pattern.match(self.spanner_type)
        ):
            # Extract digits from string (i.e. STRING(50) -> 50)
            return int("".join(filter(str.isdigit, self.spanner_type)))
        else:
            return None
