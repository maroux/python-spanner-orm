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
"""Helper to deal with field types in Spanner interactions."""

import abc
import datetime
from typing import Any, Type, Optional

from spanner_orm import error

from google.cloud.spanner_v1 import COMMIT_TIMESTAMP
from google.cloud.spanner_v1.proto import type_pb2


class FieldType(abc.ABC):
    """Base class for column types for Spanner interactions."""

    @staticmethod
    @abc.abstractmethod
    def ddl() -> str:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def grpc_type() -> type_pb2.Type:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def validate_type(value: Any) -> None:
        raise NotImplementedError


class Field(object):
    """Represents a column in a table as a field in a model."""

    def __init__(
        self,
        field_type: Type[FieldType],
        nullable: bool = False,
        primary_key: bool = False,
        allow_commit_timestamp: bool = False,
        name: Optional[str] = None,
        size: Optional[int] = None,
    ):
        self._name = name
        self._size = size
        self._type = field_type
        self._nullable = nullable
        self._primary_key = primary_key
        self._allow_commit_timestamp = allow_commit_timestamp

        if self._type.ddl() != "TIMESTAMP" and self._allow_commit_timestamp:
            raise error.ValidationError(
                "allow_commit_timestamp can not be set on field {}".format(self._type)
            )

    def ddl(self) -> str:
        if self._nullable:
            nullable = ""
        else:
            nullable = " NOT NULL"
        if self._allow_commit_timestamp:
            options = " OPTIONS (allow_commit_timestamp=true)"
        else:
            options = ""
        if self._size is None:
            field_type = self._type.ddl()
        else:
            field_type = self._type.ddl(self._size)
        return "{field_type}{nullable}{options}".format(
            field_type=field_type, nullable=nullable, options=options
        )

    @property
    def field_type(self) -> Type[FieldType]:
        return self._type

    def grpc_type(self) -> str:
        return self._type.grpc_type()

    @property
    def nullable(self) -> bool:
        return self._nullable

    @property
    def primary_key(self) -> bool:
        return self._primary_key

    @property
    def name(self) -> Optional[str]:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        if not self._name:
            self._name = value

    def validate(self, value) -> None:
        if value is None:
            if not self._nullable:
                raise error.ValidationError("None set for non-nullable field")
        elif self._allow_commit_timestamp and value == COMMIT_TIMESTAMP:
            return
        else:
            self._type.validate_type(value)


class Boolean(FieldType):
    """Represents a boolean type."""

    @staticmethod
    def ddl() -> str:
        return "BOOL"

    @staticmethod
    def grpc_type() -> type_pb2.Type:
        return type_pb2.Type(code=type_pb2.BOOL)

    @staticmethod
    def validate_type(value: Any) -> None:
        if not isinstance(value, bool):
            raise error.ValidationError("{} is not of type bool".format(value))


class Integer(FieldType):
    """Represents an integer type."""

    @staticmethod
    def ddl() -> str:
        return "INT64"

    @staticmethod
    def grpc_type() -> type_pb2.Type:
        return type_pb2.Type(code=type_pb2.INT64)

    @staticmethod
    def validate_type(value: Any) -> None:
        if not isinstance(value, int):
            raise error.ValidationError("{} is not of type int".format(value))


class Float(FieldType):
    """Represents a float type."""

    @staticmethod
    def ddl() -> str:
        return "FLOAT64"

    @staticmethod
    def grpc_type() -> type_pb2.Type:
        return type_pb2.Type(code=type_pb2.FLOAT64)

    @staticmethod
    def validate_type(value: Any) -> None:
        if not isinstance(value, (int, float)):
            raise error.ValidationError("{} is not of type float".format(value))


class String(FieldType):
    """Represents a string type."""

    @staticmethod
    def ddl(size="MAX") -> str:
        return "STRING({size})".format(size=size)

    @staticmethod
    def grpc_type() -> type_pb2.Type:
        return type_pb2.Type(code=type_pb2.STRING)

    @staticmethod
    def validate_type(value) -> None:
        if not isinstance(value, str):
            raise error.ValidationError("{} is not of type str".format(value))


class Date(FieldType):
    """Represents a date type."""

    @staticmethod
    def ddl() -> str:
        return "DATE"

    @staticmethod
    def grpc_type() -> type_pb2.Type:
        return type_pb2.Type(code=type_pb2.DATE)

    @staticmethod
    def validate_type(value) -> None:
        try:
            datetime.datetime.strptime(value, "%Y-%m-%d")
        except:
            raise error.ValidationError(
                "{} is not of type date (YYYY-[M]M-[D]D)".format(value)
            )


class StringArray(FieldType):
    """Represents an array of strings type."""

    @staticmethod
    def ddl(size="MAX") -> str:
        return "ARRAY<STRING({size})>".format(size=size)

    @staticmethod
    def grpc_type() -> type_pb2.Type:
        return type_pb2.Type(code=type_pb2.ARRAY)

    @staticmethod
    def validate_type(value: Any) -> None:
        if not isinstance(value, list):
            raise error.ValidationError("{} is not of type list".format(value))
        for item in value:
            if not isinstance(item, str):
                raise error.ValidationError("{} is not of type str".format(item))


class BoolArray(FieldType):
    """Represents an array of booleans type."""

    @staticmethod
    def ddl() -> str:
        return "ARRAY<BOOL>"

    @staticmethod
    def grpc_type() -> type_pb2.Type:
        return type_pb2.Type(code=type_pb2.ARRAY)

    @staticmethod
    def validate_type(value: Any) -> None:
        if not isinstance(value, list):
            raise error.ValidationError("{} is not of type list".format(value))
        for item in value:
            if not isinstance(item, bool):
                raise error.ValidationError("{} is not of type bool".format(item))


class IntegerArray(FieldType):
    """Represents an array of integers type."""

    @staticmethod
    def ddl() -> str:
        return "ARRAY<INT64>"

    @staticmethod
    def grpc_type() -> type_pb2.Type:
        return type_pb2.Type(code=type_pb2.ARRAY)

    @staticmethod
    def validate_type(value: Any) -> None:
        if not isinstance(value, list):
            raise error.ValidationError("{} is not of type list".format(value))
        for item in value:
            if not isinstance(item, int):
                raise error.ValidationError("{} is not of type int".format(item))


class FloatArray(FieldType):
    """Represents an array of floats type."""

    @staticmethod
    def ddl() -> str:
        return "ARRAY<FLOAT64>"

    @staticmethod
    def grpc_type() -> type_pb2.Type:
        return type_pb2.Type(code=type_pb2.ARRAY)

    @staticmethod
    def validate_type(value: Any) -> None:
        if not isinstance(value, list):
            raise error.ValidationError("{} is not of type list".format(value))
        for item in value:
            if not isinstance(item, float):
                raise error.ValidationError("{} is not of type float".format(item))


class DateArray(FieldType):
    """Represents an array of dates type."""

    @staticmethod
    def ddl() -> str:
        return "ARRAY<DATE>"

    @staticmethod
    def grpc_type() -> type_pb2.Type:
        return type_pb2.Type(code=type_pb2.ARRAY)

    @staticmethod
    def validate_type(value: Any) -> None:
        if not isinstance(value, list):
            raise error.ValidationError("{} is not of type list".format(value))
        for item in value:
            try:
                datetime.datetime.strptime(item, "%Y-%m-%d")
            except:
                raise error.ValidationError(
                    "{} is not of type date (YYYY-[M]M-[D]D)".format(item)
                )


class Timestamp(FieldType):
    """Represents a timestamp type."""

    @staticmethod
    def ddl() -> str:
        return "TIMESTAMP"

    @staticmethod
    def grpc_type() -> type_pb2.Type:
        return type_pb2.Type(code=type_pb2.TIMESTAMP)

    @staticmethod
    def validate_type(value: Any) -> None:
        if not isinstance(value, datetime.datetime):
            raise error.ValidationError("{} is not of type datetime".format(value))


ALL_TYPES = [
    Boolean,
    Integer,
    Float,
    String,
    Date,
    Timestamp,
    StringArray,
    BoolArray,
    IntegerArray,
    FloatArray,
    DateArray,
]
