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
"""Represents an index on a Model."""

from typing import List, Optional, Dict, Union

from spanner_orm import error


class Index(object):
    """Represents an index on a Model."""

    PRIMARY_INDEX = "PRIMARY_KEY"

    def __init__(
        self,
        columns: List[str],
        parent: Optional[str] = None,
        null_filtered: bool = False,
        unique: bool = False,
        storing_columns: Optional[List[str]] = None,
        name: Optional[str] = None,
        column_ordering: Union[Dict[str, bool], bool] = None,
    ):
        """
    Represents an index on the table

    :param columns: List of columns in the index
    :param parent:
    :param null_filtered: Should null values be filtered?
    :param unique: Enforce unique constraint?
    :param storing_columns: Additional columns to store with this index. This can help performance if fields are
      accessed together
    :param name: Name of the index (this may be used to customize default index name)
    :param column_ordering: Map of column names to bool (True for ASC, and False for DESC) indicating order for the
      field (defaults to ASC). For single column indexes, this can be a single bool value as well.
    """
        if not columns:
            raise error.ValidationError("An index must have at least one column")
        if isinstance(column_ordering, bool):
            if len(columns) != 1:
                raise error.ValidationError(
                    "column_ordering can be set to a bool only if single-column index"
                )
            column_ordering = {columns[0]: column_ordering}
        self._columns = columns
        self._name = name
        self._parent = parent
        self._null_filtered = null_filtered
        self._unique = unique
        self._storing_columns = storing_columns or []
        self._column_ordering = column_ordering or {}

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def name(self) -> Optional[str]:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        if not self._name:
            self._name = value

    @property
    def parent(self) -> Optional[str]:
        return self._parent

    @property
    def null_filtered(self) -> bool:
        return self._null_filtered

    @property
    def unique(self) -> bool:
        return self._unique

    @property
    def storing_columns(self) -> List[str]:
        return self._storing_columns

    @property
    def column_ordering(self) -> Dict[str, bool]:
        """
    Map of column name to order (True for ascending, False for descending)
    """
        return self._column_ordering

    @property
    def primary(self) -> bool:
        return self.name == self.PRIMARY_INDEX
