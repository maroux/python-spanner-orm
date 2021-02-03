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
import logging
import unittest
from unittest import mock

from spanner_orm import error, relationship
from spanner_orm import field
from spanner_orm.admin import update
from spanner_orm.index import Index
from spanner_orm.tests import models


class UpdateTest(unittest.TestCase):
    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_add_column(self, get_model):
        table_name = models.SmallTestModel.table
        get_model.return_value = models.SmallTestModel

        new_field = field.Field(field.String, nullable=True)
        test_update = update.AddColumn(table_name, "bar", new_field)
        test_update.validate()
        self.assertEqual(
            test_update.ddl(),
            "ALTER TABLE {} ADD COLUMN bar STRING(MAX)".format(table_name),
        )

    @mock.patch("spanner_orm.admin.index_column.IndexColumnSchema.count")
    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_drop_column(self, get_model, index_count):
        table_name = models.SmallTestModel.table
        get_model.return_value = models.SmallTestModel
        index_count.return_value = 0

        test_update = update.DropColumn(table_name, "value_2")
        test_update.validate()
        self.assertEqual(
            test_update.ddl(), "ALTER TABLE {} DROP COLUMN value_2".format(table_name)
        )

    @mock.patch("spanner_orm.admin.index_column.IndexColumnSchema.count")
    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_drop_column_error_on_primary_key(self, get_model, index_count):
        get_model.return_value = models.SmallTestModel
        index_count.return_value = 1

        test_update = update.DropColumn(models.SmallTestModel.table, "key")
        with self.assertRaisesRegex(error.SpannerError, "Column key is indexed"):
            test_update.validate()

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_add_foreign_key(self, get_model):
        table_name1 = models.SmallTestModel.table
        table_name2 = models.UnittestModel.table
        get_model.side_effect = [models.SmallTestModel, models.UnittestModel]

        fk_constraint = "FK_Testing"
        key1 = "value_1"
        key2 = "string"

        test_update = update.AddForeignKeyConstraint(
            table_name1, fk_constraint, table_name2, {key1: key2}
        )
        test_update.validate()
        self.assertEqual(
            test_update.ddl(),
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})".format(
                table_name1, fk_constraint, key1, table_name2, key2
            ),
        )

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_drop_foreign_key(self, get_model):
        table_name = models.SmallTestModel.table
        get_model.return_value = models.SmallTestModel

        fk_constraint = "FK_Testing"

        test_update = update.DropForeignKeyConstraint(table_name, fk_constraint)
        test_update.validate()
        self.assertEqual(
            test_update.ddl(),
            "ALTER TABLE {} DROP CONSTRAINT {}".format(table_name, fk_constraint),
        )

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_create_table(self, get_model):
        get_model.return_value = None
        new_model = models.UnittestModel
        test_update = update.CreateTable(new_model)
        test_update.validate()

        test_model_ddl = (
            "CREATE TABLE table (int_ INT64 NOT NULL, int_2 INT64,"
            " float_ FLOAT64 NOT NULL, float_2 FLOAT64,"
            " string STRING(MAX) NOT NULL, string_2 STRING(MAX),"
            " string_3 STRING(10),"
            " timestamp TIMESTAMP NOT NULL,"
            " timestamp_2 TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
            " date DATE,"
            " bytes_ BYTES(MAX),"
            " bytes_2 BYTES(2048),"
            " bool_array ARRAY<BOOL>,"
            " int_array ARRAY<INT64>,"
            " float_array ARRAY<FLOAT64>,"
            " date_array ARRAY<DATE>,"
            " string_array ARRAY<STRING(MAX)>,"
            " string_array_2 ARRAY<STRING(50)>) PRIMARY KEY (int_, float_, string)"
        )

        self.assertEqual(test_update.ddl(), test_model_ddl)

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_create_table_no_model(self, get_model):
        get_model.return_value = None
        test_update = update.CreateTable(
            table_name="table",
            primary_keys=["int_", "float_", "string"],
            fields={
                "int_": field.Field(field.Integer, primary_key=True, name="int_"),
                "int_2": field.Field(field.Integer, nullable=True, name="int_2"),
                "float_": field.Field(field.Float, primary_key=True, name="float_"),
                "float_2": field.Field(field.Float, nullable=True, name="float_2"),
                "string": field.Field(field.String, primary_key=True, name="string"),
                "string_2": field.Field(field.String, nullable=True, name="string_2"),
                "string_3": field.Field(
                    field.String, nullable=True, size=10, name="string_3"
                ),
                "timestamp": field.Field(field.Timestamp, name="timestamp"),
                "timestamp_2": field.Field(
                    field.Timestamp,
                    nullable=True,
                    allow_commit_timestamp=True,
                    name="timestamp_2",
                ),
                "date": field.Field(field.Date, nullable=True, name="date"),
                "bool_array": field.Field(
                    field.BoolArray, nullable=True, name="bool_array"
                ),
                "int_array": field.Field(
                    field.IntegerArray, nullable=True, name="int_array"
                ),
                "float_array": field.Field(
                    field.FloatArray, nullable=True, name="float_array"
                ),
                "date_array": field.Field(
                    field.DateArray, nullable=True, name="date_array"
                ),
                "string_array": field.Field(
                    field.StringArray, nullable=True, name="string_array"
                ),
                "string_array_2": field.Field(
                    field.StringArray, nullable=True, size=50, name="string_array_2"
                ),
            },
        )
        test_update.validate()

        test_model_ddl = (
            "CREATE TABLE table (int_ INT64 NOT NULL, int_2 INT64,"
            " float_ FLOAT64 NOT NULL, float_2 FLOAT64,"
            " string STRING(MAX) NOT NULL, string_2 STRING(MAX),"
            " string_3 STRING(10),"
            " timestamp TIMESTAMP NOT NULL,"
            " timestamp_2 TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
            " date DATE,"
            " bool_array ARRAY<BOOL>,"
            " int_array ARRAY<INT64>,"
            " float_array ARRAY<FLOAT64>,"
            " date_array ARRAY<DATE>,"
            " string_array ARRAY<STRING(MAX)>,"
            " string_array_2 ARRAY<STRING(50)>) PRIMARY KEY (int_, float_, string)"
        )

        self.assertEqual(test_update.ddl(), test_model_ddl)

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_create_table_interleaved(self, get_model):
        get_model.return_value = None
        new_model = models.ChildTestModel
        test_update = update.CreateTable(new_model)
        test_update.validate()

        test_model_ddl = (
            "CREATE TABLE ChildTestModel ("
            "key STRING(MAX) NOT NULL, "
            "child_key STRING(MAX) NOT NULL) "
            "PRIMARY KEY (key, child_key), "
            "INTERLEAVE IN PARENT SmallTestParentModel ON DELETE CASCADE"
        )
        self.assertEqual(test_update.ddl(), test_model_ddl)

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_create_table_interleaved_parent(self, get_model):
        get_model.return_value = None
        new_model = models.SmallTestParentModel
        test_update = update.CreateTable(new_model)
        test_update.validate()

        test_model_ddl = (
            "CREATE TABLE SmallTestParentModel ("
            "key STRING(MAX) NOT NULL, "
            "value_1 STRING(MAX) NOT NULL, "
            "value_2 STRING(MAX)) "
            "PRIMARY KEY (key)"
        )
        self.assertEqual(test_update.ddl(), test_model_ddl)

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_create_table_interleaved_no_model(self, get_model):
        get_model.return_value = None
        test_update = update.CreateTable(
            table_name="ChildTestModel",
            primary_keys=["key", "child_key"],
            fields={
                "key": field.Field(field.String, primary_key=True, name="key"),
                "child_key": field.Field(
                    field.String, primary_key=True, name="child_key"
                ),
            },
            interleaved=models.SmallTestModel,
        )
        test_update.validate()

        test_model_ddl = (
            "CREATE TABLE ChildTestModel ("
            "key STRING(MAX) NOT NULL, "
            "child_key STRING(MAX) NOT NULL) "
            "PRIMARY KEY (key, child_key), "
            "INTERLEAVE IN PARENT SmallTestModel ON DELETE CASCADE"
        )
        self.assertEqual(test_update.ddl(), test_model_ddl)

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_create_table_foreign_keys(self, get_model):
        get_model.return_value = None
        new_model = models.RelationshipTestModel
        test_update = update.CreateTable(new_model)
        test_update.validate()

        test_model_ddl = (
            "CREATE TABLE RelationshipTestModel ("
            "parent_key STRING(MAX) NOT NULL, "
            "child_key STRING(MAX) NOT NULL, "
            "CONSTRAINT parent FOREIGN KEY (parent_key) REFERENCES spanner_orm.tests.models.SmallTestModel (key), "
            "CONSTRAINT parents FOREIGN KEY (parent_key) REFERENCES spanner_orm.tests.models.SmallTestModel (key), "
            "CONSTRAINT fk_multicolumn FOREIGN KEY (parent_key, parent_key2) REFERENCES spanner_orm.tests.models.SmallTestModel (key, key2)) "
            "PRIMARY KEY (parent_key, child_key)"
        )
        self.assertEqual(test_update.ddl(), test_model_ddl)

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_create_table_foreign_keys_no_model(self, get_model):
        get_model.return_value = None
        test_update = update.CreateTable(
            table_name="RelationshipTestModel",
            primary_keys=["parent_key", "child_key"],
            fields={
                "parent_key": field.Field(
                    field.String, primary_key=True, name="parent_key"
                ),
                "child_key": field.Field(
                    field.String, primary_key=True, name="child_key"
                ),
            },
            relations={
                "parent": relationship.Relationship(
                    "spanner_orm.tests.models.SmallTestModel",
                    {"parent_key": "key"},
                    single=True,
                ),
                "parents": relationship.Relationship(
                    "spanner_orm.tests.models.SmallTestModel", {"parent_key": "key"}
                ),
                "fk_multicolumn": relationship.Relationship(
                    "spanner_orm.tests.models.SmallTestModel",
                    {"parent_key": "key", "parent_key2": "key2"},
                ),
            },
        )
        test_update.validate()

        test_model_ddl = (
            "CREATE TABLE RelationshipTestModel ("
            "parent_key STRING(MAX) NOT NULL, "
            "child_key STRING(MAX) NOT NULL, "
            "CONSTRAINT parent FOREIGN KEY (parent_key) REFERENCES spanner_orm.tests.models.SmallTestModel (key), "
            "CONSTRAINT parents FOREIGN KEY (parent_key) REFERENCES spanner_orm.tests.models.SmallTestModel (key), "
            "CONSTRAINT fk_multicolumn FOREIGN KEY (parent_key, parent_key2) REFERENCES spanner_orm.tests.models.SmallTestModel (key, key2)) "
            "PRIMARY KEY (parent_key, child_key)"
        )
        self.assertEqual(test_update.ddl(), test_model_ddl)

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_create_table_error_on_existing_table(self, get_model):
        get_model.return_value = models.SmallTestModel
        new_model = models.SmallTestModel
        test_update = update.CreateTable(new_model)
        with self.assertRaisesRegex(error.SpannerError, "already exists"):
            test_update.validate()

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.indexes")
    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.tables")
    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_drop_table(self, get_model, tables, indexes):
        table_name = models.RelationshipTestModel.table
        get_model.return_value = models.RelationshipTestModel
        tables.return_value = {}
        indexes.return_value = {}

        test_update = update.DropTable(table_name)
        test_update.validate()
        self.assertEqual(test_update.ddl(), "DROP TABLE {}".format(table_name))

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_add_index(self, get_model):
        table_name = models.SmallTestModel.table
        get_model.return_value = models.SmallTestModel

        test_update = update.CreateIndex(table_name, "foo", ["value_1"])
        test_update.validate()
        self.assertEqual(
            test_update.ddl(), "CREATE INDEX foo ON {} (value_1)".format(table_name)
        )

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_add_index_model_index(self, get_model):
        table_name = models.SmallTestModel.table
        get_model.return_value = models.SmallTestModel
        idx = Index(["value_1"])
        idx.name = "foo"

        test_update = update.CreateIndex(table_name, model_index=idx)
        test_update.validate()
        self.assertEqual(
            test_update.ddl(), "CREATE INDEX foo ON {} (value_1)".format(table_name)
        )

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_add_unique_index(self, get_model):
        table_name = models.SmallTestModel.table
        get_model.return_value = models.SmallTestModel

        test_update = update.CreateIndex(table_name, "foo", ["value_1"], unique=True)
        test_update.validate()
        self.assertEqual(
            test_update.ddl(),
            "CREATE UNIQUE INDEX foo ON {} (value_1)".format(table_name),
        )

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_add_null_filtered_index(self, get_model):
        table_name = models.SmallTestModel.table
        get_model.return_value = models.SmallTestModel

        test_update = update.CreateIndex(
            table_name, "foo", ["value_1"], null_filtered=True
        )
        test_update.validate()
        self.assertEqual(
            test_update.ddl(),
            "CREATE NULL_FILTERED INDEX foo ON {} (value_1)".format(table_name),
        )

    @mock.patch("spanner_orm.admin.metadata.SpannerMetadata.model")
    def test_add_null_filtered_unique_index(self, get_model):
        table_name = models.SmallTestModel.table
        get_model.return_value = models.SmallTestModel

        test_update = update.CreateIndex(
            table_name, "foo", ["value_1"], unique=True, null_filtered=True
        )
        test_update.validate()
        self.assertEqual(
            test_update.ddl(),
            "CREATE UNIQUE NULL_FILTERED INDEX foo ON {} (value_1)".format(table_name),
        )


if __name__ == "__main__":
    logging.basicConfig()
    unittest.main()
