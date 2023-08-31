import json
import sqlglot
from typing import Tuple, Dict, List

import exceptions


class BigQueryConverterInteractor:

    def __init__(self):
        table_mapping, field_mapping = type(self)._fetch_required_data_mappings()

        self.table_mapping = table_mapping
        # field mapping key format -> "{Template Name}#{Field Name}"
        self.field_mapping = field_mapping

    def get_converted_sql_query(self, sql_query: str) -> str:
        select_expression = sqlglot.parse_one(self.format_sql_query(sql_query))
        table_name = self._get_table_name_from_select_expression(select_expression)
        field_names = [
            expression.this.name
            for expression in list(select_expression.find_all(sqlglot.expressions.Column))
        ]
        # where_field_name = [arg.where.this.name for arg in select_expression.args]
        bq_table_name = self.table_mapping.get(table_name)
        no_mapping_field_names = [
            f_name for f_name in field_names
            if not self.field_mapping.get(self._prep_field_mapping_key(
                table_name=table_name,
                field_name=f_name
            ))
        ]
        if not bq_table_name:
            raise exceptions.TableNameMappingNotFound(table_name=table_name)
        if no_mapping_field_names:
            field_names = [f_name for f_name in field_names if f_name not in no_mapping_field_names]
            # raise exceptions.NoMappingFoundForFieldNames(field_names=field_names)

        updated_query = self._replace_table_name(
            table_name=table_name,
            bq_table_name=bq_table_name,
            sql_query=sql_query
        )
        updated_query = self._replace_field_names(
            sql_query=updated_query,
            table_name=table_name,
            field_names=field_names
        )
        return updated_query

    def _replace_field_names(
        self, sql_query: str, table_name: str, field_names: List[str]
    ) -> str:
        for f_name in field_names:
            field_mapping_key = self._prep_field_mapping_key(
                table_name=table_name,
                field_name=f_name
            )
            bq_field_name = self.field_mapping[field_mapping_key]["bigquery_column_name"]
            sql_query = sql_query.replace(f_name, bq_field_name)
        return sql_query

    @staticmethod
    def _replace_table_name(table_name: str, bq_table_name: str, sql_query: str) -> str:
        return (
            sql_query.replace(f'"{table_name}"', bq_table_name)
            .replace(f"`{table_name}`", bq_table_name)
            .replace(f"'{table_name}'", bq_table_name)
            .replace(f" {table_name} ", f" {bq_table_name} ")
            .replace(f" {table_name}", f" {bq_table_name}")
            .replace(f" {table_name};", f" {bq_table_name};")
        )

    @staticmethod
    def _get_table_name_from_select_expression(
        select_expression: sqlglot.expressions.Expression
    ) -> str:
        return select_expression.args["from"].this.name

    @classmethod
    def _fetch_required_data_mappings(cls) -> Tuple[Dict[str, str], Dict[str, Dict]]:
        file_path = "tables.json"
        with open(file_path, "r") as json_file:
            data = json.load(json_file)

        table_mapping, field_mapping = {}, {}
        for table_dict in data:
            table_name = table_dict["Table Name"]
            table_mapping[table_name] = f"`{table_dict['big_query_table_name']}`"
            for field_dict in table_dict["fields"]:
                field_dict["bigquery_column_name"] = f"`{field_dict['bigquery_column_name']}`"
                field_key_ = cls._prep_field_mapping_key(
                    table_name=table_name,
                    field_name=field_dict["field_name"]
                )
                field_mapping[field_key_] = field_dict
        return table_mapping, field_mapping

    @staticmethod
    def _prep_field_mapping_key(table_name: str, field_name: str) -> str:
        return f"{table_name}#{field_name}"

    @staticmethod
    def format_sql_query(query: str):
        return query.replace("`", "'")


# if __name__ == "__main__":
#     interactor = BigQueryConverterInteractor()
#     input_sql_query = "select count(CASE WHEN creation_datetime = 2020 THEN 1 END) as ct, lead_id , live_session_qualified_tag as act from online_live_session_qualification where live_session_qualified_channel like '%12%' and live_session_qualified_tag > 123 HAVING act > 3;"
#     print(interactor.get_converted_sql_query(input_sql_query))
