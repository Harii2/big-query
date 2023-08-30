import json
import re
from copy import deepcopy
from typing import Dict, List, Tuple

from sqlglot import exp, parse_one

import exceptions


def format_sql_query(sql_query: str):
    sql_query = sql_query.replace("`", "'")
    return sql_query


class SQLQueryConversion:
    def __init__(self):
        table_mappings, field_mappings = type(self)._fetch_required_data_mappings()

        self.table_mappings = table_mappings
        self.field_mappings = self._update_template_mappings(
            field_mappings=deepcopy(field_mappings)
        )
        self.mapped_fields_dict = {}
        self.aliases = []

    def get_converted_sql_query(self, sql_query: str) -> Tuple[str, Dict]:
        sql_query_updated = self._get_sql_query_with_replacing_field_names(
            sql_query=sql_query
        )

        query_data = self._parse_sql_query(sql_query)

        table_name = query_data["table_name"]

        mapped_table_name = self.table_mappings.get(table_name)
        if not mapped_table_name:
            raise exceptions.TableNameMappingNotFound(table_name=table_name)

        sql_query_updated = sql_query_updated.replace("\n", " ")

        sql_query_updated = sql_query_updated.replace('"', "'")

        # Replace Table Name (We can update this at the Parse Script,
        # if we do that but we can't get parsed query without replacing
        # fields, or table_name with their respective mapped field)
        sql_query_updated = (
            sql_query_updated.replace(f'"{table_name}"', mapped_table_name)
            .replace(f"`{table_name}`", mapped_table_name)
            .replace(f"'{table_name}'", mapped_table_name)
            .replace(f" {table_name} ", f" {mapped_table_name} ")
            .replace(f" {table_name}", f" {mapped_table_name}")
            .replace(f" {table_name};", f" {mapped_table_name};")
        )
        print("Original sql query(json):", json.dumps(sql_query))
        print("Updated sql query:", sql_query_updated)
        return sql_query_updated, query_data

    @staticmethod
    def _prepare_expression_result_for_alias_expression(expression_response):
        alias_expression = expression_response["alias_expression"]

        alias_expression_type = alias_expression.get("type")
        alias = expression_response["alias"]

        result = {}

        if alias_expression_type == "count_method":
            aggregation_dict = expression_response["alias_expression"]

            aggregation_dict.update({"alias": alias})

            alias_response = {
                "aggregation": aggregation_dict,
                "aggregation_type": "COUNT",
                "alias": alias,
                "type": "alias_aggregation",
            }
        elif alias_expression_type == "avg_method":
            aggregation_dict = expression_response["alias_expression"]

            aggregation_dict.update({"alias": alias})

            alias_response = {
                "aggregation": aggregation_dict,
                "aggregation_type": "AVG",
                "alias": alias,
                "type": "alias_aggregation",
            }
        elif alias_expression_type == "column":
            alias_field = alias_expression["field"]

            # mapping_field, mapping_field_type =
            # self.get_field_mapping_if_exists(field_name=alias_field)
            #
            # if not mapping_field:
            #     mapping_field = alias_field

            alias_response = {
                "type": "alias_column",
                "alias": alias,
                "field": alias_field,
            }
        elif alias_expression_type == "methods":
            aggregation_dict = expression_response["alias_expression"]

            aggregation_dict.update({"alias": alias})

            alias_response = {
                "aggregation": aggregation_dict,
                "aggregation_type": "METHOD",
                "alias": alias,
                "type": "alias_aggregation",
            }
        elif alias_expression_type == "sum_method":
            aggregation_dict = expression_response["alias_expression"]
            aggregation_dict.update({"alias": alias})

            alias_response = {
                "alias": alias,
                "aggregation": aggregation_dict,
                "aggregation_type": "SUM",
                "type": "alias_aggregation",
            }
        elif alias_expression_type == "multiplication_operation":
            aggregation_dict = expression_response["alias_expression"]
            aggregation_dict.update({"alias": alias})

            alias_response = {
                "alias": alias,
                "aggregation": aggregation_dict,
                "aggregation_type": "MULTIPLY",
                "type": "alias_aggregation",
            }
        elif alias_expression_type == "division_operation":
            aggregation_dict = expression_response["alias_expression"]
            aggregation_dict.update({"alias": alias})

            alias_response = {
                "alias": alias,
                "aggregation": aggregation_dict,
                "aggregation_type": "DIVISION",
                "type": "alias_aggregation",
            }
        elif alias_expression_type == "month_method":
            aggregation_dict = expression_response["alias_expression"]

            aggregation_dict.update({"alias": alias})

            alias_response = {
                "aggregation": aggregation_dict,
                "aggregation_type": "MONTH",
                "alias": alias,
                "type": "alias_aggregation",
            }
        elif alias_expression_type == "week_method":
            aggregation_dict = expression_response["alias_expression"]

            aggregation_dict.update({"alias": alias})

            alias_response = {
                "aggregation": aggregation_dict,
                "aggregation_type": "WEEK",
                "alias": alias,
                "type": "alias_aggregation",
            }
        elif alias_expression_type == "datetrunc_method":
            aggregation_dict = expression_response["alias_expression"]

            aggregation_dict.update({"alias": alias})

            alias_response = {
                "aggregation": aggregation_dict,
                "aggregation_type": "DATE_TRUNC",
                "alias": alias,
                "type": "alias_aggregation",
            }
        elif alias_expression_type == "date_method":
            aggregation_dict = expression_response["alias_expression"]

            aggregation_dict.update({"alias": alias})

            alias_response = {
                "aggregation": aggregation_dict,
                "aggregation_type": "DATE",
                "alias": alias,
                "type": "alias_aggregation",
            }
        elif alias_expression_type == "parenthesis":
            aggregation_dict = expression_response["alias_expression"]

            aggregation_dict.update({"alias": alias})

            alias_response = {
                "aggregation": aggregation_dict,
                "aggregation_type": "PARENTHESIS",
                "alias": alias,
                "type": "alias_aggregation",
            }
        else:
            print(
                f"Alias Response type '{alias_expression_type}' supported in "
                f"Generic Case",
                alias_expression,
            )
            aggregation_dict = expression_response.get("alias_expression", {})
            aggregation_dict.update({"alias": alias})
            alias_response = {
                "aggregation": aggregation_dict,
                "alias": alias,
                "type": "alias_aggregation",
            }

        result.update(alias_response)
        # print(f"Alias expression response: {alias_response}")
        return result

    def _prepare_expression_result_for_select_expression_response(
        self,
        select_expression_responses: List,
    ):
        result = {}

        columns = []
        aliases = []
        aggregations = []

        for expression_response in select_expression_responses:
            expression_response_type = expression_response.get("type")

            if expression_response_type == "column":
                column_name = expression_response["field"]
                columns.append(column_name)

            elif expression_response_type == "count_method":
                aggregations.append(expression_response)
            elif expression_response_type == "datetrunc_method":
                aggregations.append(expression_response)
            elif expression_response_type == "date_method":
                aggregations.append(expression_response)
            elif expression_response_type == "year_method":
                aggregations.append(expression_response)
            elif expression_response_type == "month_method":
                aggregations.append(expression_response)
            elif expression_response_type == "week_method":
                aggregations.append(expression_response)
            elif expression_response_type == "alias_column":
                alias_response = (
                    self._prepare_expression_result_for_alias_expression(
                        expression_response
                    )
                )

                if alias_response.get("aggregation"):
                    aggregations.append(alias_response["aggregation"])

                if alias_response:
                    aliases.append(alias_response)

                if alias_response.get("field"):
                    columns.append(alias_response["field"])  # TODO Update
            elif expression_response_type == "methods":
                methods = expression_response.get("methods")

                if len(methods.keys()) > 1:
                    print(
                        "Expression result not supported for multiple methods",
                        methods,
                    )
                    return result
                elif not len(methods.keys()):
                    print("Expression result not supported with 0 methods")
                    return result

                method_type = list(methods.keys())[0]

                if method_type == "WEEKDAY":
                    method_params = methods[method_type]

                    if not method_params or len(method_params) > 1:
                        print(
                            f"Expression result with '{method_type}' must "
                            f"only have 1 param, got {len(method_params)}"
                        )
                        return result

                    custom_expression_response = {
                        "field": method_params[0],
                        "type": "week_day_method",
                    }

                    aggregations.append(custom_expression_response)
                else:
                    print(
                        f"Select Expression result not supported for '"
                        f"{method_type}'"
                    )
            else:
                print(
                    f"Expression Response type '{expression_response_type}' "
                    f"not supported",
                    select_expression_responses,
                )

        result.update(
            {
                "columns": columns,
                "aliases": aliases,
                "aggregations": aggregations,
            }
        )

        return result

    def _prepare_expression_result(
        self, expression, parent_key=None, parent_field=None, source=None
    ):
        expression_type = expression.key
        #     print(expression, "Expression", expression_type)

        response = {}

        if expression_type == "column":
            field_name = expression.this.output_name

            # mapping_field, mapping_field_type =
            # self.get_field_mapping_if_exists(field_name=field_name)
            #
            # if not mapping_field:
            #     mapping_field = field_name

            # print(f"field: {field_name}, mapping: {mapping_field}")
            response = {
                "field": field_name,
                "type": "column",
            }  # TODO Add field Mapping here
        elif expression_type == "literal":
            field_val = expression.output_name

            response = {"value": field_val, "type": "literal"}
        elif expression_type == "where":
            response = self._prepare_expression_result(
                expression.this, parent_key=expression_type, source=source
            )
        elif expression_type == "select":
            select_expression_responses = []
            groups = []

            for iter_key, iter_expression in expression.iter_expressions():
                #             print(iter_key, type(iter_key), iter_expression)

                if iter_key == "from":
                    response.update(
                        {
                            "table_name": self._prepare_expression_result(
                                iter_expression, source=source
                            )
                        }
                    )
                elif iter_key == "where":
                    response.update(
                        {
                            "conditions": self._prepare_expression_result(
                                iter_expression, source=source
                            )
                        }
                    )
                elif iter_key == "limit":
                    response.update(
                        self._prepare_expression_result(
                            iter_expression,
                            parent_key=expression_type,
                            source=source,
                        )
                    )
                elif iter_key == "order":
                    response.update(
                        {
                            "order_by": self._prepare_expression_result(
                                iter_expression, source=source
                            )
                        }
                    )
                elif iter_key == "expressions":
                    select_expression_responses.append(
                        self._prepare_expression_result(
                            iter_expression,
                            parent_key=expression_type,
                            source=source,
                        )
                    )
                elif iter_key == "group":
                    groups = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                else:
                    print(
                        f"'SELECT' Expression Type -> '{iter_key}' Not handled"
                    )

            print(f"groups: {groups}")
            if groups:
                response.update({"group_by_fields": groups})

            select_expression_result = (
                self._prepare_expression_result_for_select_expression_response(
                    select_expression_responses=select_expression_responses
                )
            )

            response.update(select_expression_result)
        elif expression_type == "subquery":
            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    sub_query_response = self._prepare_expression_result(
                        iter_expression, source=source
                    )
                    response = {
                        "sub_query": sub_query_response,
                        "type": "subquery",
                    }
                else:
                    print(
                        f"'SUBQUERY' Expression Type -> '{iter_key}' Not "
                        f"handled"
                    )
        elif expression_type == "union":
            response = {}

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    left_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )

                    response = left_val

                elif iter_key == "expression":
                    right_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                    response.setdefault("unions", [])
                    response["unions"].append(right_val)
                else:
                    print(
                        f"'UNION' Expression Type -> '{iter_key}' Not handled"
                    )

        elif expression_type == "from":
            for iter_key, iter_expression in expression.iter_expressions():
                #             print(iter_key, type(iter_key), iter_expression)
                if iter_key == "this":
                    response = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                else:
                    print(
                        f"'FROM' Expression Type -> '{iter_key}' Not handled"
                    )
        elif expression_type == "table":
            response = self._prepare_expression_result(
                expression.this, source=source
            )
            # TODO Add Table Mapping Here
        elif expression_type == "identifier":
            response = expression.output_name
        elif expression_type == "between":
            response = {}

            for iter_key, iter_expression in expression.iter_expressions():
                #             print(iter_key, type(iter_key), iter_expression)

                if iter_key == "this":
                    field_info = self._prepare_expression_result(
                        iter_expression, source=source
                    )

                    between_field = field_info  # TODO Apply field Mapping here

                    response = {
                        "type": "between_condition",
                        "field": between_field,
                    }

                elif iter_key == "low":
                    response.update(
                        {
                            "low": self._prepare_expression_result(
                                iter_expression, source=source
                            )
                        }
                    )
                elif iter_key == "high":
                    response.update(
                        {
                            "high": self._prepare_expression_result(
                                iter_expression, source=source
                            )
                        }
                    )
                else:
                    print(
                        f"'BETWEEN' Expression Type -> '{iter_key}' Not "
                        f"handled"
                    )
        elif expression_type == "in":
            response = {}
            values_list = []

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    field_dict = self._prepare_expression_result(
                        expression=iter_expression, source=source
                    )

                    response = {
                        "type": "in_condition",
                        "field": field_dict,
                    }
                elif iter_key == "expressions":
                    field_value_option = self._prepare_expression_result(
                        expression=iter_expression, source=source
                    )
                    values_list.append(field_value_option)
                else:
                    print(f"'IN' Expression Type -> '{iter_key}' Not handled")

            if values_list:
                response.update({"values_list": values_list})
        elif expression_type == "anonymous":
            #         print(expression, "anonymous")
            method_key = str(expression.this)

            if method_key == "GETDATE":
                response = {"methods": {"CURDATE": []}, "type": "methods"}
            else:
                method_params = []

                for iter_key, iter_expression in expression.iter_expressions():
                    param_resp = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )

                    method_params.append(param_resp) if param_resp else None

                if method_key == "DATEADD":
                    response = {
                        "params": method_params,
                        "type": "date_add_method",
                    }

                else:
                    response = {
                        "methods": {method_key: method_params},
                        "type": "methods",
                    }

        elif expression_type == "eq":
            where_field = ""  # TODO Add field Mapping Here
            where_val = ""

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    where_field_exp_result = self._prepare_expression_result(
                        iter_expression, source=source
                    )
                    where_field = where_field_exp_result

                else:
                    where_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        parent_field=where_field,
                        source=source,
                    )

            if where_field:
                response = {
                    "field": where_field,
                    "value": where_val,
                    "type": "eq_condition",
                }

        elif expression_type == "neq":
            where_field = ""  # TODO Add field Mapping Here
            where_val = ""

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    where_field_exp_result = self._prepare_expression_result(
                        iter_expression, source=source
                    )
                    where_field = where_field_exp_result

                else:
                    where_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )

            if where_field:
                response = {
                    "field": where_field,
                    "value": where_val,
                    "type": "neq_condition",
                }
        elif expression_type == "gt":
            where_field = ""  # TODO Add field Mapping Here
            where_val = ""

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    where_field_exp_result = self._prepare_expression_result(
                        iter_expression, source=source
                    )
                    where_field = where_field_exp_result

                else:
                    where_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )

            if where_field:
                response = {
                    "field": where_field,
                    "value": where_val,
                    "type": "gt_condition",
                }

        elif expression_type == "lt":
            where_field = ""  # TODO Add field Mapping Here
            where_val = ""

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    where_field_exp_result = self._prepare_expression_result(
                        iter_expression, source=source
                    )
                    where_field = where_field_exp_result

                else:
                    where_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )

            if where_field:
                response = {
                    "field": where_field,
                    "value": where_val,
                    "type": "lt_condition",
                }
        elif expression_type == "gte":
            where_field = ""  # TODO Add field Mapping Here
            where_val = ""

            for iter_key, iter_expression in expression.iter_expressions():
                #             print("GTE", iter_key, iter_expression,
                #             type(iter_expression))
                if iter_key == "this":
                    where_field_exp_result = self._prepare_expression_result(
                        iter_expression, source=source
                    )
                    where_field = where_field_exp_result
                else:
                    where_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )

            if where_field:
                response = {
                    "field": where_field,
                    "value": where_val,
                    "type": "gte_condition",
                }
        elif expression_type == "lte":
            where_field = ""  # TODO Add field Mapping Here
            where_val = ""

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    where_field_exp_result = self._prepare_expression_result(
                        iter_expression, source=source
                    )
                    where_field = where_field_exp_result

                else:
                    where_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )

            if where_field:
                response = {
                    "field": where_field,
                    "value": where_val,
                    "type": "lte_condition",
                }
        elif expression_type == "and":
            conditional_items = []

            for iter_key, iter_expression in expression.iter_expressions():
                #             print(iter_key, iter_expression)
                expression_result = self._prepare_expression_result(
                    iter_expression, source=source
                )

                if expression_result:
                    conditional_items.append(expression_result)

            response.update(
                {"conditional_and": conditional_items, "type": "and_condition"}
            )
        elif expression_type == "or":
            conditional_items = []

            for iter_key, iter_expression in expression.iter_expressions():
                #             print(iter_key, iter_expression)
                expression_result = self._prepare_expression_result(
                    iter_expression, source=source
                )

                if expression_result:
                    conditional_items.append(expression_result)

            response.update(
                {"conditional_or": conditional_items, "type": "or_condition"}
            )
        elif expression_type == "not":
            response = self._prepare_expression_result(
                expression.this, parent_key=expression_type, source=source
            )
        elif expression_type == "neg":
            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    val = self._prepare_expression_result(
                        expression.this,
                        parent_key=expression_type,
                        source=source,
                    )
                    response = {"value": val, "type": "neg_sign"}
                else:
                    print(
                        f"'NEG' Expression Type -> '{iter_key}, "
                        f"{iter_expression.key}' Not handled"
                    )
        elif expression_type == "null":
            response = {"null": {}}
        elif expression_type == "ordered":
            is_desc_order = expression.args.get("desc")

            response = []

            for iter_key, iter_expression in expression.iter_expressions():
                field = self._prepare_expression_result(
                    iter_expression, parent_key=expression_type, source=source
                )

                if "field" not in field:
                    print(
                        f"Order by with '{field['type']}' is not supported",
                        field,
                    )
                    break

                response.append(
                    {
                        "field": field["field"],
                        "desc": is_desc_order,
                        "type": field["type"],
                    }
                )
        elif expression_type == "order":
            order_by_fields = []

            for iter_key, iter_expression in expression.iter_expressions():
                order_by_fields.extend(
                    self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                )

            response.update(
                {"fields": order_by_fields, "type": "order_by_method"}
            )
        elif expression_type == "group":
            group_by_items = []
            for iter_key, iter_expression in expression.iter_expressions():
                group_by_items.append(
                    self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                )

            response.update(
                {"fields": group_by_items, "type": "group_by_method"}
            )
        elif expression_type == "limit":
            response = {
                "limit": self._prepare_expression_result(
                    expression.expression, source=source
                ),
                "type": "limit",
            }
        elif expression_type == "star":
            response = {"field": "*", "type": "star"}
        elif expression_type == "alias":
            alias_name = ""
            alias_expression = ""

            for iter_key, iter_expression in expression.iter_expressions():
                #             print("ALIAS", iter_key, type(iter_expression),
                #             iter_expression.key, iter_expression)
                if iter_key == "alias":
                    alias_name = self._prepare_expression_result(
                        iter_expression, source=source
                    )
                elif iter_key == "this":
                    alias_expression = self._prepare_expression_result(
                        iter_expression, source=source
                    )
                else:
                    print(
                        f"'ALIAS' Expression Type -> '{iter_key}' Not handled"
                    )

            response = {
                "alias_expression": alias_expression,
                "alias": alias_name,
                "type": "alias_column",
            }
        elif expression_type == "count":
            count_fields = []

            for iter_key, iter_expression in expression.iter_expressions():
                #             print("COUNT", iter_key, type(iter_expression),
                #             iter_expression.key, iter_expression)

                count_fields.append(
                    self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                )

            if len(count_fields) > 1:
                print("Got Invalid no of Counts", expression)

            response = {"count": count_fields[0], "type": "count_method"}
        elif expression_type == "avg":
            avg_fields = []

            for iter_key, iter_expression in expression.iter_expressions():
                avg_fields.append(
                    self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                )

            if len(avg_fields) > 1:
                print("Got Invalid no of Avgs", expression)

            response = {"avg": avg_fields[0], "type": "avg_method"}
        elif expression_type == "sub":
            left_val = ""
            right_val = ""

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    left_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                else:
                    right_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
            response = {
                "left_val": left_val,
                "right_val": right_val,
                "type": "sub_operation",
            }
        elif expression_type == "interval":
            left_val = ""
            right_val = ""

            for iter_key, iter_expression in expression.iter_expressions():
                #             print("INTERVAL", iter_key,
                #             type(iter_expression), iter_expression.key,
                #             iter_expression)
                if iter_key == "this":
                    left_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                else:
                    right_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
            response = {
                "left_val": left_val,
                "right_val": right_val,
                "type": "interval_method",
            }

        elif expression_type == "var":
            var_val = expression.this

            response = {"value": var_val, "type": "var_method"}
        elif expression_type == "is":
            field_name = ""

            for iter_key, iter_expression in expression.iter_expressions():
                # print(iter_key, type(iter_expression.key),
                # iter_expression.key, iter_expression)
                if iter_expression.key == "column":
                    field_name = self._prepare_expression_result(
                        iter_expression, source=source
                    )["field"]

                elif iter_expression.key == "null":
                    if field_name:
                        response = {
                            "type": "is_condition",
                            "field": field_name,
                            "nullable": False
                            if parent_key and parent_key == "not"
                            else True,
                        }
                    else:
                        print(
                            f"'IS' Expression Type -> '{iter_key}' and empty "
                            f"Field Case Not handled"
                        )

                elif iter_expression.key == "literal":
                    field_name = iter_expression.output_name
                else:
                    print(
                        f"'IS' Expression Type -> '{iter_key}, "
                        f"{iter_expression.key}' Not handled"
                    )

            response.update({"type": "is_condition"})
        elif expression_type == "like":
            field_name = ""
            field_expression = ""

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_expression.key == "column":
                    field_name = self._prepare_expression_result(
                        iter_expression, source=source
                    )["field"]
                elif iter_expression.key == "literal":
                    field_expression = iter_expression.output_name
                else:
                    print(
                        f"'LIKE' Expression Type -> '{iter_key}, "
                        f"{iter_expression.key}' Not handled"
                    )

            if field_name and field_expression:
                response.update(
                    {
                        "type": "like_operator",
                        "field": field_name,
                        "expression": field_expression,
                    }
                )

        elif expression_type == "distinct":
            fields = []

            for iter_key, iter_expression in expression.iter_expressions():
                #             print("DISTINCT", iter_key,
                #             type(iter_expression.key), iter_expression.key,
                #             iter_expression)
                fields.append(
                    self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                )

            if len(fields) > 1:
                print("Got Invalid no of Distinct fields", expression)

            response = {"type": "distinct_method", "field": fields[0]}
        elif expression_type == "sum":
            sum_fields = []

            for iter_key, iter_expression in expression.iter_expressions():
                sum_fields.append(
                    self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                )

            if len(sum_fields) > 1:
                print("Got Invalid no of SUM fields", expression)

            response = {"field": sum_fields[0], "type": "sum_method"}
        elif expression_type == "mul":
            left_val = ""
            right_val = ""

            for iter_key, iter_expression in expression.iter_expressions():
                #             print("INTERVAL", iter_key,
                #             type(iter_expression), iter_expression.key,
                #             iter_expression)
                if iter_key == "this":
                    left_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                else:
                    right_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
            response = {
                "left_val": left_val,
                "right_val": right_val,
                "type": "multiplication_operation",
            }
        elif expression_type == "div":
            left_val = ""
            right_val = ""

            for iter_key, iter_expression in expression.iter_expressions():
                #             print("INTERVAL", iter_key,
                #             type(iter_expression), iter_expression.key,
                #             iter_expression)
                if iter_key == "this":
                    left_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                else:
                    right_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
            response = {
                "left_val": left_val,
                "right_val": right_val,
                "type": "division_operation",
            }
        elif expression_type == "paren":
            fields = []

            for iter_key, iter_expression in expression.iter_expressions():
                fields.append(
                    self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                )

            response = {
                "fields": fields,
                "type": "parenthesis",
            }
        elif expression_type == "case":
            default = ""
            cases = []

            for iter_key, iter_expression in expression.iter_expressions():
                #             print("CASE", iter_key, iter_expression,
                #             type(iter_expression))
                if iter_key == "default":
                    default = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                elif iter_key == "ifs":
                    cases.append(
                        self._prepare_expression_result(
                            iter_expression,
                            parent_key=expression_type,
                            source=source,
                        )
                    )
                else:
                    print(f"Case : type '{iter_key}' not handled")

            response = {
                "cases": cases,
                "default": default,
                "type": "case",
            }
        elif expression_type == "if":
            condition = None
            condition_value = None

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    condition = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                elif iter_key == "true":
                    condition_value = self._prepare_expression_result(
                        iter_expression, source=source
                    )
                else:
                    print(f"IF Condition with '{iter_key}' not handled")

            response = {
                "condition": condition,
                "condition_value": condition_value,
                "type": "if_condition",
            }
        elif expression_type == "date":
            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this" or iter_key == "expressions":
                    field = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                    response = {
                        "field": field,
                        "type": "date_method",
                    }
                else:
                    print(f"DATE: Expression type {iter_key} not handled yet")
        elif expression_type == "month":
            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    field = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                    response = {
                        "field": field,
                        "type": "month_method",
                    }
                else:
                    print(f"MONTH: Expression type {iter_key} not handled yet")
        elif expression_type == "year":
            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    field = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                    response = {
                        "field": field,
                        "type": "year_method",
                    }
                else:
                    print(f"YEAR: Expression type {iter_key} not handled yet")
        elif expression_type == "week":
            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    field = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                    response = {
                        "field": field,
                        "type": "week_method",
                    }
                else:
                    print(f"WEEK: Expression type {iter_key} not handled yet")
        elif expression_type == "datetrunc":
            response = {}

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    field = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                    response.update(
                        {
                            "field": field,
                            "type": "datetrunc_method",
                        }
                    )
                elif iter_key == "unit":
                    unit = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                    response.update(
                        {
                            "unit": unit,
                        }
                    )
                else:
                    print(
                        f"DATE_TRUNC: Expression type {iter_key} not handled "
                        f"yet"
                    )
        elif expression_type == "datesub":
            left_val = ""
            right_val = ""

            for iter_key, iter_expression in expression.iter_expressions():
                if iter_key == "this":
                    left_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
                else:
                    right_val = self._prepare_expression_result(
                        iter_expression,
                        parent_key=expression_type,
                        source=source,
                    )
            response = {
                "left_val": left_val,
                "right_val": right_val,
                "type": "sub_operation",
                "alias_for": "date_sub",
            }
        elif expression_type == "currentdate":
            response = {"methods": {"CURDATE": []}, "type": "methods"}
        else:
            print(f"Expression type {expression_type} not handled yet")

        if not response:
            print(f"Got Empty response for {expression_type} type")

        # Set cols for aggregations
        if type(response) is dict:
            for aggregation in response.get("aggregations", {}):
                if aggregation.get("alias"):
                    response.setdefault("columns", [])
                    response["columns"].append(
                        aggregation.get("alias")
                    ) if aggregation["alias"] not in response[
                        "columns"
                    ] else None

            for alias_dict in response.get("aliases", {}):
                if alias_dict.get("field"):
                    response.setdefault("columns", [])
                    response["columns"].append(
                        alias_dict["field"]
                    ) if alias_dict["field"] not in response[
                        "columns"
                    ] else None

        return response

    @staticmethod
    def _replace_whole_word(query, old_word, new_word):
        pattern = r"(?<!`)\b{}\b(?!`)".format(re.escape(old_word))
        replaced_query = re.sub(pattern, new_word, query)
        return replaced_query

    def _get_sql_query_with_replacing_field_names(self, sql_query):
        aliases = []

        for item in parse_one(format_sql_query(sql_query)).find_all(exp.Alias):
            aliases.append(item.output_name)

        field_names = []

        for item in parse_one(format_sql_query(sql_query)).find_all(
            exp.Column
        ):
            field_names.append(item.output_name)

        #     print("Field_names: ", field_names)

        mapped_fields_dict = {}

        for field_name in field_names:
            #         if field_name in aliases:
            #             continue

            (
                mapping_field,
                mapping_field_type,
            ) = self._get_field_mapping_if_exists(field_name)

            if mapping_field:
                mapped_fields_dict.update({mapping_field: field_name})

            if not mapping_field:
                mapping_field = field_name

            sql_query = (
                sql_query.replace(f"`{field_name}`", f"{field_name}")
                .replace(f'"{field_name}"', f"{field_name}")
                .replace(f"'{field_name}'", f"{field_name}")
            )

            if mapping_field == field_name:
                print(f"Mapping field_uuid not found for '{field_name}'")

            if "." in field_name:
                sql_query = self._replace_whole_word(
                    query=sql_query,
                    old_word=field_name,
                    new_word=f"`{mapping_field}`",
                )
            else:
                sql_query = self._replace_whole_word(
                    query=sql_query,
                    old_word=field_name,
                    new_word=f"{mapping_field}",
                )

        self.mapped_fields_dict = mapped_fields_dict

        return sql_query

    @staticmethod
    def _starts_with_any(string, prefixes):
        response = ""

        for prefix in prefixes:
            if string.startswith(prefix):
                response = prefix
        return response

    def _get_field_mapping_if_exists(self, field_name):
        # print("Field: ", field_name)

        matched_template = self._starts_with_any(
            field_name, list(self.field_mappings.keys())
        )

        if matched_template:
            template_id = self.field_mappings[matched_template].get(
                "sales_template_id"
            )

            updated_field_name = template_id

            field_name_without_template_name = field_name[
                len(matched_template) + 1 :
            ]

            matched_field = (
                field_name_without_template_name
                if field_name_without_template_name
                in self.field_mappings[matched_template]["fields"].keys()
                else None
            )

            if matched_field:
                matched_field_details = self.field_mappings[matched_template][
                    "fields"
                ][matched_field]

                field_uuid = matched_field_details.get("field_id")

                field_type = matched_field_details.get("field_type")

                updated_field_name += "." + field_uuid
                # print("Matched Field: ", updated_field_name)
                return updated_field_name, field_type
        # print("Not Matched Field: ", field_name)

        # Lead Field
        lead_template_fields = list(
            self.field_mappings.get("lead", {}).get("fields", {}).keys()
        )

        matched_field = (
            field_name if field_name in lead_template_fields else None
        )

        if matched_field:
            matched_field_details = self.field_mappings["lead"]["fields"][
                matched_field
            ]

            field_uuid = matched_field_details.get("field_id")

            field_type = matched_field_details.get("field_type")

            updated_field_name = field_uuid
            # print("Matched Field: ", updated_field_name)
            return updated_field_name, field_type

        return None, None

    def _parse_sql_query(self, sql_query: str):
        select_expression = parse_one(format_sql_query(sql_query))
        query_data = self._prepare_expression_result(
            expression=select_expression, source="OPEN_SEARCH_SQL_QUERY"
        )
        return query_data

    @staticmethod
    def _get_alias_config(alias, aliases):
        matched_alias = None

        for item in aliases:
            # print("Alias name: ", item.get("alias"))
            if item.get("alias") and item["alias"] == alias:
                matched_alias = item
                break
        return matched_alias

    def _get_field_for_given_alias(self, alias: str, aliases: List[Dict]):
        # print(f"Alias: {aliases}")
        matched_alias = self._get_alias_config(alias=alias, aliases=aliases)

        if matched_alias and matched_alias["type"] == "alias_column":
            return matched_alias["field"]
        elif matched_alias and matched_alias["type"] == "alias_aggregation":
            aggregation_dict = matched_alias["aggregation"]

            aggregation_type = aggregation_dict["type"]

            if aggregation_type == "count_method":
                aggregation_field = aggregation_dict["count"]

                if aggregation_field["type"] == "column":
                    return aggregation_field["field"]
                elif aggregation_field["type"] == "star":
                    return "_id"
                else:
                    print(
                        f"Alias Config Fetch with aggregation type "
                        f"{aggregation_type} and field_type '"
                        f"{aggregation_field['type']}' not supported"
                    )
            elif aggregation_type == "month_method":
                aggregation_field = aggregation_dict["field"]

                if aggregation_field["type"] == "column":
                    return aggregation_field["field"]
                elif aggregation_field["type"] == "star":
                    return "_id"
                else:
                    print(
                        f"Alias Config Fetch with aggregation type "
                        f"{aggregation_type} and field_type '"
                        f"{aggregation_field['type']}' not supported"
                    )
            elif aggregation_type == "week_method":
                aggregation_field = aggregation_dict["field"]

                if aggregation_field["type"] == "column":
                    return aggregation_field["field"]
                elif aggregation_field["type"] == "star":
                    return "_id"
                else:
                    print(
                        f"Alias Config Fetch with aggregation type "
                        f"{aggregation_type} and field_type '"
                        f"{aggregation_field['type']}' not supported"
                    )
            elif aggregation_type == "datetrunc_method":
                aggregation_field = aggregation_dict["field"]

                if aggregation_field["type"] == "column":
                    return aggregation_field["field"]
                elif aggregation_field["type"] == "star":
                    return "_id"
                else:
                    print(
                        f"Alias Config Fetch with aggregation type "
                        f"{aggregation_type} and field_type '"
                        f"{aggregation_field['type']}' not supported"
                    )
            elif aggregation_type == "date_method":
                aggregation_field = aggregation_dict["field"]

                if aggregation_field["type"] == "column":
                    return aggregation_field["field"]
                elif aggregation_field["type"] == "star":
                    return "_id"
                else:
                    print(
                        f"Alias Config Fetch with aggregation type "
                        f"{aggregation_type} and field_type '"
                        f"{aggregation_field['type']}' not supported"
                    )

            else:
                print(
                    f"Alias Config Fetch with aggregation type "
                    f"{aggregation_type} not supported"
                )

    @staticmethod
    def _get_alias_for_given_field(field: str, aliases: List[Dict]):
        matched_alias = None

        for item in aliases:
            # print("Alias name: ", item.get("alias"))
            if item.get("field") and item["field"] == field:
                matched_alias = item
                break

        if matched_alias and matched_alias["type"] == "alias_column":
            return matched_alias["alias"]

    @staticmethod
    def _update_template_mappings(field_mappings):
        updated_field_mappings = {}

        for mapping in field_mappings:
            template_name = mapping["normalized_name"]
            mapping["sales_template_id"] = f"`{mapping['sales_template_id']}`"
            updated_field_mappings[template_name] = mapping

            fields = (
                updated_field_mappings[template_name].pop("fields")
                if "fields" in updated_field_mappings[template_name]
                else []
            )

            updated_field_mappings[template_name].setdefault("fields", {})

            for field in fields:
                field_name = field["normalized_name"]
                field["field_id"] = f"`{field['field_id']}`"
                updated_field_mappings[template_name]["fields"][
                    field_name
                ] = field

        return updated_field_mappings

    @classmethod
    def _fetch_required_data_mappings(cls) -> Tuple[Dict[str, str], List[Dict]]:
        file_path = "tables.json"
        with open(file_path, "r") as json_file:
            data = json.load(json_file)

        table_wise_data_mappings = [
            cls._prep_table_data_mapping_json(table_dict)
            for table_dict in data
        ]
        table_mappings = {
            tb_data_mapping["sales_template_name"]: tb_data_mapping["normalized_name"]
            for tb_data_mapping in table_wise_data_mappings
        }
        return table_mappings, table_wise_data_mappings

    @classmethod
    def _prep_table_data_mapping_json(cls, table_dict: Dict) -> Dict:
        return {
            "sales_template_name": table_dict["Table Name"],
            "normalized_name": table_dict["Table Name"],
            "sales_template_id": table_dict["sales_template_id"],
            "sales_template_type": table_dict["sales_template_type"],
            "fields": [
                {
                    "field_name": field_dict["field_name"],
                    "normalized_name": field_dict["field_id"],
                    "field_id": field_dict["field_id"],
                    "field_type": field_dict["field_type"],
                } for field_dict in table_dict.get("fields", [])
            ]
        }
