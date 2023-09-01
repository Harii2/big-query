import json


def hello(event, context):
    print("event:", event)
    print("context:", context)

    from big_query_converter import BigQueryConverterInteractor
    import exceptions

    sql_query = event["body"]["sql_query"]
    try:
        util = BigQueryConverterInteractor()
        updated_query = util.get_converted_sql_query(sql_query=sql_query)
    except exceptions.TableNamesMappingNotFound as err:
        return {"statusCode": 400, "body": json.dumps({
            "reason": f"TableNameMappingNotFound: {err.table_names}"
        })}
    except exceptions.NoMappingFoundForFieldNames as err:
        return {"statusCode": 400, "body": json.dumps({
            "reason": f"NoMappingFoundForFieldNames: {err.field_names}"
        })}

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "updated_query": updated_query,
            }
        )
    }
