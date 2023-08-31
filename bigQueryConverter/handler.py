import json


def hello(event, context):
    print("event:", event)
    print("context:", context)

    from big_query_converter import BigQueryConverterInteractor
    import exceptions

    sql_query = event["body"]["sql_query"]
    try:
        util = BigQueryConverterInteractor()
        updated_query, query_data = util.get_converted_sql_query(sql_query=sql_query)
    except exceptions.TableNameMappingNotFound as err:
        return {"statusCode": 400, "body": json.dumps({
            "reason": f"TableNameMappingNotFound: {err.table_name}"
        })}

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "updated_query": updated_query,
                # "query_data": query_data
            }
        )
    }
