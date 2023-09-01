import json


def hello(event, context):
    print("event:", event)
    print("context:", context)

    from big_query_converter import BigQueryConverterInteractor

    sql_query = event["body"]["sql_query"]
    util = BigQueryConverterInteractor()
    updated_query = util.get_converted_sql_query(sql_query=sql_query)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "updated_query": updated_query,
            }
        )
    }
