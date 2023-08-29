import json


def hello(event, context):
    print("event:", event)
    print("context:", context)

    from big_query_sql_script import SQLQueryConversion
    sql_query = event["body"]["sql_query"]
    util = SQLQueryConversion()
    updated_query, query_data = util.get_converted_sql_query(sql_query=sql_query)
    response = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "updated_query": updated_query,
                # "query_data": query_data
            }
        )
    }

    return response
