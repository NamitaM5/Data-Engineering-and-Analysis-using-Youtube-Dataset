import awswrangler as wr
import pandas as pd
import urllib.parse
import os

os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:

        # Creating DF from content
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))
        # Use pandas.read_json() to directly read JSON strings or files as pandas.DataFrame instead of an object consisting of a dictionary or list.

        
        # Extract required columns:
        df_step_1 = pd.json_normalize(df_raw['items'])
        # pandas.json_normalize() - converts the nested dictionaries into separate columns for each key.
        #l_nested = [{'name': 'Alice', 'age': 25, 'id': {'x': 2, 'y': 8}},
        #             {'name': 'Bob', 'id': {'x': 10, 'y': 4}}]
        #
        #Difference between pd.DataFrame & pd.json_normalize
        #print(pd.DataFrame(l_nested))
        #     name   age                 id
        # 0  Alice  25.0   {'x': 2, 'y': 8}
        # 1    Bob   NaN  {'x': 10, 'y': 4}
        #
        #print(pd.json_normalize(l_nested))
        #     name   age  id.x  id.y
        # 0  Alice  25.0     2     8
        # 1    Bob   NaN    10     4

        # Write to S3
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation
        )

        return wr_response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
