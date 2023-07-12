import json
import os
import logging
import fnmatch
import datetime
import argparse
import sys

import boto3
from botocore.exceptions import ClientError

from file_managment import read_file, write_file, find_files
from config import logging_basic_config, aws_initial_config


if __name__ == '__main__':
    logging_basic_config()

    logging.info('Process start - lambda invoke')

    config_file = read_file('settings.json')
    
    try:
        json_dir = 'files/'
        json_list = find_files('*.json', json_dir, True)
        path_json = json_list[0]
        json_file = read_file(path_json)
    except FileNotFoundError as e:
        logging.error(f'Process finished with error - lambda invoke: {e!r}')
        logging.info(f'Ingestion ends')
        raise

    try:
        lambda_client = aws_initial_config('lambda', config_file)

        data = {
            'Bucket': config_file['aws']['bucket'],
            'Key': f"{config_file['aws']['key']}/{os.path.basename(path_json)}"
        }

        response_invoke_lambda = lambda_client.invoke(FunctionName=config_file['aws']['lambda_function'],
                                                      InvocationType='RequestResponse',
                                                      LogType="None",
                                                      Payload=json.dumps(data)
                                                      )
        
        print("response invocation")
        print(response_invoke_lambda)
        
        if "Error" in response_invoke_lambda:
            raise Exception(f"Error en la nube: {response_invoke_lambda['Error']}")
        
        payload = json.loads(response_invoke_lambda['Payload'].read())

        print("payload")
        print(payload)
        
        print("payloads keys")
        print(payload.keys())
        
        if "errorMessage" in payload:
            raise Exception(f"Error en la nube: {payload['errorMessage']}")
        
        json_file['Arguments'] = payload['Arguments']
        write_file(path_json, json_file)
    except Exception as e:
        print(e)
        logging.error("Process finished with error - invoking lambda: %r", e)
        logging.info(f'Ingestion ends')
        raise
