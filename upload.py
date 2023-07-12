import argparse
import shutil
import os
import fnmatch
import base64
import json
import datetime
import logging
import boto3
import hashlib
import pprint
import sys
from pathlib import Path

from boto3.s3.transfer import TransferConfig

from file_managment import read_file, write_file, find_files
from config import logging_basic_config, aws_initial_config


if __name__ == '__main__':
    logging_basic_config()

    logging.info('Process start - upload')
    
    pp = pprint.PrettyPrinter(indent=4)
    
    config_file = read_file('settings.json')
    
    json_dir = "files/"
    
    try:
        json_list = find_files('*.json', json_dir, True)
    except FileNotFoundError as e:
        logging.error(f'Process finished with error - upload: {e!r}')
        logging.info(f'Ingestion ends')
        raise
    
    path_json = json_list[0]
    json_file = read_file(path_json)
    
    try: 
        orc_list = find_files('*.orc', json_dir, True)

        s3_client = aws_initial_config('s3', config_file)
    
        response_s3_list_objects = s3_client.list_objects_v2(Bucket=config_file['aws']['bucket'], 
                                                             Prefix=config_file['aws']['key'])
        
        print(response_s3_list_objects)

        if response_s3_list_objects['KeyCount']:
            list_files_to_delete = response_s3_list_objects['Contents']

            for file_orc in list_files_to_delete:
                if fnmatch.fnmatch(file_orc['Key'], "*"):
                    response_delete_objects = s3_client.delete_objects(Bucket=config_file['aws']['bucket'],
                                                                    Delete={'Objects': [{'Key': file_orc['Key']}]})
                
    except Exception as e:
        logging.error(f'Process finished with error - upload: {e!r}')
        logging.info(f'Ingestion ends')
        raise
    
    metadataOrc = []
    json_name = os.path.basename(path_json)
    
    for file_orc in orc_list:
        print("file orc::", file_orc)
        try:
            response_upload_file = s3_client.upload_file(file_orc, 
                                                            config_file['aws']['bucket'],
                                                            f"{config_file['aws']['key']}/{os.path.basename(file_orc)}")
        except Exception as e:
            logging.error(f'Process finished with error - upload: {e!r}')
        
    try:
        upload = s3_client.upload_file(path_json, 
                                       config_file['aws']['bucket'], 
                                       config_file['aws']['key'] + '/' + os.path.basename(path_json)
                                       )

        print("upload response::", upload)
    except Exception as e:
        logging.error(f'Process finished with error - upload: {e!r}')
        raise TypeError('FALLO PROCESO')
