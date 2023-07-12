import os
import json
import logging
import datetime
import argparse
import fnmatch
import time
import pprint
import sys

import boto3 

from file_managment import read_file, write_file, find_files
from config import logging_basic_config, set_parameters, aws_initial_config


if __name__ == '__main__':
    logging_basic_config()

    logging.info('Process start - glue job execution')
    config_file = read_file('settings.json')

    try:
        json_dir = 'files/'
        json_list = find_files('*.json', json_dir, True)
        path_json = json_list[0]
        json_file = read_file(path_json)
    except FileNotFoundError as e:
        logging.error(f'Process finished with error - glue job execution: {e!r}')
        logging.info(f'Ingestion ends')
        raise
       
    try: 
        glue_client = aws_initial_config('glue', config_file)
        
        response_start_job_run = glue_client.start_job_run(JobName=json_file['Arguments']['--gluejobname'],
                                                           Arguments=json_file['Arguments'])
        
        print("response start_job_run")
        print(response_start_job_run)
        
        status = 'IN-PROCESS'
        finished_status = ['STOPPED', 'SUCCEEDED', 'FAILED']
        
        while status not in finished_status:
            time.sleep(config_file['config']['sleep'])
            
            response_job_run = glue_client.get_job_run(JobName=json_file['Arguments']['--gluejobname'],
                                                       RunId=response_start_job_run['JobRunId']
                                                       )
            
            print("response get_job_run")
            print(response_job_run)              
            status = response_job_run['JobRun']['JobRunState']
	
        if status != 'SUCCEEDED':
            raise ValueError(f"la ejecucion del job de glue termino con estado", status)
        
        logging.info(f'Ingestion ends')
    except Exception as e:
        logging.error('Process finished with error - glue job execution: %r', e)
        logging.info(f'Ingestion ends')
        raise
