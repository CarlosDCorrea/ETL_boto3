import logging 
import argparse 
import os

import boto3


def logging_basic_config() -> None:
    """Define la forma en la que se guardaran los logs de todo el proceso
    de ingestas, esto incluye el archivo contenedor, nivel general y formato de los logs.
    
    Parameters -> None
    
    Return -> None
    """
    logging.basicConfig(
    filename='logs/file.log',
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    )  

def set_parameters() -> None:
    """Define los parametros que recive el script, esto incluye 
    el nombre y otras opciones de los parametros como su tipo etc.
    
    Parameters -> None
    
    Return -> tuple(str)
    """
    # PARTITIONED_OPTIONS = ('YES', 'NO')
    # PARTITION_TYPE_OPTIONS = ('INCREMENTAL', 'FULL')

    parser = argparse.ArgumentParser()
    parser.add_argument('--database', type=str, required=True)  ##GEST
    parser.add_argument('--schema', type=str, required=True)  ##DATALAKE
    parser.add_argument('--table', type=str, required=True)  ##PTLF_ESTADISTICAS
    parser.add_argument('--partitioned', type=str, required=True)  ##YES  OR NO
    parser.add_argument('--partition_type', type=str, required=True)  ##INCREMENTAL OR FULL
    parser.add_argument('--date', type=str, required=False)  ##2021-01-06
    parser.add_argument('--db_type', type=str, required=True)  ##ORACLE
    parser.add_argument('--column_name', type=str, required=False)  ##COLUMN NAME
    parser.add_argument('--column_type', type=str, required=False)  ##TIMESTAMP OR DATE

    args = parser.parse_args()
    
    return (
        args.database.upper(),
        args.schema.upper(), 
        args.table.upper(), 
        args.partitioned.upper(), 
        args.partition_type.upper(), 
        args.date.upper(), 
        args.db_type.upper(), 
        args.column_name, 
        args.column_type
        )
        
def aws_initial_config(service_name: str, config_file: dict):
    """Define una sesion de aws para el consumo de sus servicios

    Args:
        service_name (str): El servicio de aws que se quiere utilizar (e.g s3, lanbda, ...)
        config_file (dict): Archivo de configuracion del driver

    Returns:
        client: el cliente de la sesion iniciada con un servicio en especifico
    """
    session = boto3.session.Session()
    client = session.client(
            service_name=service_name,
            aws_access_key_id=config_file['aws']['aws_access_key_id'],
            aws_secret_access_key=config_file['aws']['aws_secret_access_key'],
            verify=False
            )
    
    return client