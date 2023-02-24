import psycopg2 
import boto3
import sys
sys.path.append("../src")
import urllib.parse

from sqlalchemy import create_engine

rs_connection = None
ssm = boto3.client('ssm', region_name='us-east-1')


def load_cnx_params():
    """
    El usuario y contrasena
    se obtienen de Parameter Store
    :return: Diccionario con parametros de conexion
    """
    cnx_params = {
        "username":ssm.get_parameter(Name="/sagemaker/redshift/mcontreras/user_ds_1")['Parameter']['Value'],
        "password":ssm.get_parameter(Name="/sagemaker/redshift/mcontreras/pass_ds_1", WithDecryption=True)['Parameter']['Value']
                                      }
    if all(cnx_params.values()):
        return cnx_params
    else:
        raise Exception("Parametros de conexion a Redshift faltantes!")


def get_engine():
    """Returns the redshift engine for pandas queries"""
    # getting credentials
    username, password = load_cnx_params().values()
    #password = urllib.parse.quote_plus(password)
    host = "redshift-privado-prod.c0iwf7ndlvaw.us-east-1.redshift.amazonaws.com"
    dbname = "prod"
    port=5439
    
    # Creating the engine
    connection = f'postgres://{username}:{password}@redshift-privado-prod.c0iwf7ndlvaw.us-east-1.redshift.amazonaws.com:5439/prod'
    return create_engine(connection)