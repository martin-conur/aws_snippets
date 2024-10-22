import boto3
from urllib.parse import quote_plus
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.engine.url import URL
import redshift_connector

rs_connection = None
ssm = boto3.client('ssm', region_name='us-east-1')


def _load_cnx_params(username: str, user_role: str):
    """
    El usuario y contrasena
    se obtienen de Parameter Store
    :return: Diccionario con parametros de conexion
    """
    cnx_params = {
        "username":ssm.get_parameter(Name=f"/sagemaker/redshift/{username}/user_{user_role}")['Parameter']['Value'],
        "password":ssm.get_parameter(Name=f"/sagemaker/redshift/{username}/pass_{user_role}", WithDecryption=True)['Parameter']['Value']
                                      }

    if all(cnx_params.values()):
        return cnx_params
    else:
        raise Exception("Parametros de conexion a Redshift faltantes!")


def get_engine(username: str, user_role: str, return_uri=False):
    """
    Returns the redshift engine for pandas queries
    """
    # getting credentials
    username_parsed, password = _load_cnx_params(username, user_role).values()

    uri = f'postgresql://{username_parsed}:{quote_plus(password)}@redshift-privado-prod.c0iwf7ndlvaw.us-east-1.redshift.amazonaws.com:5439/prod'

    if return_uri: 
        return uri
        
    # Creating the engine
    eng = create_engine(uri)
    
    return eng

def get_redshift_engine(username: str, user_role: str, return_uri=False):

    username_parsed, password = _load_cnx_params(username, user_role).values()
    # build the sqlalchemy URL
    conn = redshift_connector.connect(
    host='redshift-privado-prod.c0iwf7ndlvaw.us-east-1.redshift.amazonaws.com', # Amazon Redshift host
    port=5439, # Amazon Redshift port
    database='prod', # Amazon Redshift database
    user=username_parsed, # Amazon Redshift username
    password=password # Amazon Redshift password
    )
    
    return conn