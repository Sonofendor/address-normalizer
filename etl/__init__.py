import os
from dotenv import load_dotenv
from psycopg2 import connect as pg_connect
from io import StringIO

load_dotenv()

DB_TYPE = os.getenv('DB_TYPE')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')


def connect_to_postgresql():
    return pg_connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS, )


def upload_df_to_postgresql(df, copy_query, cursor):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, sep='\t', quotechar='"', doublequote=True)
    csv_buffer.seek(0)
    cursor.copy_expert(copy_query, csv_buffer)


if DB_TYPE == 'postgresql':
    from etl.sql.postgresql import *
    connect_to_db = connect_to_postgresql
    upload_df_to_db = upload_df_to_postgresql
else:
    raise ValueError('Variable for DB_TYPE is either not set or contains inappropriate value')
