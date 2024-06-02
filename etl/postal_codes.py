from dbfread import DBF
import pandas as pd
from etl import (
    connect_to_db, q_create_raw_postal_codes, q_copy_raw_postal_codes, q_truncate_raw_postal_codes,
    q_create_postal_codes, q_truncate_postal_codes, q_insert_into_postal_codes, upload_df_to_db
)


def extract_and_upload_postal_codes_to_db():
    df = pd.DataFrame(iter(DBF('data/raw/postal_codes.dbf')))
    df.drop(['OPSNAME', 'OPSTYPE', 'OPSSUBM', 'ACTDATE', 'INDEXOLD'], axis=1, inplace=True)
    df.columns = map(lambda x: x.lower(), df.columns)
    connection = connect_to_db()
    cursor = connection.cursor()
    try:
        cursor.execute(q_create_raw_postal_codes)
        cursor.execute(q_truncate_raw_postal_codes)
        upload_df_to_db(df, q_copy_raw_postal_codes, cursor)
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def transform_postal_codes_in_db():
    connection = connect_to_db()
    cursor = connection.cursor()
    try:
        cursor.execute(q_create_postal_codes)
        cursor.execute(q_truncate_postal_codes)
        cursor.execute(q_insert_into_postal_codes)
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def update_postal_codes():
    extract_and_upload_postal_codes_to_db()
    transform_postal_codes_in_db()
