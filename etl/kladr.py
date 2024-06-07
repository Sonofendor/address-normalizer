import os, shutil
from dbfread import DBF
import pandas as pd
import py7zr
from etl import (
    connect_to_db, upload_df_to_db,
    q_create_raw_kladr_altnames_table, q_truncate_raw_kladr_altnames_table, q_copy_raw_kladr_altnames_table,
    q_create_raw_kladr_doma_table, q_truncate_raw_kladr_doma_table, q_copy_raw_kladr_doma_table,
    q_create_raw_kladr_flat_table, q_truncate_raw_kladr_flat_table, q_copy_raw_kladr_flat_table,
    q_create_raw_kladr_kladr_table, q_truncate_raw_kladr_kladr_table, q_copy_raw_kladr_kladr_table,
    q_create_raw_kladr_namemap_table, q_truncate_raw_kladr_namemap_table, q_copy_raw_kladr_namemap_table,
    q_create_raw_kladr_socrbase_table, q_truncate_raw_kladr_socrbase_table, q_copy_raw_kladr_socrbase_table,
    q_create_raw_kladr_street_table, q_truncate_raw_kladr_street_table, q_copy_raw_kladr_street_table,
    q_create_kladr_temp_query, q_create_kladr_table, q_insert_into_kladr_table, q_create_kladr_index,
    q_create_kladr_view
)

filenames = ['altnames', 'doma', 'flat', 'kladr', 'namemap', 'socrbase', 'street',]


def clear_kladr_folder():
    for filename in os.listdir('data/raw/kladr/'):
        file_path = os.path.join('data/raw/kladr/', filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def extract_kladr_from_archive():
    with py7zr.SevenZipFile('data/raw/kladr.7z', mode='r') as z:
        z.extractall('data/raw/kladr/')


def upload_file_to_db(filename):
    if filename == 'altnames':
        q_create_raw_table = q_create_raw_kladr_altnames_table
        q_truncate_raw_table = q_truncate_raw_kladr_altnames_table
        q_copy_raw_table = q_copy_raw_kladr_altnames_table
    elif filename == 'doma':
        q_create_raw_table = q_create_raw_kladr_doma_table
        q_truncate_raw_table = q_truncate_raw_kladr_doma_table
        q_copy_raw_table = q_copy_raw_kladr_doma_table
    elif filename == 'flat':
        q_create_raw_table = q_create_raw_kladr_flat_table
        q_truncate_raw_table = q_truncate_raw_kladr_flat_table
        q_copy_raw_table = q_copy_raw_kladr_flat_table
    elif filename == 'kladr':
        q_create_raw_table = q_create_raw_kladr_kladr_table
        q_truncate_raw_table = q_truncate_raw_kladr_kladr_table
        q_copy_raw_table = q_copy_raw_kladr_kladr_table
    elif filename == 'namemap':
        q_create_raw_table = q_create_raw_kladr_namemap_table
        q_truncate_raw_table = q_truncate_raw_kladr_namemap_table
        q_copy_raw_table = q_copy_raw_kladr_namemap_table
    elif filename == 'socrbase':
        q_create_raw_table = q_create_raw_kladr_socrbase_table
        q_truncate_raw_table = q_truncate_raw_kladr_socrbase_table
        q_copy_raw_table = q_copy_raw_kladr_socrbase_table
    elif filename == 'street':
        q_create_raw_table = q_create_raw_kladr_street_table
        q_truncate_raw_table = q_truncate_raw_kladr_street_table
        q_copy_raw_table = q_copy_raw_kladr_street_table
    else:
        raise ValueError(f'Unknown filename: {filename}')
    connection = connect_to_db()
    cursor = connection.cursor()
    try:
        df = pd.DataFrame(iter(DBF(f'data/raw/kladr/{filename.upper()}.DBF')))
        df.columns = map(lambda x: x.lower(), df.columns)
        cursor.execute(q_create_raw_table)
        cursor.execute(q_truncate_raw_table)
        upload_df_to_db(df, q_copy_raw_table, cursor)
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def upload_files_to_db():
    for filename in filenames:
        upload_file_to_db(filename)


def update_data_in_db():
    connection = connect_to_db()
    cursor = connection.cursor()
    try:
        cursor.execute(q_create_kladr_temp_query)
        cursor.execute(q_create_kladr_table)
        cursor.execute(q_insert_into_kladr_table)
        cursor.execute(q_create_kladr_index)
        cursor.execute(q_create_kladr_view)
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def update_kladr():
    clear_kladr_folder()
    extract_kladr_from_archive()
    upload_files_to_db()
    update_data_in_db()
