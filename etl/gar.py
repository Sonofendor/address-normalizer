import zipfile
import pandas as pd
import fnmatch
import xml.etree.cElementTree as ET
import os
from etl import connect_to_db, upload_df_to_db
from etl.sql.postgresql import (
    q_copy_gar_table, q_create_gar_table, q_truncate_gar_table
)


def update_raw_gar_tables():
    with zipfile.ZipFile('data/raw/gar.zip', 'r') as archive:
        connection = connect_to_db()
        cursor = connection.cursor()
        tables = set()
        try:
            for i, file_info in enumerate(archive.infolist()):
                if fnmatch.fnmatch(file_info.filename, '*.XML') and ('ADDR_OBJ' in file_info.filename):
                    print(file_info.filename)
                    table = file_info.filename[6:-50] if '/' in file_info.filename else file_info.filename[3:-50]
                    region = file_info.filename[:2] if '/' in file_info.filename else None
                    filepath = f'data/raw/gar/{file_info.filename}'
                    archive.extract(file_info.filename, 'data/raw/gar')
                    with open(filepath, 'rt', encoding='utf-8-sig') as f:
                        root = ET.iterparse(f)
                        df = pd.DataFrame([elem.attrib for _, elem in root])
                        print(len(df))
                    if len(df) > 1:
                        cursor.execute(q_create_gar_table.format(table=table, fields=' TEXT, '.join([column if column != 'DESC' else 'DESC_' for column in df.columns])))
                        if table not in tables:
                            cursor.execute(q_truncate_gar_table.format(table=table))
                            tables.add(table)
                        upload_df_to_db(df, q_copy_gar_table.format(table=table, fields=','.join([column if column != 'DESC' else 'DESC_' for column in df.columns])), cursor)
                    connection.commit()
                    os.remove(filepath)
                    print(f"Extracted: {region}, {table}. Done {round(100.0 * i/len(archive.infolist()), 2)}")
        finally:
            cursor.close()
            connection.close()
