import sys
from etl.postal_codes import update_postal_codes
from etl.kladr import update_kladr
from etl.gar import update_raw_gar_tables


if __name__ == '__main__':
    '''
        Usage: python update_data.py <entity>
        Where <entity> is postal_codes, gar or kladr
    '''
    entity = sys.argv[1]

    if entity == 'postal_codes':
        update_postal_codes()
    elif entity == 'kladr':
        update_kladr()
    elif entity == 'gar':
        update_raw_gar_tables()
    else:
        raise ValueError(f'Unknown entity to update: {entity}')