import sys
from etl.postal_codes import update_postal_codes


if __name__ == '__main__':
    '''
        Usage: python update_data.py <entity>
        Where <entity> is postal_codes, gar or kladr
    '''
    entity = sys.argv[1]

    if entity == 'postal_codes':
        update_postal_codes()
    else:
        raise ValueError(f'Unknown entity to update: {entity}')