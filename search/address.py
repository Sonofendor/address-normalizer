from parsing import process_address
from etl import connect_to_db


class Address:

    original_address: str
    raw_postal_code: str
    raw_postal_code_valid: bool
    raw_address: str
    raw_flat: str
    search_string: str
    normalized_address: str
    postal_address: str

    def __init__(self, raw_address):
        self.original_address = raw_address
        processed_address = process_address(self.original_address)
        self.raw_address = processed_address['raw_address']
        self.search_string = processed_address['search_string']
        self.raw_flat = processed_address['flat']
        self.raw_postal_code = processed_address['postal_code']
        try:
            self.raw_postal_code = self.raw_postal_code.strip()
        except AttributeError:
            pass
        self.raw_postal_code_valid, self.postal_address = self.search_in_postal_codes()


    def search_in_postal_codes(self, connection=None):
        if connection is None:
            connection = connect_to_db()
            close_connection = True
        else:
            close_connection = False
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT 
                concat_ws(' ', region, area, city, settlement) as address_representation
            FROM 
                public.postal_codes 
            WHERE 
                postal_code = '{self.raw_postal_code}'
        """)
        record = cursor.fetchone()
        cursor.close()
        if close_connection:
            connection.close()
        if record:
            return True, record[0]
        else:
            return False, None

    def search_in_kladr(self, connection=None):
        if connection is None:
            connection = connect_to_db()
            close_connection = True
        else:
            close_connection = False
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT
                region_kladr,
                region,
                region_type,
                area_kladr,
                area,
                area_type,
                city_kladr,
                city,
                city_type,
                settlement_kladr,
                settlement,
                settlement_type,
                street_kladr,
                street,
                street_type,
                house_kladr,
                house ,
                house_type,
                postal_code,
                okato,
                levenshtein(address_representation, '{self.raw_address}') as distance
            FROM
                public.v_kladr
            WHERE
                address_vector @@ websearch_to_tsquery('russian', '{self.search_string}')
                {f"AND postal_code = '{self.raw_postal_code}'" if self.raw_postal_code_valid else ''}
            ORDER BY
                distance
        """)
        columns = cursor.description
        found_addresses = [{columns[index][0]:column for index, column in enumerate(record)} for record in cursor.fetchall()]
        cursor.close()
        if close_connection:
            connection.close()
        if len(found_addresses) > 0:
            first_address = found_addresses[0]
            if first_address['distance'] < 2 or len(found_addresses) == 1:
                return 1, first_address
            else:
                similar_addresses = [
                    address for address in found_addresses[1:]
                    if (address['distance'] - first_address['distance']) / first_address['distance'] <= 0.2
                ]
                if similar_addresses:
                    return 2, [first_address, *similar_addresses]
                else:
                    return 1, first_address
        else:
            return 0, None
