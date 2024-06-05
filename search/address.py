from parsing import preprocess, extract_index, extract_house, optimize_for_search
from etl import connect_to_db


class Address:

    original_address: str
    raw_postal_code: str
    raw_postal_code_valid: bool
    raw_address: str
    raw_house: str
    raw_building: str
    raw_building_type: str
    raw_flat: str
    standardized_string: str
    search_string: str
    normalized_address: str
    postal_address: str

    def __init__(self, raw_address):
        self.original_address = raw_address
        preprocessed_string = preprocess(self.original_address)
        raw_address, self.raw_postal_code = extract_index(preprocessed_string)
        try:
            self.raw_postal_code = self.raw_postal_code.strip()
        except AttributeError:
            pass
        self.raw_postal_code_valid, self.postal_address = self.search_in_postal_codes()
        self.raw_address, house = extract_house(raw_address)
        self.raw_house = house['house'] if 'house' in house else None
        self.raw_building = house['building'] if 'building' in house else None
        self.raw_flat = house['flat'] if 'flat' in house else None
        self.raw_building_type = house['building_type'] if 'building_type' in house else None
        self.standardized_string = ' '.join([str(x) for x in [
            optimize_for_search(self.raw_address).rstrip(),
            self.raw_house,
            self.raw_building_type,
            self.raw_building
        ] if x]).replace(' None', '')
        self.search_string = ' & '.join(self.standardized_string.split(' '))

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
                region_socr,
                region_socrname,
                area_kladr,
                area,
                area_socr,
                area_socrname,
                city_kladr,
                city,
                city_socr,
                city_socrname,
                settlement_kladr,
                settlement,
                settlement_socr,
                settlement_socrname,
                street_kladr,
                street,
                street_socr,
                street_socrname,
                house_kladr,
                house ,
                house_socr,
                house_socrname,
                postal_code,
                okato,
                levenshtein(address_representation, '{self.standardized_string}') as distance
            FROM
                public.v_kladr
            WHERE
                address_vector @@ websearch_to_tsquery('russian', '{self.standardized_string}')
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
                    if (address['distance'] - first_address['distance']) / first_address['distance'] <= 0.5
                ]
                if similar_addresses:
                    return 2, [first_address, *similar_addresses]
                else:
                    return 1, first_address
        else:
            return 0, None

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
