from search.parsing import process_address
from search.postgresql import q_search_kladr
from etl import connect_to_db
from typing import Tuple, Dict, List
from hashlib import md5


class Address:

    original_address: str

    raw_postal_code: str
    raw_postal_code_valid: bool
    raw_address: str
    raw_flat: str

    search_string: str
    normalized_address: str
    postal_code: str

    postal_address: str

    kladr_status: int
    kladr_suggestions: list
    kladr_unique_key: str
    kladr_region_id: str
    kladr_region_type: str
    kladr_region: str
    kladr_area_id: str
    kladr_area_type: str
    kladr_area: str
    kladr_city_id: str
    kladr_city_type: str
    kladr_city: str
    kladr_settlement_id: str
    kladr_settlement_type: str
    kladr_settlement: str
    kladr_street_id: str
    kladr_street_type: str
    kladr_street: str
    kladr_house_id: str
    kladr_house_type: str
    kladr_house: str

    def __init__(self, raw_address, connection=None):
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
        self.raw_postal_code_valid, self.postal_address = self.search_in_postal_codes(connection)

    def to_dict(self):
        return {
            'original_address': self.original_address,
            'normalized_address': self.normalized_address,
            'processed': {
                'postal_code': self.raw_postal_code,
                'postal_code_valid': self.raw_postal_code_valid,
                'address': self.raw_address,
                'flat': self.raw_flat,
            },
            'search_summary': {
                'kladr_status': self.kladr_status,
            },
            'kladr': {
                'unique_key': self.kladr_unique_key,
                'region_id': self.kladr_region_id,
                'region_type': self.kladr_region_type,
                'region': self.kladr_region,
                'area_id': self.kladr_area_id,
                'area_type': self.kladr_area_type,
                'area': self.kladr_area,
                'city_id': self.kladr_city_id,
                'city_type': self.kladr_city_type,
                'city': self.kladr_city,
                'settlement_id': self.kladr_settlement_id,
                'settlement_type': self.kladr_settlement_type,
                'settlement': self.kladr_settlement,
                'street_id': self.kladr_street_id,
                'street_type': self.kladr_street_type,
                'street': self.kladr_street,
                'house_id': self.kladr_house_id,
                'house_type': self.kladr_house_type,
                'house': self.kladr_house,
            },
            'gar': {
                # empty for now
            }
        }

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

    def set_kladr_attributes_to_none(self):
        self.kladr_region_id = self.kladr_region_type = self.kladr_region = self.kladr_area_id = None
        self.kladr_area_type = self.kladr_area = self.kladr_city_id = self.kladr_city_type = self.kladr_city = None
        self.kladr_settlement_id = self.kladr_settlement_type = self.kladr_settlement = self.kladr_street_id = None
        self.kladr_street_type = self.kladr_street = self.kladr_house_id = self.kladr_house_type = self.kladr_house = None
        self.normalized_address = self.postal_code = self.kladr_unique_key = None

    def search_in_kladr(self, use_postal_code=True, connection=None) -> Tuple[int, List[Dict]]:
        if connection is None:
            connection = connect_to_db()
            close_connection = True
        else:
            close_connection = False
        cursor = connection.cursor()
        cursor.execute(
            q_search_kladr.format(
                search_string=self.search_string,
                address_representation=self.raw_address,
                postal_code_filter=f"AND postal_code = '{self.raw_postal_code}'" if use_postal_code else ''
            )
        )
        columns = cursor.description
        found_addresses = [{columns[index][0]: column for index, column in enumerate(record)} for record in cursor.fetchall()]
        cursor.close()
        if close_connection:
            connection.close()
        if len(found_addresses) > 0:
            first_address = found_addresses[0]
            if first_address['distance'] < 2 or len(found_addresses) == 1:
                return 1, [first_address]
            else:
                similar_addresses = [
                    address for address in found_addresses[1:]
                    if (address['distance'] - first_address['distance']) / first_address['distance'] <= 0.2
                ]
                if similar_addresses:
                    return 2, [first_address, *similar_addresses]
                else:
                    return 1, [first_address]
        else:
            return 0, []

    def standardize(self, connection=None):
        if connection is None:
            connection = connect_to_db()
            close_connection = True
        else:
            close_connection = False
        kladr_search_result, kladr_found = self.search_in_kladr(
            connection=connection,
            use_postal_code=self.raw_postal_code_valid
        )
        if kladr_search_result == 0 and self.raw_postal_code_valid:
            # maybe raw postal code is valid but to a different address
            kladr_search_result, kladr_found = self.search_in_kladr(
                connection=connection,
                use_postal_code=False
            )
        if close_connection:
            connection.close()
        if kladr_search_result == 0:
            self.kladr_suggestions = []
            self.set_kladr_attributes_to_none()
        elif kladr_search_result == 1:
            self.kladr_suggestions = kladr_found
            self.kladr_region_id = kladr_found[0]['region_kladr']
            self.kladr_region_type = kladr_found[0]['region_type']
            self.kladr_region = kladr_found[0]['region']
            self.kladr_area_id = kladr_found[0]['area_kladr']
            self.kladr_area_type = kladr_found[0]['area_type']
            self.kladr_area = kladr_found[0]['area']
            self.kladr_city_id = kladr_found[0]['city_kladr']
            self.kladr_city_type = kladr_found[0]['city_type']
            self.kladr_city = kladr_found[0]['city']
            self.kladr_settlement_id = kladr_found[0]['settlement_kladr']
            self.kladr_settlement_type = kladr_found[0]['settlement_type']
            self.kladr_settlement = kladr_found[0]['settlement']
            self.kladr_street_id = kladr_found[0]['street_kladr']
            self.kladr_street_type = kladr_found[0]['street_type']
            self.kladr_street = kladr_found[0]['street']
            self.kladr_house_id = kladr_found[0]['house_kladr']
            self.kladr_house_type = kladr_found[0]['house_type']
            self.kladr_house = kladr_found[0]['house']
            self.postal_code = kladr_found[0]['postal_code']
            self.kladr_unique_key = md5(f'{self.kladr_house_id}^^{self.kladr_house_type}^^{self.kladr_house}'.encode()).hexdigest()
            self.normalized_address = (
                f"{self.postal_code}, "
                f"{f'{self.kladr_region} {self.kladr_region_type}, ' if self.kladr_region else ''}"
                f"{f'{self.kladr_area} {self.kladr_area_type}, ' if self.kladr_area else ''}"
                f"{f'{self.kladr_city} {self.kladr_city_type}, ' if self.kladr_city else ''}"
                f"{f'{self.kladr_settlement} {self.kladr_settlement_type}, ' if self.kladr_settlement else ''}"
                f"{f'{self.kladr_street} {self.kladr_street_type}, ' if self.kladr_street else ''}"
                f"{f'{self.kladr_house_type} {self.kladr_house}, ' if self.kladr_house else ''}"
                f"{f'кв {self.raw_flat}' if self.raw_flat else ''}"
            ).strip(', ')
        else:
            self.kladr_suggestions = kladr_found
            self.set_kladr_attributes_to_none()
        self.kladr_status = kladr_search_result
