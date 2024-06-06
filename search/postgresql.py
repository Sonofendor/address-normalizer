
q_search_kladr = """
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
    levenshtein(address_representation, '{address_representation}', 1, 3, 2) as distance
FROM
    public.v_kladr
WHERE
    address_vector @@ websearch_to_tsquery('russian', '{search_string}')
    {postal_code_filter}
ORDER BY
    distance
"""