# POSTAL CODES
q_create_raw_postal_codes = '''
create table if not exists public.raw_postal_codes (
    index char(6),
    region text,
    autonom text,
    area text,
    city text,
    city_1 text
);
'''
q_truncate_raw_postal_codes = 'TRUNCATE TABLE public.raw_postal_codes'
q_copy_raw_postal_codes = "COPY public.raw_postal_codes FROM STDIN WITH CSV HEADER DELIMITER '\t'"
q_create_postal_codes = '''
CREATE TABLE if not exists public.postal_codes (
    postal_code char(6) primary key,
    country text,
    region text,
    region_type text,
    area text,
    city text,
    settlement text
);
'''
q_truncate_postal_codes = 'TRUNCATE TABLE public.postal_codes'
q_insert_into_postal_codes = """
insert into public.postal_codes
with
cleaned as (
    select 
        case 
            when region in ('ГЕРМАНИЯ', 'КАЗАХСТАН') then INITCAP(region)
            else 'Россия'
        end as country,
        index as postal_code,
        INITCAP(coalesce(region, autonom)) as region,
        INITCAP(area) as area,
        INITCAP(city) as city,
        INITCAP(city_1) as settlement
    from 
        public.raw_postal_codes
),
pre as (
    select 
        postal_code,
        country,
        region,
        case 
            when split_part(region, ' ', -1) = 'Округ' then 'Автономный округ'
            when region in ('Москва', 'Санкт-Петербург', 'Севастополь') then 'Город'
            else split_part(region, ' ', -1)
        end as region_type,
        area,
        case 
            when region in ('Москва', 'Санкт-Петербург', 'Севастополь') then region
            else city
        end as city,
        case 
            when region in ('Москва', 'Санкт-Петербург', 'Севастополь') then city
            else settlement
        end as settlement
    from 
        cleaned
)
select
    postal_code,
    country,
    trim(replace(region, region_type, '')) as region,
    region_type,
    case 
        when split_part(area, ' ', -1) not in ('Район', 'Улус') and region_type != 'Город' then null 
        else area
    end as area,
    case 
        when split_part(area, ' ', -1) not in ('Район', 'Улус') and region_type != 'Город' then area 
        else city
    end as city,
    case 
        when split_part(area, ' ', -1) not in ('Район', 'Улус') and region_type != 'Город' then city 
        else settlement
    end as settlement
from 
    pre
"""

# KLADR
q_create_raw_kladr_altnames_table = '''
create table if not exists public.raw_kladr_altnames (
    oldcode text,
    newcode text,
    level smallint
)
'''
q_truncate_raw_kladr_altnames_table = 'TRUNCATE TABLE public.raw_kladr_altnames'
q_copy_raw_kladr_altnames_table = "COPY public.raw_kladr_altnames FROM STDIN WITH CSV HEADER DELIMITER '\t'"

q_create_raw_kladr_doma_table = '''
create table if not exists public.raw_kladr_doma (
    name text,
    korp text,
    socr text,
    code char(19),
    index char(6),
    gninmb char(4),
    uno char(4),
    ocatid char(11)
)
'''
q_truncate_raw_kladr_doma_table = 'TRUNCATE TABLE public.raw_kladr_doma'
q_copy_raw_kladr_doma_table = "COPY public.raw_kladr_doma FROM STDIN WITH CSV HEADER DELIMITER '\t'"

q_create_raw_kladr_flat_table = '''
create table if not exists public.raw_kladr_flat (
    code char(19),
    np text,
    gninmb char(4),
    name text,
    index char(6),
    uno char(4)
)
'''
q_truncate_raw_kladr_flat_table = 'TRUNCATE TABLE public.raw_kladr_flat'
q_copy_raw_kladr_flat_table = "COPY public.raw_kladr_flat FROM STDIN WITH CSV HEADER DELIMITER '\t'"

q_create_raw_kladr_kladr_table = '''
create table if not exists public.raw_kladr_kladr (
    name text,
    socr text,
    code char(19),
    index char(6),
    gninmb char(4),
    uno char(4),
    ocatid char(11),
    status smallint
)
'''
q_truncate_raw_kladr_kladr_table = 'TRUNCATE TABLE public.raw_kladr_kladr'
q_copy_raw_kladr_kladr_table = "COPY public.raw_kladr_kladr FROM STDIN WITH CSV HEADER DELIMITER '\t'"

q_create_raw_kladr_namemap_table = '''
create table if not exists public.raw_kladr_namemap (
    code char(19),
    name text,
    shname text,
    scname text
)
'''
q_truncate_raw_kladr_namemap_table = 'TRUNCATE TABLE public.raw_kladr_namemap'
q_copy_raw_kladr_namemap_table = "COPY public.raw_kladr_namemap FROM STDIN WITH CSV HEADER DELIMITER '\t'"

q_create_raw_kladr_socrbase_table = '''
create table if not exists public.raw_kladr_socrbase (
    level smallint,
    scname text,
    socrname text,
    kod_t_st char(3)
)
'''
q_truncate_raw_kladr_socrbase_table = 'TRUNCATE TABLE public.raw_kladr_socrbase'
q_copy_raw_kladr_socrbase_table = "COPY public.raw_kladr_socrbase FROM STDIN WITH CSV HEADER DELIMITER '\t'"

q_create_raw_kladr_street_table = '''
create table if not exists public.raw_kladr_street (
    name text,
    socr text,
    code char(19),
    index char(6),
    gninmb char(4),
    uno char(4),
    ocatid char(11)
)
'''
q_truncate_raw_kladr_street_table = 'TRUNCATE TABLE public.raw_kladr_street'
q_copy_raw_kladr_street_table = "COPY public.raw_kladr_street FROM STDIN WITH CSV HEADER DELIMITER '\t'"

q_create_kladr_temp_query = """

drop table if exists public.temp_kladr;

create table public.temp_kladr as
with 
temp_kladr as (
    select 
        house.code as house_kladr,
        house."name" as house,
        trim(region.code) as region_kladr,
        region.socr as region_type,
        region.name as region,
        case when area_socrbase.socrname is null or area_socrbase.socrname = 'Город' then null else trim(area.code) end as area_kladr,
        case when area_socrbase.socrname is null or area_socrbase.socrname = 'Город' then null else area.name end as area,
        case when area_socrbase.socrname is null or area_socrbase.socrname = 'Город' then null else area.socr end as area_type,
        trim(coalesce(city.code, city2.code)) as city_kladr,
        trim(coalesce(city.socr, city2.socr)) as city_type,
        trim(coalesce(city.name, city2.name)) as city,
        trim(settlement.code) as settlement_kladr,
        settlement.socr as settlement_type,
        settlement.name as settlement,
        trim(street.code) as street_kladr,
        street.socr as street_type,
        street.name as street,
        house.index as postal_code,
        house.ocatid as okato
    from 
        public.raw_kladr_doma house
        left join public.raw_kladr_street street 
            on left(house.code, 17) = trim(street.code)
        left join public.raw_kladr_socrbase street_socrbase
            on street.socr = street_socrbase.scname
            and street_socrbase.level = 5
        left join public.raw_kladr_kladr settlement
            on left(house.code, 13) = trim(settlement.code)
            AND settlement.socr not IN ('г', 'г.')
        left join public.raw_kladr_socrbase settlement_socrbase
            on settlement.socr = settlement_socrbase.scname
            and settlement_socrbase.level = 4
        left join public.raw_kladr_kladr city
            on left(house.code, 13) = trim(city.code)
            AND city.socr IN ('г', 'г.')
        left join public.raw_kladr_kladr city2
            on left(house.code, 8) = left(city2.code, 8)
            and right(trim(city2.code), 5) = '00000'
        left join public.raw_kladr_socrbase city_socrbase
            on (
                city.socr = city_socrbase.scname
                or city2.socr = city_socrbase.scname
            )
            and city_socrbase.level = 3
        left join public.raw_kladr_kladr area
            on left(house.code, 5) = left(area.code, 5)
            and right(trim(area.code), 8) = '00000000'
        left join public.raw_kladr_socrbase area_socrbase
            on area.socr = area_socrbase.scname 
            and area_socrbase.level = 2
        left join public.raw_kladr_kladr region
            on left(house.code, 2) = left(region.code, 2)
            and right(trim(region.code), 11) = '00000000000'
        left join public.raw_kladr_socrbase region_socrbase
            on region.socr = region_socrbase.scname
            and region_socrbase.level = 1
),
pre as (
    select 
        house_kladr,
        unnest(string_to_array(house, ',')) as house,
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
        postal_code,
        okato
    from
        temp_kladr
)
select distinct
    house_kladr,
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
    case 
        when house like '%вл%' and house not like '%двлд%' then regexp_replace(trim(regexp_replace(replace(house, 'влд', ''), '[\d]+|[\W]+', ' \& ', 'g') ), ' +', ' ', 'g')
        when house like '%двлд%' then regexp_replace(trim(regexp_replace(replace(house, 'двлд', ''), '[\d]+|[\W]+', ' \& ', 'g') ), ' +', ' ', 'g')
        else regexp_replace(trim(regexp_replace(house, '[\d]+|[\W]+', ' \& ', 'g') ), ' +', ' ', 'g')
    end as house,
    case 
        when house like '%вл%' and house not like '%двлд%' then 'влд'
        when house like '%двлд%' then 'двлд'
        else 'д'
    end as house_type,
    postal_code,
    okato
from 
    pre
"""

q_create_kladr_table = """
drop TABLE if exists public.kladr cascade;

CREATE TABLE if not exists public.kladr (
    region_kladr bpchar(13),
    region text,
    region_type text,
    area_kladr bpchar(13),
    area text,
    area_type text,
    city_kladr bpchar(13),
    city text,
    city_type text,
    settlement_kladr bpchar(13),
    settlement text,
    settlement_type text,
    street_kladr bpchar(17),
    street text,
    street_type text,
    house_kladr bpchar(19),
    house text,
    house_type text,
    postal_code bpchar(6),
    okato bpchar(11),
    address_vector tsvector,
    primary key (house_kladr, house_type, house)
);
"""

q_insert_into_kladr_table = """
insert into public.kladr
select 
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
    to_tsvector(
        'russian', 
        concat_ws(
            ' ', region, region_type, area, area_type, city, city_type, settlement, settlement_type, street, street_type, house_type, house
        )
    ) as address_vector
from 
    public.temp_kladr
"""

q_create_kladr_index = 'CREATE INDEX address_ts_idx ON public.kladr USING GIN (address_vector)'

q_create_kladr_view = """
create or replace view public.v_kladr as 
select 
    *,
    lower(
        concat_ws(
            ' ', region, area, city, settlement, street, house
        )
    ) as address_representation
from 
    public.kladr 
;
"""


