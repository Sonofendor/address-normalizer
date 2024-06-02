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
