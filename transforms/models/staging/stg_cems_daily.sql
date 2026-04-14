with source as (
    select * from {{ source('raw', 'raw_cems_daily') }}
),

cleaned as (
    select
        state,
        facility_name,
        cast(facility_id as int64)      as facility_id,
        unit_id,
        cast(date as date)              as date,
        primary_fuel,
        unit_type,
        coalesce(gross_load_mw, 0)      as gross_load_mw,
        coalesce(so2_mass_lbs, 0)       as so2_mass_lbs,
        coalesce(co2_mass_tons, 0)      as co2_mass_tons,
        coalesce(nox_mass_lbs, 0)       as nox_mass_lbs,
        coalesce(heat_input_mmbtu, 0)   as heat_input_mmbtu,
        ingested_at

    from source
    where date is not null
      and state is not null
)

select * from cleaned