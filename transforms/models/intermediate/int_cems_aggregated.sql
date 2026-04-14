with stg as (
    select * from {{ ref('stg_cems_daily') }}
),

aggregated as (
    select
        state,
        date,
        primary_fuel,
        count(distinct facility_id)     as facility_count,
        sum(gross_load_mw)              as total_gross_load_mw,
        sum(co2_mass_tons)              as total_co2_mass_tons,
        sum(so2_mass_lbs)               as total_so2_mass_lbs,
        sum(nox_mass_lbs)               as total_nox_mass_lbs,
        sum(heat_input_mmbtu)           as total_heat_input_mmbtu
    from stg
    group by state, date, primary_fuel
)

select * from aggregated