with int_data as (
    select * from {{ ref('int_cems_aggregated') }}
),

-- Simplify fuel type labels
fuel_cleaned as (
    select
        date,
        state,
        case
            when lower(primary_fuel) like '%natural gas%' then 'Natural Gas'
            when lower(primary_fuel) like '%coal%'        then 'Coal'
            when lower(primary_fuel) like '%oil%'         then 'Oil'
            when lower(primary_fuel) like '%nuclear%'     then 'Nuclear'
            when lower(primary_fuel) like '%solar%'       then 'Solar'
            when lower(primary_fuel) like '%wind%'        then 'Wind'
            when lower(primary_fuel) like '%biomass%'     then 'Biomass'
            when lower(primary_fuel) like '%hydro%'       then 'Hydro'
            else 'Other'
        end                             as fuel_category,
        total_gross_load_mw,
        total_co2_mass_tons,
        total_so2_mass_lbs,
        total_nox_mass_lbs,
        total_heat_input_mmbtu,
        facility_count
    from int_data
),

final as (
    select
        date,
        state,
        fuel_category,
        sum(total_gross_load_mw)        as total_gross_load_mw,
        sum(total_co2_mass_tons)        as total_co2_mass_tons,
        sum(total_so2_mass_lbs)         as total_so2_mass_lbs,
        sum(total_nox_mass_lbs)         as total_nox_mass_lbs,
        sum(total_heat_input_mmbtu)     as total_heat_input_mmbtu,
        sum(facility_count)             as facility_count
    from fuel_cleaned
    group by date, state, fuel_category
)

select * from final