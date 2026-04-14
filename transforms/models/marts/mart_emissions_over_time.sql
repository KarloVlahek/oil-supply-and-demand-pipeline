with int_data as (
    select * from {{ ref('int_cems_aggregated') }}
),

final as (
    select
        date,
        state,
        sum(total_gross_load_mw)        as total_gross_load_mw,
        sum(total_co2_mass_tons)        as total_co2_mass_tons,
        sum(total_so2_mass_lbs)         as total_so2_mass_lbs,
        sum(total_nox_mass_lbs)         as total_nox_mass_lbs,
        sum(total_heat_input_mmbtu)     as total_heat_input_mmbtu,
        sum(facility_count)             as facility_count
    from int_data
    group by date, state
)

select * from final