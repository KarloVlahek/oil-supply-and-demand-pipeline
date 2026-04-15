# Looker Studio Dashboard

## Access
The dashboard is publicly accessible at:
[INSERT LOOKER STUDIO LINK HERE]

## Tiles

### Tile 1 — U.S. Daily Electricity Generation Over Time
- **Chart type:** Time series
- **Data source:** `mart_emissions_over_time`
- **Dimension:** date
- **Metric:** Total Electricity Generation (MW)
- **Description:** Shows daily electricity generation across all 50 U.S. states from 2016 to present

### Tile 2 — U.S. Power Generation Mix by Fuel Type
- **Chart type:** Stacked bar chart
- **Data source:** `mart_emissions_by_fuel`
- **Dimension:** date (grouped by year)
- **Breakdown:** Fuel Type
- **Metric:** Total Electricity Generation (MW)
- **Description:** Shows how the U.S. power generation mix has shifted across fuel types from 2016 to present, illustrating the decline of coal and growth of natural gas