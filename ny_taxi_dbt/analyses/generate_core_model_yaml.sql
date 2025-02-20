{% set models_to_generate = ['dim_monthly_zone_revenue', 'dim_zones', 'fact_trips'] %}
{{ codegen.generate_model_yaml(model_names=models_to_generate) }}

-- dbt compile --select generate_core_model_yaml