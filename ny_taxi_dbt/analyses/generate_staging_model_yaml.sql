{% set models_to_generate = ['stg_green_taxi', 'stg_yellow_taxi' ] %}
{{ codegen.generate_model_yaml(model_names=models_to_generate) }}

-- to call: dbt compile --select generate_staging_model_yaml
