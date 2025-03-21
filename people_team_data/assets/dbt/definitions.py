from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import dbt_models_dbt_assets
from .project import dbt_models_project
from .schedules import schedules

# have to figure out how to include this in the definitions
defs = Definitions(
    assets=[dbt_models_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_models_project),
    },
)
