from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def dbt_models_dbt_assets(
    context: AssetExecutionContext, dbt_resource: DbtCliResource
):
    yield from dbt_resource.cli(["build"], context=context).stream()
