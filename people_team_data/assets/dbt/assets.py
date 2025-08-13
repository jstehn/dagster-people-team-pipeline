"""
This module defines the DBT assets for the Dagster pipeline.

It includes:
- A function to execute DBT models using the Dagster DBT integration.
"""

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def dbt_models_dbt_assets(
    context: AssetExecutionContext, dbt_resource: DbtCliResource
):
    """
    Execute DBT models using the Dagster DBT integration.

    Args:
        context (AssetExecutionContext): The execution context provided by Dagster.
        dbt_resource (DbtCliResource): The DBT CLI resource for executing DBT commands.

    Yields:
        Iterator: The output stream of the DBT CLI command.
    """
    yield from dbt_resource.cli(["build"], context=context).stream()
