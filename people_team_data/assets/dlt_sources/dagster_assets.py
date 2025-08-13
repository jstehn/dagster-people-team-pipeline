from datetime import datetime, timedelta, timezone

from dagster import AssetExecutionContext, EnvVar
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from dlt.destinations import duckdb

from .bamboo_api_pipeline import bamboohr_source
from .paycom_pipeline import paycom_source
from .position_control_pipeline import position_control_source
from .vector_api_pipeline import vector_source


# Dynamically determine destination based on environment
def get_destination():
    raw_env = EnvVar("DAGSTER_ENV").get_value("production") or "production"
    env = raw_env.lower()
    if env in ["dev", "development", "local"]:
        duck_path = EnvVar("DUCKDB_PATH").get_value() or ":memory:"
        return duckdb(duck_path)
    return "bigquery"


DEST = get_destination()


@dlt_assets(
    dlt_source=bamboohr_source(),
    dlt_pipeline=pipeline(
        pipeline_name="bamboohr_pipeline",
        dataset_name="staff",
        destination=DEST,
        progress="log",
    ),
    name="raw_bamboohr",
    group_name="raw_people_data",
)
def dagster_bamboohr_assets(
    context: AssetExecutionContext, dlt_resource: DagsterDltResource
):
    """
    Loads BambooHR employee data into the staff schema using DLT.
    """
    yield from dlt_resource.run(context=context, write_disposition="merge")


@dlt_assets(
    dlt_source=paycom_source(),
    dlt_pipeline=pipeline(
        pipeline_name="paycom_pipeline",
        dataset_name="staff",
        destination=DEST,
        progress="log",
    ),
    name="raw_paycom",
    group_name="raw_people_data",
)
def dagster_paycom_assets(
    context: AssetExecutionContext, dlt_resource: DagsterDltResource
):
    """
    Loads Paycom employee data into the staff schema using DLT.
    """
    yield from dlt_resource.run(context=context, write_disposition="merge")


@dlt_assets(
    dlt_source=position_control_source(),
    dlt_pipeline=pipeline(
        pipeline_name="position_control_pipeline",
        dataset_name="staff",
        destination=DEST,
        progress="log",
    ),
    name="raw_position_control",
    group_name="raw_people_data",
)
def dagster_position_control_assets(
    context: AssetExecutionContext, dlt_resource: DagsterDltResource
):
    """
    Loads position control data into the staff schema using DLT.
    """
    yield from dlt_resource.run(context=context, write_disposition="merge")


@dlt_assets(
    dlt_source=vector_source(),
    dlt_pipeline=pipeline(
        pipeline_name="vector_api_pipeline",
        dataset_name="staff",
        destination=DEST,
        progress="log",
    ),
    name="raw_vector",
    group_name="raw_people_data",
)
def dagster_vector_compliance_assets(
    context: AssetExecutionContext, dlt_resource: DagsterDltResource
):
    """
    Loads Vector API compliance and progress data incrementally into the staff schema using DLT.
    Extraction window is determined from Dagster asset metadata for incremental loads.
    """

    def _extract_prior_end() -> datetime | None:
        """Get the previous window end from Dagster asset metadata or materialization timestamp."""
        try:
            records = list(
                context.instance.get_asset_records([context.asset_key])
            )
            record = records[0] if records else None
            if not record or not getattr(
                record.asset_entry, "last_materialization", None
            ):
                return None
            last_mat = record.asset_entry.last_materialization
            meta_dict = {}
            try:
                mat_obj = getattr(last_mat, "materialization", None) or getattr(
                    last_mat, "asset_materialization", None
                )
                raw_meta = getattr(mat_obj, "metadata", None)
                if isinstance(raw_meta, dict):
                    meta_dict = raw_meta
                elif raw_meta:
                    meta_dict = {
                        m.label: getattr(m, "value", None) for m in raw_meta
                    }
            except Exception:
                pass
            window_end_str = meta_dict.get("window_end") if meta_dict else None
            if window_end_str:
                try:
                    return datetime.fromisoformat(
                        window_end_str.replace("Z", "+00:00")
                    )
                except Exception:
                    pass
            ts = getattr(
                getattr(last_mat, "event_log_entry", last_mat),
                "timestamp",
                None,
            )
            if ts:
                return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None
        return None

    prior_end = _extract_prior_end()
    now = datetime.now(timezone.utc)
    one_year_ago = now - timedelta(days=365)
    begin_dt = (
        prior_end if (prior_end and prior_end > one_year_ago) else one_year_ago
    )
    end_dt = now + timedelta(days=1)

    begin_str = begin_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    base_source = vector_source()
    base_source.raw_vector_compliance.bind(
        begin_date=begin_str,
        end_date=end_str,
    )
    base_source.raw_vector_progress.bind(
        start_date=begin_str,
        end_date=end_str,
    )

    yield from dlt_resource.run(
        context=context,
        dlt_source=base_source,
        write_disposition="merge",
    )
