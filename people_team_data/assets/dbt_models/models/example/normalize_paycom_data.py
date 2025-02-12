import pandas as pd
from dbt.adapters.base import BaseRelation
from dbt.adapters.factory import get_adapter
from dbt.context.providers import get_model_context


def normalize_paycom_data(context):
    # Get the adapter and context
    adapter = get_adapter(context)
    model_context = get_model_context(context)

    # Load the raw data from the source table
    raw_data_relation = adapter.Relation.create(
        database="calibrate", schema="paycom", identifier="paycom_data_raw"
    )
    raw_data_df = adapter.get_dataframe(model_context, raw_data_relation)

    # Normalize the data
    raw_data_df["first_name"] = (
        raw_data_df["legal_firstname"].str.lower().str.strip()
    )
    raw_data_df["last_name"] = (
        raw_data_df["legal_lastname"].str.lower().str.strip()
    )
    raw_data_df["email"] = raw_data_df["work_email"].str.lower().str.strip()
    raw_data_df["salary"] = raw_data_df["salary"].astype(float)
    raw_data_df["gender"] = (
        raw_data_df["gender"].map({"M": "Male", "F": "Female"}).fillna("Other")
    )
    raw_data_df["hire_date"] = pd.to_datetime(raw_data_df["hire_date"])
    raw_data_df["rehire_date"] = pd.to_datetime(raw_data_df["rehire_date"])
    raw_data_df["termination_date"] = pd.to_datetime(
        raw_data_df["termination_date"]
    )
    raw_data_df["department"] = (
        raw_data_df["department"].str.lower().str.strip()
    )
    raw_data_df["sub_department"] = (
        raw_data_df["sub_department_desc"].str.lower().str.strip()
    )
    raw_data_df["division"] = (
        raw_data_df["division_desc"].str.lower().str.strip()
    )
    raw_data_df["work_location"] = (
        raw_data_df["work_location"].str.lower().str.strip()
    )
    raw_data_df["formatted_phone"] = (
        raw_data_df["primary_phone"]
        .astype(str)
        .str.replace(r"(\d{3})(\d{3})(\d{4})", r"(\1) \2-\3", regex=True)
    )

    # Select the relevant columns
    normalized_data_df = raw_data_df[
        [
            "employee_code",
            "first_name",
            "last_name",
            "email",
            "salary",
            "gender",
            "hire_date",
            "rehire_date",
            "termination_date",
            "department",
            "sub_department",
            "division",
            "work_location",
            "formatted_phone",
        ]
    ]

    # Return the normalized data
    return normalized_data_df


# Register the model
def model(dbt, session):
    context = dbt.context
    return normalize_paycom_data(context)
