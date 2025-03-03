import logging

import dlt
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


@dlt.source
def bamboohr_source(
    api_key: str = dlt.secrets.value,
    company_domain: str = dlt.secrets.value,
    fields: list = dlt.secrets.value,
):
    """
    DLT source for BambooHR API.

    Args:
        api_key (str): API key for BambooHR.
        company_domain (str): Company domain for BambooHR.
        fields (list): List of fields to retrieve from the API.

    Returns:
        dlt.Source: DLT source object.
    """
    logging.debug(
        "Initializing BambooHR source with company domain: %s", company_domain
    )

    config: RESTAPIConfig = {
        "client": {
            "base_url": f"https://api.bamboohr.com/api/gateway.php/{company_domain}/v1/",
            "auth": HttpBasicAuth(api_key, "x"),
        },
        "resource_defaults": {
            "primary_key": "employeeNumber",
            "write_disposition": "merge",
        },
        "resources": [
            {
                "name": "raw_employee_data",
                "endpoint": {
                    "path": "datasets/employee",
                    "method": "POST",
                    "json": {
                        "fields": fields,
                    },
                },
            },
        ],
    }

    # Create the raw data resource
    raw_source = rest_api_source(config)

    @dlt.resource(name="bamboohr_data_raw", primary_key="employeeNumber")
    def employee_data(raw_employee_data):
        """
        Process raw employee data to normalize employee numbers.

        Args:
            raw_employee_data (iterable): Raw employee data from the API.

        Yields:
            dict: Processed employee data.
        """
        seen_employee_numbers = set()
        for record in raw_employee_data:
            employee_number = int(record.get("employeeNumber") or -1)
            if (
                employee_number >= 0
                and employee_number not in seen_employee_numbers
            ):
                seen_employee_numbers.add(employee_number)
                record["employeeNumber"] = employee_number
                yield record

    # Retrieve the raw data resource
    raw_data = raw_source.resources["raw_employee_data"]

    # Return the processed data resource
    return employee_data(raw_data)


def load_bamboohr() -> None:
    """Load data from BambooHR API into the destination for dlt."""
    logging.info("Starting BambooHR data load pipeline.")

    pipeline = dlt.pipeline(
        pipeline_name="rest_api_bamboohr",
        destination="postgres",
        dataset_name="bamboohr_employee_data",
    )

    load_info = pipeline.run(bamboohr_source(), loader_file_format="csv")
    logging.info("Data load completed. Load info: %s", load_info)
    print(load_info)


if __name__ == "__main__":
    load_bamboohr()
