import dlt
from dlt.sources.filesystem import filesystem, read_csv


# Group the resource under a source schema.
@dlt.source(name="paycom_source")
def paycom_source(
    bucket_url: str = dlt.config.value, file_glob: str = dlt.config.value
):
    @dlt.resource(
        name="paycom_data",
        primary_key="Employee_Code",
        write_disposition="merge",
    )
    def latest_csv():
        # Create the filesystem source using the provided bucket_url and file_glob.
        fs_source = filesystem(bucket_url=bucket_url, file_glob=file_glob)
        # Apply an incremental hint so only new or updated files are processed.
        fs_source.apply_hints(
            incremental=dlt.sources.incremental("modification_date")
        )
        filesystem_pipe = fs_source | read_csv()
        # Instead of calling a method like fs_source.read_csv(), use the pipe operator to chain the CSV transformer.
        yield from filesystem_pipe

    return latest_csv


def run_pipeline():
    # Create a pipeline that writes to PostgreSQL.
    # Make sure your .dlt/secrets.toml is correctly configured with your PostgreSQL connection details.
    pipeline = dlt.pipeline(
        pipeline_name="paycom_pipeline",
        destination="postgres",
        dataset_name="paycom_data",
    )
    # Run the pipeline; the loader_file_format can be specified if needed.
    load_info = pipeline.run(paycom_source())
    print(load_info)


if __name__ == "__main__":
    run_pipeline()
