import dlt
from dlt.sources.filesystem import filesystem, read_csv


# Group the resource under a source schema.
@dlt.source(name="paycom_source")
def paycom_source(
    bucket_url: str = dlt.config.value, file_glob: str = dlt.config.value
):
    @dlt.resource(
        name="raw_paycom",
        primary_key="Employee_Code",
    )
    def latest_csv():
        # Create the filesystem source using the provided bucket_url and file_glob.
        fs_source = filesystem(
            bucket_url=bucket_url,
            file_glob=file_glob
        )
        # Apply an incremental hint so only new or updated files are processed.
        fs_source.apply_hints(
            incremental=dlt.sources.incremental("modification_date")
        )
        filesystem_pipe = fs_source | read_csv()
        # Instead of calling a method like fs_source.read_csv(), use the pipe operator to chain the CSV transformer.
        yield from filesystem_pipe

    return latest_csv
