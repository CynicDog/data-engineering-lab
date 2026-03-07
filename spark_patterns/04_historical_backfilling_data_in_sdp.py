from pyspark import pipelines as dp

source_root_path = spark.conf.get("registration_events_source_root_path")
begin_year = spark.confg.get("begin_year")
backfill_years = spark.conf.get("backfill_years")
incremental_load_path = f"{source_root_path}/*/*/*"

def setup_backfill_flow(year):
    backfill_path = f"{source_root_path}/year={year}/*/*"
    @dp.append_flow(
        target="registration_events_raw",
        once=True,
        name=f"flow_registration_events_raw_backfill_{year}",
        comment="Backfill {year} Raw Registration Events",
    )
    def backfill():
        return (
            spark.read
            .format("json")
            .option("inferSchema", "true")
            .load(backfill_path)
        )

dp.create_streaming_table(
    name="registration_events_raw",
    comment="Raw Registration Events",
)

@dp.append_flow(
    target="registration_events_raw",
    name="flow_registration_events_raw_incremental",
    comment="Incremental Registration Events",
)
def ingest():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("modifiedAfter","2024-12-31T23:59:59.999+00:00")
        .load(incremental_load_path)
        .where(f"year(timestamp)>={begin_year})")
    )

for year in backfill_years.split(","):
    setup_backfill_flow(year)