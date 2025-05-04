import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd

import duckdb
import os

from dagster_essentials.assets import constants
from dagster._utils.backoff import backoff

@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats() -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips
        from trips
        left join zones on zones.zone_id = trips.pickup_zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """
    with backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={"database": os.getenv("DUCKDB_DATABASE")},
        max_retries=10,
    ) as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@dg.asset(
    deps=["manhattan_stats"]
)
def manhattan_map():
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10,10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # Adjust longitude range
    ax.set_ylim([40.70, 40.82])  # Adjust latitude range

    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)

@dg.asset(
    deps=["taxi_trips"]
)
def trips_per_week() -> None:
    """
    Origin schema (trips):        
        VendorID as vendor_id,
        PULocationID as pickup_zone_id,
        DOLocationID as dropoff_zone_id,
        RatecodeID as rate_code_id,
        payment_type as payment_type,
        tpep_dropoff_datetime as dropoff_datetime,
        tpep_pickup_datetime as pickup_datetime,
        trip_distance as trip_distance,
        passenger_count as passenger_count,
        total_amount as total_amount

    Target trips_per_week:
        period - a string representing the Sunday of the week aggregated by, ex. 2023-03-05
        num_trips - The total number of trips that started in that week
        passenger_count - The total number of passengers that were on a taxi trip that week
        total_amount - The total sum of the revenue produced by trips that week
        trip_distance - The total miles driven in all trips that happened that week
    """
    query = f"""
        WITH trips_with_period AS (
            SELECT 
                (pickup_datetime - ((EXTRACT(DOW FROM pickup_datetime)) || ' days')::INTERVAL)::DATE AS period,
                passenger_count,
                total_amount,
                trip_distance
            FROM trips
        )
        SELECT
            period,
            COUNT(*) AS num_trips,
            SUM(passenger_count) AS passenger_count,
            SUM(total_amount) AS total_amount,
            SUM(trip_distance) AS trip_distance
        FROM trips_with_period
        GROUP BY period
    """
    with backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={"database": os.getenv("DUCKDB_DATABASE")},
        max_retries=10,
    ) as conn:
        trips_by_week = conn.execute(query).fetch_df()

    # Save the trips_by_week as a CSV
    trips_by_week.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
