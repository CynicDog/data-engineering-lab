import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium", layout_file="layouts/main.slides.json")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Python Polars: The Definitive Guide, in marimo!
    """)
    return


@app.cell
def _():
    import marimo as mo

    import polars as pl 
    import polars_geo

    return mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Chapter 1 - Introducing Polars
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Load and Sort Citi Bike Trips Data

    Read March 2024 Citi Bike trip data from Parquet files and sort the trips by their start time.
    """)
    return


@app.cell
def _(pl):
    trips = (
        pl.read_parquet("data/citibike/trips-2024-03-*.parquet")
        .sort("datetime_start")
    )

    trips
    return (trips,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Load and Prepare NYC Neighborhood Boundaries

    Read the NYC neighborhoods GeoJSON file, extract polygon coordinates, and organize the data by neighborhood.
    """)
    return


@app.cell
def _(pl):
    neighborhoods = (
        pl.read_json("./data/citibike/nyc-neighborhoods.geojson")
        .select("features")
        .explode("features")
        .unnest("features")
        .unnest("properties")
        .select("neighborhood", "borough", "geometry")
        .unnest("geometry")
        .with_columns(polygon=pl.col("coordinates").list.first())
        .select("neighborhood", "borough", "polygon")
        .sort("neighborhood")
    )

    neighborhoods
    return (neighborhoods,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Derive Station Locations from Trip Data

    Group trips by starting station and compute the median latitude and longitude to estimate each station’s location.
    """)
    return


@app.cell
def _(pl, trips):
    stations = (
        trips.group_by(station=pl.col("station_start")) 
        .agg(
            lon=pl.col("lon_start").median(), 
            lat=pl.col("lat_start").median(), 
        )
        .sort("station")
        .drop_nulls()
    )

    stations
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Expand Neighborhood Polygons into Coordinate Points

    Assign an ID to each neighborhood and explode polygon coordinates into individual latitude and longitude points.
    """)
    return


@app.cell
def _(neighborhoods, pl):
    neighborhoods_coords = (
        neighborhoods.with_row_index("id")
        .explode("polygon")
        .with_columns(
            lon=pl.col("polygon").list.first(), 
            lat=pl.col("polygon").list.last(), 
        )
        .drop("polygon")
    )

    neighborhoods_coords
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Compute Daily Trips by Borough

    Aggregate trips into daily windows and count the number of trips starting in each borough.
    """)
    return


@app.cell
def _(pl, trips):
    trips_per_day = trips.group_by_dynamic(
        "datetime_start", group_by="borough_start", every="1d" 
    ).agg(num_trips=pl.len())

    trips_per_day
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Chapter 4. Data Structures and Data Types
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Series, DataFrames and LazyFrames
    """)
    return


@app.cell
def _(pl):
    # A Series is a one-dimensional data structure that holds a sequence of values. 
    sales_series = pl.Series("sales", [150.00, 300.00, 250.00]) 

    sales_series
    return (sales_series,)


@app.cell
def _(pl, sales_series):
    # A DataFrame is a two-dimensional data structure that organizes data in a table format. 

    sales_df = pl.DataFrame({
        "sales": sales_series, 
        "customer_id": [24, 25, 26]
    })

    sales_df 
    return


@app.cell
def _(pl):
    # A LazyFrame resembles a DataFrame but holds no data, containing only instructions for reading and processing the data. 
    lazy_df = pl.scan_csv("./data/fruit.csv").with_columns(
        is_heavy=pl.col("weight") > 200 
    )

    lazy_df
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Data Types
    """)
    return


@app.cell
def _(pl):
    dtypes = [
        getattr(pl, name) for name in dir(pl)
        if isinstance(getattr(pl, name), pl.datatypes.DataTypeClass)
    ]

    print(dtypes)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Nested Data Types

    Polars has three nested data types: Array, List, and Struct
    """)
    return


@app.cell
def _(pl):
    # An Array is a collection of elements that are of the same data type. 
    coordinates = pl.DataFrame(
        [
            pl.Series("point_2d", [[1,3], [2,5]]), 
            pl.Series("point_3d", [[1,7,3], [8,1,0]]), 
        ], 
        schema={
            "point_2d": pl.Array(shape=2, inner=pl.Int64), 
            "point_3d": pl.Array(shape=3, inner=pl.Int64), 
        }
    )

    coordinates
    return


@app.cell
def _(pl):
    # A List is comparable to an Array in that it is a collection of elements of the same data type. 
    # A List does not have to have the same length on every row. 
    weather_readings = pl.DataFrame({
        "temperature": [[72.5, 75.0, 77.3], [68.0, 70.2]], 
        "wind_speed": [[15, 20], [10, 12, 14, 16]], 
    })

    weather_readings
    return


@app.cell
def _(pl):
    # A Struct is often used to work with multiple Series at once. 
    raiting_series = pl.Series(
        "ratings", 
        [
            {
                "Movie": "Cars",
                "Theater": "NE", 
                "Avg_Rating": 4.5 
            }, 
            {
                "Movie": "Toy Story",
                "Theater": "ME", 
                "Avg_Rating": 4.9 
            },
        ]
    )
    raiting_series
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Missing Values
    """)
    return


@app.cell
def _(pl):
    missing_df = pl.DataFrame({
        "value": [None, 2, 3, None, None, 6, 7, None]
    })

    missing_df
    return (missing_df,)


@app.cell
def _(missing_df, pl):
    # Fill using a single value 
    missing_df.with_columns(
        pl.col("value").fill_null(0).alias("value_filled")
    )
    return


@app.cell
def _(missing_df, pl):
    # Fill using a fill strategy 
    missing_df.with_columns(
        forward=pl.col("value").fill_null(strategy="forward"),
        backward=pl.col("value").fill_null(strategy="backward"),
        min=pl.col("value").fill_null(strategy="min"),
        max=pl.col("value").fill_null(strategy="max"),
        mean=pl.col("value").fill_null(strategy="mean"),
        zero=pl.col("value").fill_null(strategy="zero"),
        one=pl.col("value").fill_null(strategy="one"),
    )
    return


@app.cell
def _(missing_df, pl):
    # Fill using an expression 
    missing_df.with_columns(
        pl.col("value").fill_null(pl.col("value").mean()).alias("value_filled")
    )
    return


@app.cell
def _(missing_df, pl):
    # Fill using an interpolation
    missing_df.with_columns(
        pl.col("value").interpolate().alias("value_filled")
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Data Type Conversion
    """)
    return


@app.cell
def _(pl):
    string_df = pl.DataFrame({
        "id": ["10000", "20000", "30000"]
    }) 

    print(f"Estimated size: {string_df.estimated_size('b')} bytes") 
    return (string_df,)


@app.cell
def _(pl, string_df):
    int_df = string_df.select(
        pl.col("id").cast(pl.UInt16)
    )

    print(f"Estimated size: {int_df.estimated_size('b')} bytes") 
    return


@app.cell
def _(pl):
    data_types_df = pl.DataFrame({
        "id": [10000, 20000, 30000], 
        "some_value": [1.0, 2.0, 3.5], 
        "other_value": ["1", "2", "3"]
    })

    data_types_df.cast(pl.UInt16)
    return (data_types_df,)


@app.cell
def _(data_types_df, pl):
    # original DataFrame 
    print(data_types_df)  

    # cast specific columns to new types by column names 
    print(data_types_df.cast({
        "id": pl.UInt16, 
        "some_value": pl.Float32, 
        "other_value": pl.UInt8,
    }))  

    # cast columns based on their dtype 
    print(data_types_df.cast({
        pl.Float64: pl.Float32, 
        pl.String: pl.UInt8
    }))  
    return


if __name__ == "__main__":
    app.run()
