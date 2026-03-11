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
    ch01_trips = (
        pl.read_parquet("data/citibike/trips-2024-03-*.parquet")
        .sort("datetime_start")
    )

    ch01_trips
    return (ch01_trips,)


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
def _(ch01_trips, pl):
    stations = (
        ch01_trips.group_by(station=pl.col("station_start")) 
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
def _(ch01_trips, pl):
    trips_per_day = ch01_trips.group_by_dynamic(
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
        "id": ["10000", "20000", "30000", "40000"]
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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Chapter 5 - Eager and Lazy APIs
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### EagerAPI: DataFrame
    """)
    return


@app.cell
def _(pl):
    ch05_trips = pl.read_parquet("data/taxi/yellow_tripdata_*.parquet")

    ch05_trips \
        .group_by("VendorID") \
        .agg(
            pl.sum("total_amount"),
            pl.sum("trip_distance"),
        ) \
        .select(
            "VendorID", 
            income_per_distance = pl.col("total_amount") / pl.col("trip_distance"),
        ) \
        .sort(by="income_per_distance", descending=True) \
        .head(3)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Lazy API: LazyFrame
    """)
    return


@app.cell
def _(pl):
    ch05_trips_lazy = pl.scan_parquet("data/taxi/yellow_tripdata_*.parquet")

    ch05_trips_lazy \
        .group_by("VendorID") \
        .agg(
            pl.sum("total_amount"),
            pl.sum("trip_distance"),
        ) \
        .select(
            "VendorID", 
            income_per_distance = pl.col("total_amount") / pl.col("trip_distance"),
        ) \
        .sort(by="income_per_distance", descending=True) \
        .head(3)
    return (ch05_trips_lazy,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Streaming (Out-of-core)
    """)
    return


@app.cell
def _(ch05_trips_lazy, pl):
    ch05_trips_lazy \
        .group_by("VendorID") \
        .agg(
            pl.sum("total_amount"),
            pl.sum("trip_distance"),
        ) \
        .select(
            "VendorID", 
            income_per_distance = pl.col("total_amount") / pl.col("trip_distance"),
        ) \
        .sort(by="income_per_distance", descending=True) \
        .head(3) \
        .collect(engine="streaming")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Chapter 6 - Reading and Writing Data
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Reading CSV Files
    """)
    return


@app.cell
def _(pl):
    penguins = pl.read_csv("./data/penguins.csv", null_values="NA")
    penguins
    return (penguins,)


@app.cell
def _(penguins):
    penguins.null_count().transpose(
        include_header=True, column_names=["null_count"]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Reading Files with Encodings Other than UTF-8
    """)
    return


@app.cell
def _():
    import chardet 

    def detect_encoding(filename: str) -> str: 
        """Return the most probable character encoding for a file."""

        with open(filename, "rb") as f: 
            raw_data = f.read()
            result = chardet.detect(raw_data)

            return result["encoding"]

    return (detect_encoding,)


@app.cell
def _(detect_encoding, pl):
    file = "./data/directors.csv"

    pl.read_csv(file, encoding=detect_encoding(file))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Reading Excel Spreadsheets
    """)
    return


@app.cell
def _():
    # # Expects ```ModuleNotFoundError: required package 'fastexcel' not found.``` 
    # songs = pl.read_excel("./data/top2000-2023.xlsx")
    # songs
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Working with Multiple Files
    """)
    return


@app.cell
def _(pl):
    import calendar 

    filenames = [
        f"./data/stock/asml/{year}.csv" for year in range(1999, 2024) if calendar.isleap(year) 
    ]

    pl.concat(pl.read_csv(f) for f in filenames)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Reading Parquet
    """)
    return


@app.cell
def _(pl):
    pl.read_parquet_metadata("./data/taxi/yellow_tripdata_2022-12.parquet")
    return


@app.cell
def _(pl):
    pl.read_parquet_schema("./data/taxi/yellow_tripdata_2022-12.parquet")
    return


@app.cell
def _(pl):
    pl.read_parquet("./data/taxi/yellow_tripdata_2022-12.parquet")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Reading JSON and NDJSON
    """)
    return


@app.cell
def _(pl):
    pokedex = pl.read_json("./data/pokedex.json")
    pokedex # Polars doesn't make any assumptions as to how to flatten a nested structure into a rectangular shape 
    return (pokedex,)


@app.cell
def _(pokedex):
    pokedex.explode("pokemon") \
        .unnest("pokemon")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Other File Formats

    - `pl.read_ipc()`
    - `pl.read_avro()`
    - `pl.read_delta()`
    - `pl.scan_pyarrow_dataset()`
    """)
    return


@app.cell
def _():
    # import pandas as pd

    # url = "https://en.wikipedia.org/wiki/List_of_Latin_abbreviations"
    # pl.from_pandas(pd.read_html(url)[0])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Querying Databases
    """)
    return


@app.cell
def _(pl):
    pl.read_database_uri(
        query="""
        SELECT
            f.film_id,
            f.title,
            c.name AS category,
            f.rating,
            f.length / 60.0 AS length
        FROM
            film AS f,
            film_category AS fc,
            category AS c
        WHERE
            fc.film_id = f.film_id
            AND fc.category_id = c.category_id
        LIMIT 10
        """,
        uri="sqlite:::data/sakila.db",
    )
    return


@app.cell
def _(pl):
    db = "sqlite:::data/sakila.db"
    films = pl.read_database_uri("SELECT * FROM film", db)
    film_categories = pl.read_database_uri("SELECT * FROM film_category", db)
    categories = pl.read_database_uri("SELECT * FROM category", db)

    (
        films.join(film_categories, on="film_id", suffix="_fc")
        .join(categories, on="category_id", suffix="_c")
        .select(
            "film_id",
            "title",
            pl.col("name").alias("category"),
            "rating",
            pl.col("length") / 60,
        )
        .limit(10)
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Writing Data

    - `df.write_csv()`
    - `df.write_excel()`
    - `df.write_parquet()`
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Chapter 7 - Beginning Expressions
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Expressions By Example
    """)
    return


@app.cell
def _(pl):
    fruit = pl.read_csv("./data/fruit.csv")
    fruit
    return (fruit,)


@app.cell
def _(fruit, pl):
    # Selecting Columns with Expressions 
    fruit.select(
        pl.col("name"), 
        pl.col("^.*or.*$"), 
        pl.col("weight") / 1000, 
        "is_round"
    )
    return


@app.cell
def _(fruit, pl):
    # Creating New Columns with Expressions 
    fruit.with_columns(
        pl.lit(True).alias("is_fruit"), 
        is_berry=pl.col("name").str.ends_with("berry"), 
    )
    return


@app.cell
def _(fruit, pl):
    # Filtering Rows with Expressions 
    fruit.filter(
        (pl.col("weight") > 1000) & pl.col("is_round")
    )
    return


@app.cell
def _(fruit, pl):
    # Aggregating with Expressions 
    fruit \
        .group_by( 
            pl.col("origin").str.split(" ").list.last() # e.g. North America → America
        ) \
        .agg(
            count=pl.len(), 
            average_weight=pl.col("weight").mean()
        )
    return


@app.cell
def _(fruit, pl):
    # Sorting Rows with Expressions 
    fruit.sort(
        pl.col("name").str.len_bytes(),
        descending=True
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Creating Expressions
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### From Existing Columns
    """)
    return


@app.cell
def _(fruit, pl):
    fruit.select(pl.col("color"))
    return


@app.cell
def _(fruit, pl):
    fruit.select(pl.col(pl.Boolean, pl.Int64))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### From Literal Values
    """)
    return


@app.cell
def _(pl):
    pl.DataFrame({
        "int32": pl.select(pl.lit(42)),
        "bool": pl.select(pl.lit(True)),
    })
    return


@app.cell
def _(pl):
    pl.select(
        pl.repeat("A", 3), 
        pl.zeros(3), 
        pl.ones(3),
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### From Ranges
    """)
    return


@app.cell
def _(pl):
    pl.select(
        start=pl.date_range(pl.date(1999,12,1), pl.date(1999,12,5)), 
        end=pl.repeat(pl.date(2000,1,1), 5), 
    ).with_columns(
        range=pl.datetime_ranges("start", "end", interval="8h")
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Chapter 8 - Continuing Expressions
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Types of Operations

    - Maintain Length
      - Element-wise
      - Series-wise
    - Reduce Length
      - To One
      - To One or More
    - Extend Length
    """)
    return


@app.cell
def _(pl):
    # Example A - Element-Wise Operations 

    penguins = pl.read_csv("./data/penguins.csv", null_values="NA") \
        .select(
            "species", 
            "island", 
            "sex", 
            "year", 
            mass=pl.col("body_mass_g") / 1000, 
        ) \
        .with_columns(
            mass_sqrt=pl.col("mass").sqrt(),
            mass_exp=pl.col("mass").exp()
        )

    penguins
    return (penguins,)


@app.cell
def _(penguins, pl):
    # Example B - Operations that summarize to one 
    penguins.select(
        pl.col("mass").mean(), 
        pl.col("island").first(),
    )
    return


@app.cell
def _(penguins, pl):
    # Example C - Operations that summarize to one or more 
    penguins.select(
        pl.col("island").unique()
    )
    return


@app.cell
def _(penguins, pl):
    # Example D -Opertaions that extend 
    penguins.select(
        pl.col("species") 
            .unique() 
            .repeat_by(5)
            .explode()
            .extend_constant("Saiyan", n=3)
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Element-Wise Operations
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Operations that perform mathematical transformations
    """)
    return


@app.cell
def _(pl):
    import math 

    pl.DataFrame({
        "x": [-2.0, 0.0, 0.5, 1.0, math.e, 1000.0]
    }).with_columns(
        abs=pl.col("x").abs(),
        exp=pl.col("x").exp(),
        log2=pl.col("x").log(2),
        log10=pl.col("x").log10(),
        log1p=pl.col("x").log1p(),
        sign=pl.col("x").sign(),
        sqrt=pl.col("x").sqrt(),
    )
    return (math,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Operations related to Trigonometry
    """)
    return


@app.cell
def _(math, pl):
    pl.DataFrame(
        {"x": [-math.pi, 0.0, 1.0, math.pi, 2 * math.pi, 90.0, 180.0, 360.0]}
    ).with_columns(
        arccos=pl.col("x").arccos(),
        cos=pl.col("x").cos(),
        degrees=pl.col("x").degrees(),
        radians=pl.col("x").radians(),
        sin=pl.col("x").sin(),
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Operations that round and categorize
    """)
    return


@app.cell
def _(math, pl):
    pl.DataFrame(
        {"x": [-6.0, -0.5, 0.0, 0.5, math.pi, 9.9, 9.99, 9.999]}
    ).with_columns(
        ceil=pl.col("x").ceil(),
        clip=pl.col("x").clip(-1, 1),
        cut=pl.col("x").cut([-1, 1], labels=["bad", "neutral", "good"]),
        floor=pl.col("x").floor(),
        qcut=pl.col("x").qcut([0.5], labels=["below median", "above median"]),
        round2=pl.col("x").round(2),
        round0=pl.col("x").round(0), 
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Operations for missing or infinite values
    """)
    return


@app.cell
def _(math, pl):
    x = [42.0, math.nan, None, math.inf, -math.inf]
    pl.DataFrame({
        "x": x
    }).with_columns(
        fill_nan=pl.col("x").fill_nan(99), 
        fill_null=pl.col("x").fill_null(0), 
        is_finite=pl.col("x").is_finite(), 
        is_infinite=pl.col("x").is_infinite(), 
        is_nan=pl.col("x").is_nan(), 
        is_null=pl.col("x").is_null(),
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Series-Wise Operation: Non-reducing
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Operation That Accumulate
    """)
    return


@app.cell
def _(math, pl):

    pl.DataFrame({
        "x": [0.0, 1.0, 2.0, None, 3.0, math.nan, -1.0, 2.0]
    }).with_columns(
        cumulative_count=pl.col("x").cum_count(), 
        cumulative_max=pl.col("x").cum_max(), 
        cumulative_min=pl.col("x").cum_min(), 
        cumulative_prod=pl.col("x").cum_prod(reverse=True), 
        cumulative_sum=pl.col("x").cum_sum(), 
        diff=pl.col("x").diff(), 
        pct_change=pl.col("x").pct_change(),
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Operations that fill and shift
    """)
    return


@app.cell
def _(math, pl):
    pl.DataFrame({
        "x": [-1.0, 0.0, 1.0, None, None, 3.0, 4.0, math.nan, 6.0]
    }).with_columns(
        backward_fill=pl.col("x").backward_fill(),
        forward_fill=pl.col("x").forward_fill(),
        interp1=pl.col("x").interpolate(method="linear"),
        interp2=pl.col("x").interpolate(method="nearest"),
        shift1=pl.col("x").shift(1),
        shift2=pl.col("x").shift(-2)
    )

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Operations related to duplicate values
    """)
    return


@app.cell
def _(pl):
    pl.DataFrame({"x": ["A", "C", "D", "C"]}).with_columns(  
        is_duplicated=pl.col("x").is_duplicated(),
        is_first_distinct=pl.col("x").is_first_distinct(),
        is_last_distinct=pl.col("x").is_last_distinct(),
        is_unique=pl.col("x").is_unique(),
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Operations that compute rolling statistics
    """)
    return


@app.cell
def _(pl):
    stock = pl.read_csv("./data/stock/nvda/2023.csv", try_parse_dates=True) \
        .select("date", "close") \
        .with_columns(
            rolling_mean_7 = pl.col("close").rolling_mean(window_size=7),
            rolling_std_7 = pl.col("close").rolling_std(window_size=7),
            rolling_max_14 = pl.col("close").rolling_max(window_size=14),
            rolling_min_14 = pl.col("close").rolling_min(window_size=14),
            rolling_median_7 = pl.col("close").rolling_median(window_size=7),
            ewm_mean = pl.col("close").ewm_mean(com=7),
            ewm_std = pl.col("close").ewm_std(com=7),
        )

    stock
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Operations that sort
    """)
    return


@app.cell
def _(pl):
    pl.DataFrame({
        "x": [1, 3, None, 3, 7],
        "y": ["D", "I", "S", "C", "O"],
    }).with_columns(
        arg_sort=pl.col("x").arg_sort(),
        shuffle=pl.col("x").shuffle(seed=42),
        sort=pl.col("x").sort(nulls_last=True),
        sort_by=pl.col("x").sort_by("y"),
        reverse=pl.col("x").reverse(),
        rank=pl.col("x").rank()
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Series-Wise Operations: Reducing to One
    """)
    return


if __name__ == "__main__":
    app.run()
