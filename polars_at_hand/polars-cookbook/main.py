import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Polars Cookbook
    """)
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import requests

    return mo, pl, requests


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Chapter 1
    """)
    return


@app.cell
def _(pl):
    ch01_titanic = pl.read_csv("data/titanic_dataset.csv")

    ch01_titanic.schema
    return (ch01_titanic,)


@app.cell
def _(ch01_titanic):
    ch01_titanic.columns
    return


@app.cell
def _(ch01_titanic):
    ch01_titanic.flags
    return


@app.cell
def _(pl):
    ch01_titanic_lazy = pl.scan_csv("data/titanic_dataset.csv")
    print(ch01_titanic_lazy.explain())
    return (ch01_titanic_lazy,)


@app.cell
def _(ch01_titanic_lazy):
    ch01_titanic_lazy.head(3).collect()
    return


@app.cell
def _(ch01_titanic_lazy):
    ch01_titanic_lazy.head(3).collect_schema()
    return


@app.cell
def _(ch01_titanic_lazy, pl):
    ch01_titanic_lazy.select(
        pl.col('Name'),
        pl.col('Sex'),
        pl.col('Age'),
        pl.col('Fare'),
        pl.col('Cabin'),
        pl.col('Pclass'),
        pl.col('Survived'),
    ) \
    .filter(pl.col('Age') >= 35) \
    .sort(by=(
        pl.col('Age'), 
        pl.col('Name')
    )) \
    .collect()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Chapter 2
    """)
    return


@app.cell
def _(pl):
    url = "https://raw.githubusercontent.com/PacktPublishing/Polars-Cookbook/refs/heads/main/data/customer_shopping_data.csv"

    ch02_customer_shopping = pl.read_csv(
        url, 
        try_parse_dates=True, 
        schema_overrides={ 'age': pl.Int8, 'quantity': pl.Int32 }
    )
    ch02_customer_shopping.head()
    return (ch02_customer_shopping,)


@app.cell
def _(ch02_customer_shopping):
    ch02_customer_shopping.write_parquet(
        "data/ch02_customer_shopping.parquet", 
        compression='lz4', 
        compression_level=10,
        # use_pyarrow=True, 
        # pyarrow_options={
        #     'existing_data_behavior': 'overwrite_or_ignore'
        # }, 
    )
    return


@app.cell
def _(pl):
    ch02_customer_shopping_arrow = pl.read_parquet(
        "data/ch02_customer_shopping.parquet",
        use_pyarrow=True,
    )

    ch02_customer_shopping_arrow.head()
    return


@app.cell
def _(ch02_customer_shopping):
    ch02_customer_shopping_delta = 'data/ch02_customer_shopping_delta'
    ch02_customer_shopping.write_delta(
        ch02_customer_shopping_delta,
        mode="overwrite", 
        delta_write_options={'partition_by': 'gender'}
    )
    return (ch02_customer_shopping_delta,)


@app.cell
def _(ch02_customer_shopping_delta, pl):
    pl.scan_delta(ch02_customer_shopping_delta)
    return


@app.cell
def _(ch02_customer_shopping_delta, pl):
    pl.scan_delta(
        ch02_customer_shopping_delta, 
        use_pyarrow=True, 
        pyarrow_options={'partitions': [('gender', '=', 'Male')]}
    ).collect()
    return


@app.cell
def _(pl):
    ## requires `fsspec`, `s3fs` pacakge (uv add fsspec s3fs)
    KEY = "csv/month=2019-06/country=ZWE/type=children_under_five/ZWE_children_under_five.csv.gz"
    BUCKET = "dataforgood-fb-data"

    storage_options = {
        "aws_skip_signature": "true",
        "aws_region": "us-east-1" 
    }

    print(pl.scan_csv(f"s3://{BUCKET}/{KEY}", storage_options=storage_options).explain())
    return


@app.cell
def _(pl, requests):
    url = "https://raw.githubusercontent.com/jeroenjanssens/python-polars-the-definitive-guide/refs/heads/main/data/pokedex.json"

    ch02_pokemon = pl.read_json(requests.get(url).content)
    ch02_pokemon.schema
    return (ch02_pokemon,)


@app.cell
def _(ch02_pokemon):
    ch02_pokemon.explode("pokemon") \
        .unnest("pokemon")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
