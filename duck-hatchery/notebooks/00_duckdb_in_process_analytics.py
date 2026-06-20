import marimo

__generated_with = "0.23.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # DuckDB, hands-on

    A beginner-friendly but thorough tour of **DuckDB** through its Python client.

    DuckDB is an **in-process analytical (OLAP) database** — the easiest mental model is *"SQLite, but for analytics."* SQLite gives you a full SQL database that runs *inside* your process with no server to install or start; DuckDB does the same, but its engine is built for the queries analysts actually run — scanning columns, grouping, aggregating, joining — over tables with millions of rows.

    The thing to feel by the end of this notebook: **DuckDB meets your data where it already lives.** A pandas DataFrame, a Polars DataFrame, an Arrow table, a folder of Parquet files on disk or in the cloud — DuckDB queries all of them in place, with one SQL dialect, often without copying a single byte. There is no "load the data into the database" step to suffer through first.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Setup

    There is almost nothing to set up — and that *is* the first lesson.

    A "real" analytical database (Postgres, a Spark cluster, a warehouse) is a server: a process you start, connect to over a socket, authenticate against, and feed data into before you can query it. DuckDB is a **library**. You `import duckdb` and you have a complete SQL engine in the same Python process, sharing the same memory as your variables.

    No JVM, no daemon, no ports, no credentials. `uv sync` installed it as an ordinary wheel.
    """)
    return


@app.cell
def _():
    import duckdb

    duckdb.__version__
    return (duckdb,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Your first query

    `duckdb.sql(...)` runs a SQL statement and hands back a **relation**. Two things are worth noticing immediately:

    1. You didn't open a connection. The module-level `duckdb.sql` uses a default, in-memory database that springs into existence on first use — perfect for exploration.
    2. The thing that comes back isn't a list of rows or a DataFrame. It's a `DuckDBPyRelation`, DuckDB's handle to a query.
    """)
    return


@app.cell
def _(duckdb):
    greeting = duckdb.sql("SELECT 'quack' AS sound, 42 AS the_answer")
    greeting
    return (greeting,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The boxed table you see above is DuckDB's own pretty-printer — the relation's `__repr__`. marimo is showing it as text rather than as one of its interactive tables, and that's a deliberate signal: **a relation is not yet data.** It's a *query*, sitting lazily, ready to run. DuckDB only printed a preview because we asked to look at it.

    This laziness matters. It means you can build up a big query out of small relations without executing anything until the final moment you actually need rows. Two ways to force execution (these are **actions**, in the same sense as Spark):

    - **`.show()`** — run the query and print the preview box. Good for eyeballing.
    - **`.fetchall()`** — run the query and pull every row back into Python as a list of tuples.
    """)
    return


@app.cell
def _(greeting):
    greeting.show()           # action: prints DuckDB's preview box
    greeting.fetchall()       # action: materializes rows as Python tuples
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Connections — the default one, and your own

    The module-level `duckdb.sql` is a convenience that runs against a single hidden in-memory database. The moment you want *more than one* database, or you want your data to **survive the process**, you create your own connection with `duckdb.connect(...)`.

    | Call                            | What you get                                                                    |
    |---------------------------------|---------------------------------------------------------------------------------|
    | `duckdb.connect()`              | A fresh, private **in-memory** database. Gone when the connection closes.        |
    | `duckdb.connect("hatch.duckdb")`| A database backed by a **file on disk**. Tables persist across runs.             |
    | `con.sql(...)` / `con.execute(...)` | The same API as `duckdb.sql`, but scoped to *your* connection's tables.      |

    `con.sql(query)` is for queries you want a relation back from. `con.execute(stmt)` is for statements where you mostly care about the side effect — `CREATE TABLE`, `INSERT`, `COPY` — though it returns the connection so you can chain a `.fetchall()` if you like.
    """)
    return


@app.cell
def _(duckdb):
    con = duckdb.connect()  # our own private in-memory database for the rest of the notebook

    con.execute("CREATE TABLE pond (duckling VARCHAR, weight_g DOUBLE)")
    con.execute("INSERT INTO pond VALUES ('Pip', 58.2), ('Quill', 61.0)")

    con.sql("SELECT * FROM pond")
    return (con,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Persisting to a file

    Point `connect` at a path and the database is a single file. Write to it, close it, and the data is still there next time — no export step, no schema migration dance. This is genuinely useful: a DuckDB file is a portable, self-contained analytical dataset you can email to a colleague.
    """)
    return


@app.cell
def _(duckdb):
    import tempfile
    from pathlib import Path

    workdir = Path(tempfile.mkdtemp(prefix="duck-hatchery-"))
    db_path = workdir / "hatchery.duckdb"

    # Open the file, write a table, close it.
    _first = duckdb.connect(str(db_path))
    _first.execute("CREATE TABLE clutch AS SELECT * FROM range(1, 6) t(egg_no)")
    _first.close()

    # Re-open a brand-new connection to the same file — the table is still there.
    _again = duckdb.connect(str(db_path))
    survived = _again.sql("SELECT count(*) AS eggs FROM clutch").fetchone()
    _again.close()

    f"reopened the file and found {survived[0]} eggs still in the clutch"
    return Path, workdir


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## The hatchery dataset

    For the rest of the notebook we need something with a bit of shape to it. Here's a small **duck hatchery**: ten ducklings, each belonging to a *brood* (a clutch raised together), with a species, a hatch date, a weight, and whether the vet marked it healthy.

    We build it as a **pandas DataFrame** on purpose — it's the format most people arrive with, and it sets up the first real aha moment in the next section.
    """)
    return


@app.cell
def _():
    import pandas as pd

    ducklings = pd.DataFrame(
        {
            "id":         [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "name":       ["Pip", "Quill", "Waddle", "Sora", "Reed",
                           "Marsh", "Dabble", "Scoot", "Nibble", "Puddle"],
            "brood":      ["mallard-pond", "mallard-pond", "mallard-pond", "wood-creek", "wood-creek",
                           "wood-creek", "runner-yard", "runner-yard", "runner-yard", "mallard-pond"],
            "species":    ["mallard", "mallard", "mallard", "wood", "wood",
                           "wood", "runner", "runner", "runner", "mallard"],
            "hatched_on": ["2026-05-01", "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-03",
                           "2026-05-04", "2026-05-02", "2026-05-02", "2026-05-05", "2026-05-06"],
            "weight_g":   [58.2, 61.0, 47.5, 39.1, 41.8, 35.0, 52.3, 55.9, 49.0, 44.2],
            "healthy":    [True, True, False, True, True, False, True, True, True, False],
        }
    )
    ducklings
    return ducklings, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Aha #1 — query a Python DataFrame as if it were a table

    Here is the move that makes DuckDB click. We never imported `ducklings` into DuckDB, never created a table, never copied anything. We just **wrote its variable name in the `FROM` clause**:
    """)
    return


@app.cell
def _(con, ducklings):
    con.sql("""
        SELECT brood, species, weight_g
        FROM ducklings                 -- this is the pandas DataFrame, by its Python name
        WHERE healthy AND weight_g > 45
        ORDER BY weight_g DESC
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    That `FROM ducklings` worked because of **replacement scans**: when DuckDB parses a query and hits a table name it doesn't recognize, it looks in your Python scope for a variable of that name. Find a DataFrame? It scans it directly.

    And "directly" is meant literally. For Arrow-backed data (which Polars and PyArrow tables are, and which pandas can be) DuckDB reads the **same memory** your DataFrame already occupies — **zero copy**. The SQL engine is reaching straight into your process's columns. This is the deep reason DuckDB feels different from a server database: there is no network, no serialization, no import. The data never moved.

    It works for the other dataframe libraries too — same query, different source object:
    """)
    return


@app.cell
def _(con, ducklings, pd):
    import polars as pl
    import pyarrow as pa

    ducklings_pl = pl.from_pandas(ducklings)     # a Polars DataFrame
    ducklings_arrow = pa.Table.from_pandas(ducklings)  # an Arrow table

    # Same SQL, three different in-memory sources — DuckDB doesn't care which.
    counts = {
        "from pandas": con.sql("SELECT count(*) FROM ducklings").fetchone()[0],
        "from polars": con.sql("SELECT count(*) FROM ducklings_pl").fetchone()[0],
        "from arrow":  con.sql("SELECT count(*) FROM ducklings_arrow").fetchone()[0],
    }
    counts
    return pa, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### When the name isn't in scope — `register`

    Replacement scans find variables in the *calling* Python frame. That covers almost all interactive work. But sometimes you want a stable name that doesn't depend on what a variable happens to be called — for example to hand a connection to another function. `con.register("view_name", dataframe)` pins a DataFrame under an explicit name, and `con.unregister(...)` removes it.

    > Registering does **not** copy the data either — it just records a named pointer to your DataFrame. It's the explicit cousin of the replacement scan, not a heavier one.
    """)
    return


@app.cell
def _(con, ducklings):
    con.register("hatchlings", ducklings)   # explicit name, independent of the variable name
    n = con.sql("SELECT count(*) FROM hatchlings").fetchone()[0]
    con.unregister("hatchlings")
    f"queried {n} rows through the registered name 'hatchlings'"
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Getting results back out

    A query is only useful if you can get its answer into a shape your program wants. This is where DuckDB's Python client is unusually generous — it speaks every common in-memory format, so DuckDB slots into a pandas, Polars, or Arrow workflow without friction.

    | Method                         | Returns                                  | Reach for it when…                                            |
    |--------------------------------|------------------------------------------|---------------------------------------------------------------|
    | `.fetchall()`                  | `list[tuple]`                            | Small results, plain Python, a quick loop                     |
    | `.fetchone()`                  | one `tuple` (or `None`)                  | You expect a single row — a scalar aggregate, an existence check |
    | `.fetchmany(n)`                | `list[tuple]` of ≤ n rows               | Paging through a large result a chunk at a time               |
    | `.df()`                        | **pandas** `DataFrame`                   | Handing off to the pandas / scikit-learn world                |
    | `.pl()`                        | **Polars** `DataFrame`                   | Staying in a Polars pipeline                                  |
    | `.arrow()`                     | Arrow `RecordBatchReader` (streaming)    | Feeding another Arrow-native tool without materializing       |
    | `.fetchnumpy()`                | `dict[str, np.ndarray]`                  | Column-at-a-time numeric work                                 |

    The DataFrame returns are the real payoff: a query result comes back as a first-class DataFrame you can keep computing on — and marimo renders it as a rich, sortable, paginated table instead of a plain text box.
    """)
    return


@app.cell
def _(con):
    by_brood = con.sql("""
        SELECT brood,
               count(*)            AS n,
               round(avg(weight_g), 1) AS avg_weight_g,
               sum(healthy::INT)   AS n_healthy
        FROM ducklings
        GROUP BY brood
        ORDER BY brood
    """)

    # Returning a *DataFrame* (not the relation) hands marimo an interactive table.
    by_brood.df()
    return (by_brood,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Same query, the Polars way — note the result is a genuine `polars.DataFrame`, ready for the rest of a Polars pipeline:
    """)
    return


@app.cell
def _(by_brood):
    brood_pl = by_brood.pl()
    print(type(brood_pl))
    brood_pl
    return


@app.cell
def _(con):
    # Scalar answers want fetchone; column arrays want fetchnumpy.
    heaviest = con.sql("SELECT name, weight_g FROM ducklings ORDER BY weight_g DESC LIMIT 1").fetchone()
    weights = con.sql("SELECT weight_g FROM ducklings").fetchnumpy()["weight_g"]
    heaviest, weights.mean().round(2), weights.std().round(2)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## The relational API — SQL without the SQL strings

    Writing SQL in triple-quoted strings is fine, but DuckDB also exposes a **relational API**: methods on a relation that each return a *new* relation. If you've used Spark or Polars, this is the same shape — small, composable transformations that build a query without ever materializing intermediate results.

    Every method below is lazy. Nothing runs until the final `.df()`.
    """)
    return


@app.cell
def _(con):
    base = con.sql("SELECT * FROM ducklings")   # a relation over the DataFrame

    chained = (
        base
        .filter("healthy")                      # WHERE healthy
        .project("brood, species, weight_g")    # SELECT these columns
        .order("weight_g DESC")                 # ORDER BY weight_g DESC
        .limit(5)                               # LIMIT 5
    )
    chained.df()
    return (base,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The arguments are still SQL *fragments* — `"healthy"`, `"weight_g DESC"` — so you keep SQL's expressiveness, but the **structure** of the query is ordinary Python you can build with loops and conditionals. That's the sweet spot the relational API hits: assemble a query from variables, then drop into a SQL string only for the parts that are genuinely relational.

    Because each step returns a relation, relations **compose** — you can derive one from another, and even refer to one relation *inside another's SQL by its Python name* (replacement scans again, now over DuckDB's own relations):
    """)
    return


@app.cell
def _(base, con):
    healthy_ducklings = base.filter("healthy")   # one relation...

    # ...referenced by name inside a fresh query. Still lazy, still zero intermediate copies.
    con.sql("""
        SELECT species, count(*) AS n_healthy
        FROM healthy_ducklings
        GROUP BY species
        ORDER BY n_healthy DESC
    """).df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Aha #2 — query files on disk without loading them

    The replacement-scan trick extends to **files**. You can put a path to a Parquet, CSV, or JSON file straight in the `FROM` clause and DuckDB will scan it — no "read the file into a DataFrame first" step. For columnar formats like Parquet this is the heart of why DuckDB is fast: it reads only the columns and row groups a query actually touches.

    First, let's write our hatchery out to disk in two formats so we have files to query. `COPY ... TO` is the SQL way; `.write_parquet()` / `.write_csv()` are the relational-API shortcuts.
    """)
    return


@app.cell
def _(con, workdir):
    parquet_path = workdir / "ducklings.parquet"
    csv_path = workdir / "ducklings.csv"

    con.sql("SELECT * FROM ducklings").write_parquet(str(parquet_path))
    con.execute(f"COPY (SELECT * FROM ducklings) TO '{csv_path}' (HEADER, DELIMITER ',')")

    sorted(p.name for p in workdir.iterdir())
    return csv_path, parquet_path


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Now query the Parquet file as a table. Notice there's no read step and no schema declaration — Parquet is self-describing, so DuckDB knows the column types already.
    """)
    return


@app.cell
def _(con, parquet_path):
    con.sql(f"""
        SELECT species, count(*) AS n, round(avg(weight_g), 1) AS avg_weight_g
        FROM '{parquet_path}'          -- a file path, used exactly like a table name
        GROUP BY species
        ORDER BY species
    """).df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    CSV has no type information baked in, so DuckDB **sniffs** it — sampling rows to auto-detect column types, the delimiter, and the header. `read_csv` (alias `read_csv_auto`) exposes options when the guess needs help; the bare path form just works for clean files.

    The `read_parquet` / `read_csv` function forms (instead of a bare path) are what you use when you need **globs** — one query over a whole folder of files, e.g. `read_parquet('data/year=2026/*.parquet')`. DuckDB reads them as a single table and, with `filename=true`, can even tell you which file each row came from.
    """)
    return


@app.cell
def _(con, csv_path):
    con.sql(f"""
        SELECT *
        FROM read_csv('{csv_path}', header = true)
        WHERE NOT healthy            -- types were sniffed: `healthy` came back as a real BOOLEAN
    """).df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    And the move that retires a lot of glue code — **join a file against an in-memory DataFrame in one query.** DuckDB treats the Parquet file and the pandas `ducklings` as two tables and joins them. No staging, no matching import formats.
    """)
    return


@app.cell
def _(con, ducklings, parquet_path):
    # Pretend the Parquet file is "yesterday's" data and `ducklings` is "today's";
    # find ducklings whose weight changed. Here they're the same, so the point is the *mechanism*.
    _ = ducklings  # ensure the name is in scope for the replacement scan
    con.sql(f"""
        SELECT today.name,
               today.weight_g       AS weight_now,
               disk.weight_g        AS weight_on_disk
        FROM ducklings AS today
        JOIN '{parquet_path}' AS disk USING (id)
        WHERE today.weight_g <> disk.weight_g
    """).df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    > **Remote files, same syntax.** With the `httpfs` extension (`INSTALL httpfs; LOAD httpfs;`) the path can be an `https://` URL or an `s3://` bucket key, and everything above works unchanged — DuckDB ranges over the remote file and pulls only the bytes it needs. That's how people query Parquet on S3 from a laptop without downloading the whole dataset.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Inspecting data — `DESCRIBE` and `SUMMARIZE`

    Two built-ins pay for themselves constantly. `DESCRIBE` shows the schema of any query or file. `SUMMARIZE` runs a full profiling pass — min, max, approximate distinct count, quartiles, null percentage per column — the first thing worth doing on data you've never seen.
    """)
    return


@app.cell
def _(con, parquet_path):
    print("— DESCRIBE —")
    con.sql(f"DESCRIBE SELECT * FROM '{parquet_path}'").show()

    print("— SUMMARIZE —")
    con.sql("SUMMARIZE SELECT * FROM ducklings").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## The friendly SQL dialect

    DuckDB's SQL has a set of ergonomic extensions that, once you've used them, are hard to give up. They're small, but they remove the most common papercuts of writing analytical SQL.

    | Feature                       | What it does                                                                 |
    |-------------------------------|------------------------------------------------------------------------------|
    | `FROM tbl SELECT ...`        | `FROM` may come first — even `FROM tbl` alone means `SELECT * FROM tbl`       |
    | `SELECT * EXCLUDE (c)`       | All columns *except* some — no more typing out 19 of 20 column names         |
    | `SELECT * REPLACE (expr AS c)`| All columns, but swap one for a computed version, keeping its position       |
    | `GROUP BY ALL`               | Group by every non-aggregated `SELECT` column — no restating the list        |
    | `ORDER BY ALL`               | Order by every selected column, left to right                                |
    | `COLUMNS(...)`               | Apply a function or pattern across many columns at once                      |
    | `col::TYPE`                  | Postgres-style cast shorthand for `CAST(col AS TYPE)`                        |

    A whirlwind tour:
    """)
    return


@app.cell
def _(con):
    # FROM-first, and `* EXCLUDE` to drop a column without listing the rest.
    con.sql("FROM ducklings SELECT * EXCLUDE (id, hatched_on) LIMIT 3").show()

    # GROUP BY ALL: group by brood & species without repeating them in a GROUP BY list.
    con.sql("""
        SELECT brood, species, count(*) AS n, avg(weight_g) AS avg_w
        FROM ducklings
        GROUP BY ALL
        ORDER BY ALL
    """).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    `COLUMNS(...)` is the quietly powerful one: it lets a single expression fan out over many columns. Here we ask for the `max` of *every numeric column* without naming them, by excluding the non-numeric ones — handy when a table is wide or its columns aren't known in advance.
    """)
    return


@app.cell
def _(con):
    con.sql("""
        SELECT max(COLUMNS(* EXCLUDE (name, brood, species, hatched_on)))
        FROM ducklings
    """).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Aggregations, windows, and `QUALIFY`

    Everything so far has been setup for the work analytical databases are built for: summarizing and ranking. Two layers:

    - **Aggregates** (`GROUP BY`) collapse a group of rows into one — counts, sums, averages.
    - **Window functions** (`OVER (...)`) compute across a group of related rows but keep **one output row per input row**. Running totals, rankings, "compared to my brood's average" — all windows.

    Casting dates is worth a beat too: our `hatched_on` is a string, but `::DATE` turns it into a real date so we can do arithmetic on it.
    """)
    return


@app.cell
def _(con):
    con.sql("""
        SELECT
            name,
            brood,
            weight_g,
            hatched_on::DATE AS hatched,
            -- window: each duckling's weight vs. its brood's average, no collapsing
            round(avg(weight_g) OVER (PARTITION BY brood), 1)            AS brood_avg,
            round(weight_g - avg(weight_g) OVER (PARTITION BY brood), 1) AS vs_brood_avg
        FROM ducklings
        ORDER BY brood, weight_g DESC
    """).df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    `QUALIFY` is to window functions what `HAVING` is to `GROUP BY`: it filters on the *result* of a window function without forcing you to wrap the query in a subquery. Here we rank ducklings by weight within each brood and keep only the heaviest of each — a clean "top-N per group," which in plain SQL is famously clumsy.
    """)
    return


@app.cell
def _(con):
    con.sql("""
        SELECT name, brood, weight_g,
               row_number() OVER (PARTITION BY brood ORDER BY weight_g DESC) AS rank_in_brood
        FROM ducklings
        QUALIFY rank_in_brood = 1
        ORDER BY brood
    """).df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Persisting results — tables, CTAS, and views

    Querying in place is the headline, but sometimes you do want DuckDB to *hold* data — a cleaned table to query repeatedly, or a saved view of a transformation. The vocabulary is standard SQL:

    | Statement                          | Meaning                                                              |
    |------------------------------------|----------------------------------------------------------------------|
    | `CREATE TABLE t AS SELECT ...`     | **CTAS** — materialize a query's result into a new stored table      |
    | `INSERT INTO t SELECT ...`         | Append more rows                                                     |
    | `CREATE VIEW v AS SELECT ...`      | A *named query* — re-runs each time you select from it, stores no data |

    A CTAS copies rows into the database (so it no longer tracks the source DataFrame); a view stores only the query text. Reach for a table when you'll query the result many times; a view when you want a reusable name for a transformation.
    """)
    return


@app.cell
def _(con):
    # CTAS: freeze a cleaned, enriched table inside the database.
    con.execute("""
        CREATE OR REPLACE TABLE healthy_report AS
        SELECT brood, species, count(*) AS n, round(avg(weight_g), 1) AS avg_weight_g
        FROM ducklings
        WHERE healthy
        GROUP BY ALL
    """)

    # A view: a saved name for "the underweight ducklings", recomputed on each use.
    con.execute("CREATE OR REPLACE VIEW underweight AS SELECT * FROM ducklings WHERE weight_g < 45")

    stored = con.sql("SELECT table_name, table_type FROM information_schema.tables ORDER BY table_name").df()
    stored
    return


@app.cell
def _(con):
    con.sql("SELECT * FROM healthy_report ORDER BY brood, species").show()
    con.sql("SELECT name, weight_g FROM underweight ORDER BY weight_g").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Peeking under the hood — `EXPLAIN`

    Why is DuckDB fast on this kind of work? Two reasons worth internalizing:

    1. **Columnar storage.** Data is laid out one column at a time, so a query that touches 3 of 20 columns reads only those 3. A row-store database would read all 20.
    2. **Vectorized execution.** DuckDB processes data in *batches* of ~2048 values at a time rather than row-by-row, keeping the CPU's caches hot and letting it use SIMD instructions. It's the middle ground between slow tuple-at-a-time interpreters and the complexity of full query compilation.

    `EXPLAIN` shows the physical plan DuckDB will run; `EXPLAIN ANALYZE` runs it and annotates each operator with timings and row counts. Read the plan bottom-up: scan at the leaves, then filters, then the aggregate at the top.
    """)
    return


@app.cell
def _(con):
    plan = con.sql("""
        EXPLAIN
        SELECT brood, avg(weight_g)
        FROM ducklings
        WHERE healthy
        GROUP BY brood
    """).fetchone()[1]
    print(plan)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Look for the `PROJECTION` / `FILTER` / `AGGREGATE` boxes and, at the leaf, the scan over the pandas DataFrame. Even on this toy table you can see the optimizer pushing the `WHERE healthy` filter down toward the scan so fewer rows climb the plan — the same pushdown idea that lets DuckDB skip whole row groups in a Parquet file.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## When to reach for DuckDB

    Pulling the threads together — the aha moments, restated as a decision guide:

    - **You have data in a DataFrame and want SQL.** `duckdb.sql("... FROM your_df ...")` — zero copy, no import. The fastest path from "I have a pandas/Polars frame" to "I can write a GROUP BY."
    - **You have Parquet/CSV files** (locally or on S3) and want to query them without spinning up a warehouse or loading them into anything. Put the path in `FROM`.
    - **Your data is bigger than a DataFrame library wants to hold** but smaller than needs a cluster. DuckDB streams from disk and spills when it must, so it handles tables larger than RAM on a single machine.
    - **You want one SQL dialect across pandas, Polars, Arrow, and files** — joining across all of them in a single query.

    Where it's *not* the tool: high-concurrency transactional workloads (many small writes from many clients) — that's an OLTP database's job, Postgres or SQLite. DuckDB is built for analytical reads, one process at a time.

    The mental shift to keep: with a server database you bring your data *to* the engine. With DuckDB, the engine comes to your data — wherever it already is.
    """)
    return


@app.cell(hide_code=True)
def _(con, mo):
    # Tidy up the connection when the notebook tears down.
    _ = con
    mo.md("*Connection stays open for interactive use; marimo closes it when the kernel stops.*")
    return


if __name__ == "__main__":
    app.run()
