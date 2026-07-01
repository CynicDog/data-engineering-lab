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
    # High Performance Spark — Lab Notebook

    Runnable Python translations from *High Performance Spark* (Karau & Warren).
    Covers Ch. 5 (DataFrames, Datasets, and Spark SQL) and Ch. 6 (Joins).
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Setup

    This notebook boots a local `SparkSession` against a pinned JDK 17 (Temurin).
    The shell default JDK on this machine is 25, which Spark 4 does not officially support — so we override `JAVA_HOME` *before* importing `pyspark`.
    """)
    return


@app.cell
def _():
    import os
    import subprocess
    import tempfile
    from pathlib import Path
    from textwrap import dedent

    JAVA_HOME = Path(
        subprocess.check_output(["/usr/libexec/java_home", "-v", "17"], text=True).strip()
    )
    assert (JAVA_HOME / "bin" / "java").exists(), f"JDK not found at {JAVA_HOME}"

    os.environ["JAVA_HOME"] = str(JAVA_HOME)
    os.environ["PATH"] = f"{JAVA_HOME / 'bin'}:{os.environ['PATH']}"

    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

    log4j_props = dedent("""
        rootLogger.level = warn
        rootLogger.appenderRef.stdout.ref = console

        appender.console.type = Console
        appender.console.name = console
        appender.console.target = SYSTEM_ERR
        appender.console.layout.type = PatternLayout
        appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex

        logger.nativecodeloader.name = org.apache.hadoop.util.NativeCodeLoader
        logger.nativecodeloader.level = error
    """).strip()

    log4j_path = Path(tempfile.gettempdir()) / "hps-lab-log4j2.properties"
    log4j_path.write_text(log4j_props)
    return JAVA_HOME, log4j_path


@app.cell
def _(JAVA_HOME, log4j_path):
    from pyspark.sql import SparkSession

    log4j_opt = f"-Dlog4j2.configurationFile=file:{log4j_path}"

    spark = (
        SparkSession.builder
        .appName("high-performance-spark-lab")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", log4j_opt)
        .config("spark.executor.extraJavaOptions", log4j_opt)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark, JAVA_HOME
    return (spark,)


@app.cell(hide_code=True)
def _(mo, spark):
    sc = spark.sparkContext
    ui_url = sc.uiWebUrl or "(UI disabled)"
    mo.md(
        f"""
        **Spark version:** `{spark.version}`
        **Master:** `{sc.master}`
        **App ID:** `{sc.applicationId}`
        **Spark UI:** [{ui_url}]({ui_url})
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Basics of Schemas

    The **schema** — and the optimizer work it unlocks — is one of the core differences between Spark SQL and the core (RDD) API. Inspecting it is especially useful for DataFrames, because there's no templated static type to read off in source the way there is with RDDs or (in Scala/Java) `Dataset[T]`.

    Schemas are usually handled for you:

    - **inferred** when loading self-describing data (JSON, Parquet, …),
    - **computed** from the parent DataFrame plus the transformation being applied.

    But you can also build them by hand — to skip inference, pin down nullability, or document a contract.

    > **Python note.** PySpark has no `Dataset[T]` API; every Python entry point produces a `DataFrame`. The closest analog to a Scala `case class` is a `@dataclass`, but the runtime artifact is still a DataFrame.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 2.1. Inspecting a schema

    Two views, two audiences:

    - **`df.printSchema()`** — human-readable, indented tree. Use it interactively to *see* what you have, especially for JSON or anything where the layout isn't obvious from the first few rows.
    - **`df.schema`** — programmatic `StructType` you can introspect, pattern-match, or hand to ML pipeline transformers and other schema-aware code.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    A single JSON record with a nested array of structs — schema inferred at read time.
    """)
    return


@app.cell
def _(spark):
    import json

    raw_record = {
        "name": "mission",
        "otters": [
            {
                "id": 1,
                "zip": "94110",
                "species": "giant",
                "happy": True,
                "attributes": [0.4, 0.5],
            }
        ],
    }

    otter_json = json.dumps(raw_record)
    df_otters = spark.read.json(spark.sparkContext.parallelize([otter_json]))
    df_otters.printSchema()

    df_otters
    return (df_otters,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    `df.schema` returns a `StructType` made of `StructField`s. Each field carries a name, a `DataType`, and a `nullable` flag (plus optional metadata). `StructType` itself is a `DataType`, which is what lets schemas nest — same idea as a Scala case class containing other case classes.

    For comparison, the Scala definition of `StructField` itself:

    ```scala
    case class StructField(
        name: String,
        dataType: DataType,
        nullable: Boolean = true,
        metadata: Metadata = Metadata.empty)
    ```
    """)
    return


@app.cell
def _(df_otters):
    df_otters.schema
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 2.2. Building a schema by hand

    The same shape as above, written explicitly. This is what you reach for when you don't trust inference (heterogeneous JSON, missing fields, costly scans), or when you want to lock down nullability.
    """)
    return


@app.cell
def _(spark):
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    otter_schema = StructType(
        [
            StructField("name", StringType(), nullable=True),
            StructField(
                "otters",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType(), nullable=False),
                            StructField("zip", StringType(), nullable=True),
                            StructField("species", StringType(), nullable=True),
                            StructField("happy", BooleanType(), nullable=False),
                            StructField(
                                "attributes",
                                ArrayType(DoubleType(), containsNull=False),
                                nullable=True,
                            ),
                        ]
                    ),
                    containsNull=True,
                ),
                nullable=True,
            ),
        ]
    )

    df_explicit = spark.createDataFrame(
        [("toronto", [(1, "M1B 5K7", "giant", True, [0.1, 0.1])])],
        schema=otter_schema,
    )
    df_explicit.printSchema()
    return (otter_schema,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 2.3. `@dataclass` as the `case class` analog

    Python `@dataclass` declarations let you define record shapes in code, much like Scala case classes. PySpark can build a DataFrame from a list of dataclass instances directly; for nested fields it's safer to also pass the explicit `schema=` so inference doesn't have to climb generic type hints.

    For comparison, the same shape as Scala case classes:

    ```scala
    case class RawOtter(id: Long, zip: String, species: String,
                        happy: Boolean, attributes: Array[Double])
    case class OtterPlace(name: String, otters: Array[RawOtter])
    ```
    """)
    return


@app.cell
def _(otter_schema, spark):
    from dataclasses import asdict, dataclass

    @dataclass
    class RawOtter:
        id: int
        zip: str
        species: str
        happy: bool
        attributes: list[float]

    @dataclass
    class OtterPlace:
        name: str
        otters: list[RawOtter]

    damao = RawOtter(
        id=1, zip="M1B 5K7", species="giant", happy=True, attributes=[0.1, 0.1]
    )
    toronto = OtterPlace(name="toronto", otters=[damao])

    df_from_dc = spark.createDataFrame([asdict(toronto)], schema=otter_schema)
    df_from_dc.printSchema()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 2.4. Complex Spark SQL types

    | Python type   | Spark SQL type                                          | What it is                                                                                       | Example                                                                                                          |
    |---------------|---------------------------------------------------------|--------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
    | `list[T]`     | `ArrayType(elementType, containsNull)`                  | Array of one element type. `containsNull=True` if any element may be null.                       | `list[int]` → `ArrayType(IntegerType(), True)`                                                                    |
    | `dict[K, V]`  | `MapType(keyType, valueType, valueContainsNull)`        | Key/value map. `valueContainsNull=True` if any value may be null.                                | `dict[str, int]` → `MapType(StringType(), IntegerType(), True)`                                                  |
    | `@dataclass`  | `StructType([StructField, …])`                          | Named heterogeneous fields — the analog of a Scala `case class` or Java bean.                    | `Otter(name: str, age: int)` → `StructType([StructField("name", StringType()), StructField("age", IntegerType())])` |

    `StructType`s nest: a `StructField` can carry an `ArrayType` of `StructType`, a `MapType` whose value is a `StructType`, and so on — exactly how `otters` nests `RawOtter` above.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. DataFrame API

    The DataFrame API lets you work with structured data without registering temp views or hand-writing SQL strings — though both are still available, and we'll show below where the SQL-string form is actually nicer.

    Like RDDs, the API splits into **transformations** (lazy, return a new DataFrame) and **actions** (eager, force execution). The relational flavor is the point: instead of arbitrary `T => U` lambdas, you build expressions out of `Column` objects, and Catalyst can read those expressions to plan, push down, and prune.

    > **Partially lazy.** Transformations are lazy with respect to *data*, but **schema is computed eagerly** — selecting a non-existent column blows up at the `select(...)` call, not at `.show()`. That's a feature: you find typos earlier than in the RDD world.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.1. Eager schema in action

    A quick demo. The `select` call resolves column names against the schema *immediately*; no action is needed to surface the error.
    """)
    return


@app.cell
def _(df_otters):
    try:
        df_otters.select("definitely_not_a_column")
    except Exception as e:
        eager_error = (type(e).__name__, str(e).splitlines()[0])
    else:
        eager_error = ("no error", "")
    eager_error
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.2. A flat otter dataset for the rest of this section

    `df_otters` from the JSON example has a single row with a nested array of otters. To make filters and aggregations readable, we'll build a small **flat** DataFrame here — one otter per row — and use it for the remaining examples.
    """)
    return


@app.cell
def _(spark):
    from pyspark.sql.types import (
        ArrayType as _AT,
        BooleanType as _Bool,
        DoubleType as _Dbl,
        LongType as _Long,
        StringType as _Str,
        StructField as _F_,
        StructType as _S_,
    )

    flat_schema = _S_(
        [
            _F_("id", _Long(), False),
            _F_("zip", _Str(), True),
            _F_("species", _Str(), True),
            _F_("happy", _Bool(), False),
            _F_("attributes", _AT(_Dbl(), False), True),
        ]
    )

    otters_flat = spark.createDataFrame(
        [
            (1, "94110", "giant", True, [0.4, 0.5]),
            (2, "94110", "red", False, [0.7, 0.2]),
            (3, "10001", "giant", False, [0.1, 0.9]),
            (4, "M1B 5K7", "giant", True, [0.6, 0.3]),
            (5, "94110", "red", True, [0.5, 0.5]),
            (6, "94110", "giant", False, [0.2, 0.8]),
        ],
        schema=flat_schema,
    )
    otters_flat.show(truncate=False)
    return (otters_flat,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.3. Three ways to reference a column

    Three idiomatic forms — all return the same `Column` object, each has a different ergonomic sweet spot:

    | Form              | Best for                                                                  | Watch out for                                              |
    |-------------------|---------------------------------------------------------------------------|------------------------------------------------------------|
    | `df["happy"]`     | When the DataFrame is a clear, named handle. Works with reserved words.    | Verbose for long expressions.                              |
    | `F.col("happy")`  | Inside `select`/`withColumn` chains where `df` is anonymous or pipelined.  | The string is unchecked until eager schema resolution.     |
    | `df.happy`        | One-off shells & demos.                                                   | Breaks for reserved words / dotted names; collides with DataFrame methods like `.id`. |

    Rule of thumb: reach for **`F.col`** when chaining transformations and there's no clean `df` handle to dereference; reach for `df["c"]` when the DataFrame is a clear, named binding.
    """)
    return


@app.cell
def _(otters_flat):
    import pyspark.sql.functions as F

    via_index = otters_flat["happy"]
    via_F_col = F.col("happy")
    via_attr = otters_flat.happy

    same_kind = (
        type(via_index).__name__,
        type(via_F_col).__name__,
        type(via_attr).__name__,
    )
    same_results = (
        otters_flat.filter(via_index).count(),
        otters_flat.filter(via_F_col).count(),
        otters_flat.filter(via_attr).count(),
    )
    same_kind, same_results
    return (F,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.4. Filtering — the unhappy otters

    Three idiomatic spellings, pick by readability:
    """)
    return


@app.cell
def _(F, otters_flat):
    unhappy_via_negation = otters_flat.filter(~F.col("happy"))
    unhappy_via_eq = otters_flat.filter(F.col("happy") == False)  # noqa: E712
    unhappy_via_sql = otters_flat.filter("not happy")

    print("via ~ :", unhappy_via_negation.count())
    print("via == False:", unhappy_via_eq.count())
    print("via SQL string:", unhappy_via_sql.count())
    unhappy_via_negation.show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.5. Python operator gotchas

    Three pitfalls that bite every PySpark user at least once:

    1. **`not df["happy"]` raises `TypeError`.** A `Column` has no truth value, so Python's `not`/`and`/`or` keywords don't work. Use `~`, `&`, `|` instead.
    2. **Bitwise operators bind tighter than comparisons.** `df["a"] > 1 & df["b"] < 2` parses as `df["a"] > (1 & df["b"]) < 2` — silently wrong. Always wrap each comparison: `(df["a"] > 1) & (df["b"] < 2)`.
    3. **Chained comparisons don't work.** `0 < df["x"] < 10` calls `bool(...)` on the first comparison and raises. Write it out: `(F.col("x") > 0) & (F.col("x") < 10)`.

    The first one is worth feeling once.
    """)
    return


@app.cell
def _(F, otters_flat):
    try:
        bad = otters_flat.filter(not F.col("happy"))
    except Exception as e:
        gotcha = (type(e).__name__, str(e).splitlines()[0])
    else:
        gotcha = ("no error", "")
    gotcha
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.6. A complex filter — multi-column, with array indexing

    Combine boolean logic and nested-element access in one predicate. Note the parens-around-each-comparison rule from above is doing real work here.
    """)
    return


@app.cell
def _(F, otters_flat):
    happy_left_squishy = otters_flat.filter(
        F.col("happy") & (F.col("attributes")[0] > F.col("attributes")[1])
    )
    happy_left_squishy.show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.7. When SQL strings beat the Column DSL

    `filter` and `selectExpr` accept SQL strings. The Column DSL is great inside Python code; SQL strings shine when the predicate **isn't known at code-write time** — driven from config, a UI, or a rules table. Both go through the same Catalyst optimizer, so there's no perf difference.
    """)
    return


@app.cell
def _(otters_flat):
    config_predicate = "happy AND attributes[0] > attributes[1]"
    otters_flat.filter(config_predicate).show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.8. Reading the optimizer with `.explain()`

    The DSL story ("Catalyst can see your filter, so it can push down") is abstract until you see the plan. `.explain()` prints the parsed → analyzed → optimized → physical plan — and on real file-backed sources you'll see `PushedFilters` in the physical plan when the predicate makes it down to the scan.

    With our in-memory `LocalRelation` the scan is trivial, but the *optimized logical plan* still tells you what survived simplification.
    """)
    return


@app.cell
def _(F, otters_flat):
    otters_flat.filter(
        (F.col("zip") == "94110") & F.col("happy")
    ).explain(mode="extended")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.9. Built-in functions

    The DSL operators (`+`, `==`, `~`, `&`, …) cover row-level arithmetic and logic. Anything fancier — literals, arrays, math, strings, dates, JSON, hashing, windows — lives in `pyspark.sql.functions`, conventionally aliased to `F`.

    A few standards — literals, array constructors, NaN tests, basic math:
    """)
    return


@app.cell
def _(F, spark):
    spark.range(1).select(
        F.lit(1).alias("one"),
        F.array(F.lit(1), F.lit(2), F.lit(3)).alias("arr"),
        F.isnan(F.lit(float("nan"))).alias("is_nan"),
        (~F.lit(True)).alias("not_true"),
        F.abs(F.lit(-1)).alias("abs_neg1"),
        F.sqrt(F.lit(4.0)).alias("sqrt_4"),
        F.acos(F.lit(0.5)).alias("acos_half"),
    ).show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.10. Array functions

    These are some of the most useful functions when working with self-describing data (JSON, Parquet with arrays). `explode` in particular is what we'd use to flatten the original nested `df_otters` into something shaped like `otters_flat` — a far more common move in real pipelines than constructing the flat schema by hand.
    """)
    return


@app.cell
def _(F, otters_flat):
    otters_flat.select(
        F.col("id"),
        F.col("attributes"),
        F.array_contains(F.col("attributes"), 0.5).alias("has_half"),
        F.sort_array(F.col("attributes")).alias("sorted_attrs"),
    ).show(truncate=False)
    return


@app.cell
def _(F, otters_flat):
    otters_flat.select(
        F.col("id"),
        F.posexplode(F.col("attributes")).alias("idx", "attr"),
    ).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.11. Flattening nested columns

    Round-tripping the original nested JSON DataFrame into something flat with `explode` + struct projection. Far more idiomatic than reconstructing a flat schema by hand.
    """)
    return


@app.cell
def _(F, df_otters):
    otters_flat_from_json = (
        df_otters
        .select(F.col("name").alias("place"), F.explode("otters").alias("p"))
        .select(
            "place",
            F.col("p.id").alias("id"),
            F.col("p.zip").alias("zip"),
            F.col("p.species").alias("species"),
            F.col("p.happy").alias("happy"),
            F.col("p.attributes").alias("attributes"),
        )
    )
    otters_flat_from_json.show(truncate=False)
    otters_flat_from_json.printSchema()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.12. Operator & function quick reference (Python)

    The names that actually show up in Python code, paired with their `Column`-method aliases:

    | Python form                   | Method on `Column`         | Purpose                                                |
    |-------------------------------|----------------------------|--------------------------------------------------------|
    | `==`, `!=`                    | `eqNullSafe` for null-safe | Equality (regular `==` is null-unsafe)                 |
    | `<`, `<=`, `>`, `>=`          | `lt`, `leq`, `gt`, `geq`   | Comparisons                                            |
    | `&`, `\|`, `~`                | `and`, `or` / unary not    | Boolean — **always parenthesise comparisons**          |
    | `+`, `-`, `*`, `/`, `%`       | —                          | Arithmetic                                             |
    | `**`                          | `pow`                      | Power                                                  |
    | `col["key"]`, `col[i]`        | `getField`, `getItem`      | Access nested struct / array element                   |
    | `col.alias("x")`              | `alias`                    | Rename in `select` output                              |
    | `col.isNull()`, `col.isNotNull()` | —                      | Null tests                                             |
    | `col.startswith(...)`, `col.like(...)`, `col.rlike(...)` | — | String predicates                          |
    | `col.cast("double")`          | `cast`                     | Type coercion — explicit beats implicit                |

    And from `pyspark.sql.functions`:

    | Function                                     | Purpose                                                            |
    |----------------------------------------------|--------------------------------------------------------------------|
    | `F.lit(value)`                               | Wrap a Python literal as a `Column`                                |
    | `F.array(c1, c2, ...)`                       | Build an array column from same-typed columns                      |
    | `F.struct("a", "b")`                         | Build a struct column from named fields                            |
    | `F.isnan(c)`, `F.isnull(c)`                  | Special-value tests                                                |
    | `F.abs`, `F.sqrt`, `F.exp`, `F.log`, `F.acos`, … | Standard math                                                  |
    | `F.array_contains(c, v)`                     | Membership test — pushes down to Parquet on supported sources       |
    | `F.sort_array(c, asc=True)`                  | Sort an array column                                               |
    | `F.explode(c)` / `F.posexplode(c)`           | One row per element (with index for `posexplode`) — `O(N)` fan-out |
    | `F.expr("any SQL expression")`               | Escape hatch when the DSL can't say it cleanly                     |
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. select and withColumn

    Filtering thins rows out; `select` and `withColumn` give you new or updated columns. Both accept the same `Column` expression syntax used in `filter` — so anything you can put in a predicate, you can use to compute a value.

    Two idiomatic shapes:

    - **`df.select(...)`** — replace the projection wholesale. Best when you want to drop columns or reshape.
    - **`df.withColumn("name", expr)`** — add or overwrite a single column, keep everything else. Best when adding one derived column to a wide schema.

    > **Practical note.** `withColumn` runs schema analysis on every call. Adding ten columns with ten chained `withColumn`s is materially slower than one `select` listing all ten — and Catalyst can do less cross-column simplification. For three or more derived columns, prefer a single `select`.
    """)
    return


@app.cell
def _(F, df_otters):
    otter_info = (
        df_otters
        .select(F.explode("otters").alias("o"))
        .select(
            F.col("o.id").alias("id"),
            F.col("o.zip").alias("zip"),
            F.col("o.species").alias("species"),
            F.col("o.happy").alias("happy"),
            F.col("o.attributes").alias("attributes"),
            (F.col("o.attributes")[0] / F.col("o.attributes")[1]).alias("squishyness"),
        )
    )
    otter_info.show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 4.1. `withColumn` — additive form

    Same expression, applied additively to `otters_flat`. Notice the column comes out at the end and the rest of the schema is untouched.
    """)
    return


@app.cell
def _(F, otters_flat):
    otters_with_ratio = otters_flat.withColumn(
        "squishyness",
        F.col("attributes")[0] / F.col("attributes")[1],
    )
    otters_with_ratio.show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 4.2. Aliasing — taming auto-generated names

    Without `.alias(...)`, computed columns get auto-named like `(attributes[0] / attributes[1])`. After a few transforms those names become unreadable and impossible to reference later. Always alias derived columns at the point you compute them.
    """)
    return


@app.cell
def _(F, otters_flat):
    otters_flat.select(
        "id",
        F.col("attributes")[0] / F.col("attributes")[1],
    ).show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. when / otherwise

    Sometimes the cleanest way to express a derived column is if/else. `F.when(cond, value)` returns a partial expression you can chain with `.when(...)` for else-if branches and close with `.otherwise(default)`. Without an `otherwise`, unmatched rows get `null`.
    """)
    return


@app.cell
def _(F, otters_flat):
    encoded = otters_flat.select(
        "id",
        "species",
        (
            F.when(F.col("species") == "giant", 0)
            .when(F.col("species") == "red", 1)
            .otherwise(2)
        ).alias("species_code"),
    )
    encoded.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 6. Missing and noisy data

    Real data has nulls and `NaN`s. The DSL has dedicated tools so you don't end up sprinkling `if x is None` everywhere.

    | Need                                       | Reach for                                                |
    |--------------------------------------------|----------------------------------------------------------|
    | "this field has a value"                   | `col.isNull()`, `col.isNotNull()`                        |
    | "this is not a NaN"                        | `~F.isnan(col)`                                          |
    | First non-null across columns              | `F.coalesce(c1, c2, ...)`                                |
    | First non-NaN across columns               | `F.nanvl(c1, c2)` (binary; nest for more)                |
    | Drop rows with nulls in any/all/subset     | `df.na.drop(how="any" \| "all", subset=[...])`            |
    | Fill nulls with a default                  | `df.na.fill(value, subset=[...])`                        |
    | Replace one literal with another           | `df.na.replace(to_replace, value, subset=[...])`         |

    > **Tip.** Don't reach for `df.na.fill` reflexively — silently substituting a sentinel for missing data hides upstream problems. For analytics, often the right move is `na.drop(subset=[<key column>])` plus an explicit decision per column.
    """)
    return


@app.cell
def _(F, spark):
    noisy = spark.createDataFrame(
        [
            (1, None, 0.5, float("nan")),
            (2, 1.0, None, 1.0),
            (3, None, None, float("nan")),
            (4, 4.0, 4.5, 4.2),
        ],
        "id INT, sensor_a DOUBLE, sensor_b DOUBLE, sensor_c DOUBLE",
    )

    cleaned = noisy.select(
        "id",
        F.coalesce("sensor_a", "sensor_b", F.lit(0.0)).alias("primary_reading"),
        F.nanvl("sensor_c", F.lit(-1.0)).alias("c_or_default"),
    )
    cleaned.show()
    return (noisy,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 6.1. `df.na` shortcuts
    """)
    return


@app.cell
def _(noisy):
    noisy.na.drop(subset=["sensor_a"]).show()
    noisy.na.fill({"sensor_a": -1.0, "sensor_b": -1.0}).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 7. dropDuplicates

    `filter` decides per row in isolation. `dropDuplicates` needs to *compare* rows, which means a shuffle — much more expensive than `filter`. Two flavors:

    - `df.dropDuplicates()` — full-row uniqueness (same as `df.distinct()`).
    - `df.dropDuplicates(["id"])` — collapse to one row per `id`. **The retained row is non-deterministic** unless you pre-sort and use a windowed `row_number()` filter (shown later in the windowing section).
    """)
    return


@app.cell
def _(otters_flat):
    dups = otters_flat.union(otters_flat)
    print("with dupes:", dups.count())
    print("after dropDuplicates():", dups.dropDuplicates().count())
    print("after dropDuplicates(['zip']):", dups.dropDuplicates(["zip"]).count())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 8. Aggregates

    `groupBy` returns a `GroupedData` handle. Pick one of two paths:

    1. **Convenience reducers** — `.count()`, `.sum("c")`, `.avg("c")`, `.min("c")`, `.max("c")`. Compact when you need exactly one aggregate.
    2. **`agg(...)`** — pass any number of `F.<aggregate>(col).alias(...)` expressions. Use this whenever you need more than one aggregate, or want to control output column names.

    Both paths return a regular DataFrame, so you can keep chaining (`.orderBy`, `.filter`, …) on the result.

    > **Why `groupBy` is no longer scary.** On RDDs, `groupBy` famously materialised entire groups in memory. On DataFrames, Catalyst rewrites most aggregations into a partial-aggregate + shuffle + final-aggregate pipeline — the giant per-key list never gets built. Plan accordingly: aggregates are usually fine; `collect_list` over a wide group is still dangerous.
    """)
    return


@app.cell
def _(F, otters_flat):
    by_zip = otters_flat.groupBy("zip").agg(
        F.count("*").alias("n"),
        F.sum(F.col("happy").cast("int")).alias("n_happy"),
        F.avg(F.col("attributes")[0]).alias("avg_attr0"),
        F.min(F.col("attributes")[0]).alias("min_attr0"),
        F.max(F.col("attributes")[0]).alias("max_attr0"),
        F.stddev(F.col("attributes")[0]).alias("stddev_attr0"),
        F.countDistinct("species").alias("n_species"),
        F.approx_count_distinct("species", rsd=0.01).alias("approx_n_species"),
    )
    by_zip.orderBy("zip").show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 8.1. describe and summary

    For a quick look at numeric columns, `describe` gives count/mean/stddev/min/max. The newer `summary` is more flexible — pass any subset of `count`, `mean`, `stddev`, `min`, `max`, plus arbitrary percentiles like `"50%"`, `"95%"`.
    """)
    return


@app.cell
def _(F, otters_flat):
    enriched = otters_flat.withColumn("attr0", F.col("attributes")[0])
    enriched.describe("attr0").show()
    enriched.summary("count", "mean", "stddev", "50%", "95%", "max").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 8.2. Aggregate function quick reference

    | Function                                | Purpose                                                         |
    |-----------------------------------------|-----------------------------------------------------------------|
    | `F.count(c)` / `F.count("*")`           | Non-null count / row count                                      |
    | `F.countDistinct(*cols)`                | Exact distinct — `O(distinct elements)` memory                  |
    | `F.approx_count_distinct(c, rsd=0.05)`  | HyperLogLog estimate — constant memory, tunable error           |
    | `F.sum(c)` / `F.sumDistinct(c)`         | Sum / sum-of-distinct                                           |
    | `F.avg(c)` / `F.mean(c)`                | Mean (aliases)                                                  |
    | `F.min(c)` / `F.max(c)`                 | Bounds (any sortable type)                                      |
    | `F.stddev(c)` / `F.stddev_pop(c)`       | Sample / population standard deviation                          |
    | `F.first(c, ignorenulls=True)` / `F.last(c, ...)` | Pick a representative — order-dependent, often misused |
    | `F.collect_list(c)` / `F.collect_set(c)` | Materialize the whole group — dangerous on wide groups         |

    For multi-dimensional summaries with subtotals, swap `groupBy(...)` for `rollup(...)` or `cube(...)` — covered in the next section.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 9. Rollup and cube

    `groupBy(a, b)` gives you one row per `(a, b)` combination — that's it. Dashboards usually want **subtotals too**: totals per `a`, per `b`, and a grand total. You can union three separate aggregates, or use `rollup` / `cube` to compute all of them in a single shuffle.

    | Operator                   | Groups produced (for `rollup/cube(a, b)`)                        | Mental model                |
    |----------------------------|------------------------------------------------------------------|-----------------------------|
    | `groupBy(a, b)`            | `(a, b)`                                                          | Just the leaves             |
    | `rollup(a, b)`             | `(a, b)`, `(a, NULL)`, `(NULL, NULL)`                            | Hierarchical: drill down `a → b` plus grand total |
    | `cube(a, b)`               | `(a, b)`, `(a, NULL)`, `(NULL, b)`, `(NULL, NULL)`               | All subsets — every "slice" |

    The `NULL`s in subtotal rows are real nulls in the result — that's how you tell a subtotal from a real group with a missing dimension. `F.grouping_id()` (or `F.grouping(col)`) lets you label which level a row represents when you can't trust the nulls.
    """)
    return


@app.cell
def _(F, otters_flat):
    otters_flat.rollup("zip", "species").agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.col("happy").cast("int")).alias("n_happy"),
    ).orderBy("zip", "species").show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Reading the result: rows where `species IS NULL` are the per-zip subtotals; the row where both are null is the grand total. Rolling up `(zip, species)` is hierarchical — you don't get totals per species across zips, because that would require dropping the *outer* dimension.

    Switch to `cube` and you do:
    """)
    return


@app.cell
def _(F, otters_flat):
    otters_flat.cube("zip", "species").agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.col("happy").cast("int")).alias("n_happy"),
    ).orderBy("zip", "species").show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 9.1. grouping_id

    `F.grouping_id(*cols)` returns a bitmap that tells you, per row, which of the grouping columns were aggregated away. For `cube(zip, species)`:

    - `0b00` = `0` — both columns present (a leaf row)
    - `0b01` = `1` — `species` rolled up
    - `0b10` = `2` — `zip` rolled up
    - `0b11` = `3` — grand total

    Useful for filtering ("just give me the per-zip subtotals") or for labelling rows in a dashboard without depending on `IS NULL` checks.
    """)
    return


@app.cell
def _(F, otters_flat):
    otters_flat.cube("zip", "species").agg(
        F.grouping_id().alias("level"),
        F.count(F.lit(1)).alias("n"),
    ).orderBy("level", "zip", "species").show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 10. Pivot and unpivot

    `pivot` turns a column's distinct values into new columns, often dramatically reshaping the result. It's expensive — Spark needs to either know the pivot values up front (cheap) or scan once to discover them (extra pass), then aggregate per pivoted column.

    > **Always pass the pivot values explicitly when you know them.** It saves a pre-scan and bounds the output schema.
    """)
    return


@app.cell
def _(F, otters_flat):
    otters_flat.groupBy("zip").pivot("species", ["giant", "red"]).agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.col("happy").cast("int")).alias("n_happy"),
    ).orderBy("zip").show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 10.1. Unpivot via `melt`

    Spark 3.4+ exposes `DataFrame.melt(ids, values, variableColumnName, valueColumnName)` for the inverse: collapse columns back into rows.
    """)
    return


@app.cell
def _(F, otters_flat):
    wide = otters_flat.select(
        "id",
        F.col("attributes")[0].alias("attr0"),
        F.col("attributes")[1].alias("attr1"),
    )
    wide.show()

    long_form = wide.melt(
        ids=["id"],
        values=["attr0", "attr1"],
        variableColumnName="attr_name",
        valueColumnName="attr_value",
    )
    long_form.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 11. Windowing

    Aggregates collapse a group into a single row. **Window functions** keep one row per input but compute a value over a *frame* of related rows — the canonical use cases are running totals, rankings, moving averages, lag/lead diffs.

    A window spec has three pieces:

    1. **`partitionBy`** — like a `groupBy`, but the rows aren't collapsed. **Always set this** unless you really want a single-machine global window.
    2. **`orderBy`** — defines the within-partition order. Required for `lag`, `lead`, ranking; optional but usual for frames.
    3. **Frame** — `rowsBetween(start, end)` for row-count frames or `rangeBetween(start, end)` for value-range frames. Defaults vary by function; be explicit.

    > **Warning.** A window without `partitionBy` shuffles all data onto a single executor. That's fine for thousands of rows; it's a job-killer for billions.
    """)
    return


@app.cell
def _(F, otters_flat):
    from pyspark.sql.window import Window

    win = (
        Window
        .partitionBy("zip")
        .orderBy(F.col("attributes")[0])
        .rowsBetween(-2, 2)
    )

    # Within each `zip`, sort otters by `attr0` and report per row the average `attr0` of that row's local 5-otter neighborhood (itself plus the two nearest below and above in `attr0`-rank) and how much this otter's `attr0` deviates from that neighborhood average.
    otters_flat.select(
        "id",
        "zip",
        F.col("attributes")[0].alias("attr0"),
        F.avg(F.col("attributes")[0]).over(win).alias("rolling_avg"),
        (F.col("attributes")[0] - F.avg(F.col("attributes")[0]).over(win)).alias(
            "delta_from_window_avg"
        ),
    ).orderBy("zip", "id").show(truncate=False)
    return (Window,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 11.1. Ranking via window

    `row_number`, `rank`, and `dense_rank` are window-only. They're also the answer to *deterministic* `dropDuplicates`: rank rows within an `id` partition by the column you care about, then keep `row_number == 1`.
    """)
    return


@app.cell
def _(F, Window, otters_flat):
    rank_win = Window.partitionBy("zip").orderBy(F.col("attributes")[0].desc())

    # ithin each zip, keep only the otter with the highest attr0 — a deterministic stand-in for dropDuplicates(["zip"]).
    otters_flat.withColumn("rk", F.row_number().over(rank_win)).filter("rk = 1").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 12. Sorting and limiting

    `orderBy` (alias: `sort`) takes any number of `Column.asc()` / `Column.desc()` expressions and is stable. `nullsFirst()` / `nullsLast()` are explicit options on the column when nulls matter for ranking.

    `limit(n)` is a transformation — it returns a DataFrame and you can keep chaining. `take(n)` and `head(n)` are actions — they pull rows back to the driver immediately.
    """)
    return


@app.cell
def _(F, otters_flat):
    otters_flat.orderBy(
        F.col("zip").asc(),
        F.col("attributes")[0].desc_nulls_last(),
    ).limit(3).show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 13. Set operations

    These take two same-shape DataFrames and combine them. Cost varies wildly — pick by intent:

    | Op                                    | Cost      | Notes                                                                |
    |---------------------------------------|-----------|----------------------------------------------------------------------|
    | `a.union(b)` / `a.unionAll(b)`        | Cheap     | Position-based concat — schemas must align column-by-column          |
    | `a.unionByName(b, allowMissingColumns=False)` | Cheap | Name-based concat — safer than `union` when schemas evolve            |
    | `a.intersect(b)` / `a.intersectAll(b)` | Expensive | Requires shuffle + dedup; `*All` keeps multiplicities                |
    | `a.exceptAll(b)`                      | Expensive | Anti-set — same shuffle cost as intersect                            |
    | `df.distinct()`                        | Expensive | Full-row dedup; equivalent to `df.dropDuplicates()`                  |

    > **Prefer `unionByName`.** Position-based `union` silently produces nonsense if column orders drift. The name-based variant fails loudly instead, which is what you want.
    """)
    return


@app.cell
def _(F, otters_flat):
    in_94110 = otters_flat.filter(F.col("zip") == "94110").select("id", "species")
    happy_only = otters_flat.filter(F.col("happy")).select("id", "species")

    print("union (with duplicates):", in_94110.union(happy_only).count())
    print("unionByName:           ", in_94110.unionByName(happy_only).count())
    print("intersect:             ", in_94110.intersect(happy_only).count())
    print("exceptAll:             ", in_94110.exceptAll(happy_only).count())
    print("distinct on union:     ", in_94110.union(happy_only).distinct().count())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 14. Plain SQL

    Anything you can express with the DataFrame DSL, you can express in SQL — and vice-versa. The two go through the same Catalyst optimizer, so the choice is purely ergonomic.

    Three ways to use SQL strings:

    1. **`createOrReplaceTempView` + `spark.sql`** — register a DataFrame under a name, then write full queries against it. Best when the logic is naturally relational (joins, sub-queries, CTEs).
    2. **`selectExpr` / `expr`** — drop SQL fragments inside a DSL pipeline. Best when one expression is awkward to write in DSL but you don't want to leave the chain.
    3. **Direct file queries** — `SELECT * FROM parquet.\`/path/...\`` style — no view registration needed when you just want a one-off scan over a file/folder.
    """)
    return


@app.cell
def _(otters_flat, spark):
    otters_flat.createOrReplaceTempView("otters")
    spark.sql(
        """
        SELECT zip,
               COUNT(*)                                AS n,
               SUM(CAST(happy AS INT))                 AS n_happy,
               AVG(attributes[0])                      AS avg_attr0
          FROM otters
         GROUP BY zip
         ORDER BY zip
        """
    ).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 14.1. `selectExpr` and `expr`

    `selectExpr` is `select` whose arguments are SQL strings. `F.expr("…")` does the same inside any DSL call site that takes a `Column`.
    """)
    return


@app.cell
def _(F, otters_flat):
    otters_flat.selectExpr(
        "id",
        "zip",
        "attributes[0] AS attr0",
        "CASE WHEN happy THEN 1 ELSE 0 END AS happy_int",
    ).show()

    otters_flat.select(
        "id",
        F.expr("attributes[0] + attributes[1] AS total_attr"),
    ).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 14.2. Querying file paths directly

    For one-off scans, you don't even need a view — point SQL at a file/folder via the file format alias:

    ```python
    spark.sql("SELECT * FROM parquet.`/path/to/data`")
    spark.sql("SELECT * FROM json.`/path/to/dir`")
    spark.sql("SELECT * FROM delta.`/path/to/table`")
    ```

    For long-lived datasets you'd usually register a managed or external table in a catalog (Hive, Iceberg, Unity, Polaris, Delta) and query it by its qualified name (`db.schema.table`). The DataFrame returned is identical regardless of the source — temp view, file path, or catalog table.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 15. I/O

    Spark SQL has its own I/O layer — `DataFrameReader` (`spark.read`) and `DataFrameWriter` (`df.write`) — separate from the core `SparkContext` file APIs. The reason is pushdown: if the optimizer understands what format and predicate you have, it can ask the storage layer to skip reading unnecessary rows or columns before any data enters the JVM. That contract only works if both sides speak the same language.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 15.1. JSON

    JSON carries no schema contract, so Spark samples records at read time to infer one. The default samples a fraction — enough to be fast, not enough to be certain on heterogeneous files.

    - `option("samplingRatio", "1.0")` forces Spark to examine every record. Correct for tricky files, but a full extra pass.
    - Providing an explicit schema skips inference entirely — faster, and makes the contract visible in code.

    > **Production rule.** In exploratory work, let Spark infer. In production, pin a schema. A new upstream field should be a deliberate decision, not a surprise that silently widens your schema and breaks downstream consumers.
    """)
    return


@app.cell
def _():
    import json as _json
    import tempfile as _tmp_json
    from pathlib import Path as _PJson

    _records = [
        {"id": 1, "zip": "94110",   "species": "giant", "happy": True,  "score": 0.4},
        {"id": 2, "zip": "94110",   "species": "red",   "happy": False, "score": 0.7},
        {"id": 3, "zip": "10001",   "species": "giant", "happy": False},   # score absent — tests inference
        {"id": 4, "zip": "M1B 5K7", "species": "giant", "happy": True,  "score": 0.6},
    ]

    _json_dir = _PJson(_tmp_json.mkdtemp()) / "otters_json"
    _json_dir.mkdir()
    (_json_dir / "part0.json").write_text("\n".join(_json.dumps(r) for r in _records))

    json_path = str(_json_dir)
    return (json_path,)


@app.cell
def _(json_path, spark):
    # samplingRatio=1.0 — all four records read, `score` correctly inferred as nullable double
    df_json = (
        spark.read
        .format("json")
        .option("samplingRatio", "1.0")
        .load(json_path)
    )
    df_json.printSchema()
    df_json.show()
    return (df_json,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 15.2. Parquet

    Parquet is Spark SQL's home format: columnar, splittable, self-describing, and understood by nearly every query engine in the data stack. A few options that matter in practice:

    | Config / option | Default | What it does |
    |---|---|---|
    | `spark.sql.parquet.binaryAsString` | `false` | Treat binary columns as strings — set when reading files from older Spark or Impala |
    | `mergeSchema` | `false` | Union schemas across partition files — useful when files were added over time with evolving columns |
    | `spark.sql.parquet.compression.codec` | `gzip` | Codec per write. `snappy` decompresses faster; `zstd` often wins on both size and speed on modern hardware |
    | `spark.sql.parquet.filterPushdown` | `true` | Let predicates reach the scan — skips entire row groups without reading them. Almost never turn this off |

    `df.write.format("parquet").save(path)` is the whole API for the common case.
    """)
    return


@app.cell
def _(df_json):
    import tempfile as _tmp_pq
    from pathlib import Path as _PPq

    parquet_path = str(_PPq(_tmp_pq.mkdtemp()) / "otters_parquet")
    df_json.write.format("parquet").option("compression", "snappy").save(parquet_path)
    parquet_path
    return (parquet_path,)


@app.cell
def _(parquet_path, spark):
    df_parquet = spark.read.format("parquet").load(parquet_path)
    df_parquet.printSchema()
    df_parquet.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 15.3. Save modes

    By default, writing to an existing path raises an exception — the same behaviour as core Spark's RDD writes. Specify a mode to change that:

    | Mode | Behavior |
    |---|---|
    | `errorIfExists` | Throws if the target already exists. The safe default — forces an explicit decision |
    | `append` | Adds new files alongside existing ones. The partition layout grows; no existing files are touched |
    | `overwrite` | Replaces existing content. With `spark.sql.sources.partitionOverwriteMode = dynamic`, only the affected partitions are replaced rather than the whole table |
    | `ignore` | Silently skips the write if the target already exists — useful for idempotent pipelines |

    The API: `df.write.mode("append").format("parquet").save(path)`.
    """)
    return


@app.cell
def _(parquet_path, spark):
    extra = spark.createDataFrame(
        [(5, "94110", "red", True, 0.9), (6, "10001", "giant", False, 0.1)],
        "id LONG, zip STRING, species STRING, happy BOOLEAN, score DOUBLE",
    )
    extra.write.mode("append").format("parquet").save(parquet_path)

    # Six rows now — four originals plus two appended
    spark.read.format("parquet").load(parquet_path).orderBy("id").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 15.4. Partitioned writes

    When you know your downstream readers will filter by a column, write the data partitioned on that column. Spark creates one subdirectory per distinct value (`zip=94110/`, `zip=10001/`, …), and any query with a `WHERE zip = '94110'` predicate will skip opening files in every other directory — **partition pruning**, and it is completely free.

    At read time, point to the root and Spark discovers the partitions automatically. The partition column reappears in the schema without you listing it explicitly.

    > **Watch out for small files.** A partition per-zip with six rows is a toy example. In production, a high-cardinality key like `user_id` produces millions of tiny files — slow to list, slow to open. A rule of thumb: aim for partition files between 128 MB and 1 GB, and keep the number of distinct partition values in the low thousands.
    """)
    return


@app.cell
def _(spark):
    import tempfile as _tmp_part
    from pathlib import Path as _PPart

    partitioned_path = str(_PPart(_tmp_part.mkdtemp()) / "otters_by_zip")

    _full = spark.createDataFrame(
        [
            (1, "94110",   "giant", True,  0.4),
            (2, "94110",   "red",   False, 0.7),
            (3, "10001",   "giant", False, 0.1),
            (4, "M1B 5K7", "giant", True,  0.6),
            (5, "94110",   "red",   True,  0.9),
            (6, "10001",   "giant", False, 0.2),
        ],
        "id INT, zip STRING, species STRING, happy BOOLEAN, score DOUBLE",
    )
    _full.write.partitionBy("zip").format("parquet").save(partitioned_path)

    # Partition column `zip` reappears automatically in the schema at read time
    df_partitioned = spark.read.format("parquet").load(partitioned_path)
    df_partitioned.printSchema()
    df_partitioned.orderBy("id").show(truncate=False)
    return (df_partitioned,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 15.5. Partition pruning

    Filter on the partition column and look for `PartitionFilters` in the `FileScan` line of the physical plan. Parquet files outside the matching partition directories are never opened.
    """)
    return


@app.cell
def _(F, df_partitioned):
    df_partitioned.filter(
        (F.col("zip") == "94110") & (F.col("score") > 0.5)
    ).explain(mode="simple")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 15.6. Local collections and RDDs

    `spark.createDataFrame(rows, schema)` distributes in-memory data as a DataFrame — the same pattern used throughout this notebook for sample data. Useful for unit tests, reference tables, and small lookup datasets that need to join against large distributed data.

    Going the other way — `.rdd` on a DataFrame — gives you an `RDD[Row]`. The data is converted from Spark SQL's internal Tungsten encoding into Row objects, so `.rdd` is a real conversion, not a view. Prefer staying in the DataFrame API; reach for `.rdd` only when you need something the DataFrame API genuinely can't express — cutting the optimizer's lineage for iterative algorithms is one legitimate case.
    """)
    return


@app.cell
def _(spark):
    from pyspark.sql.types import (
        ArrayType as _AT2,
        BooleanType as _Bool2,
        DoubleType as _Dbl2,
        LongType as _Long2,
        StringType as _Str2,
        StructField as _F2,
        StructType as _S2,
    )

    _schema = _S2([
        _F2("id",         _Long2(),             False),
        _F2("zip",        _Str2(),              True),
        _F2("species",    _Str2(),              True),
        _F2("happy",      _Bool2(),             False),
        _F2("attributes", _AT2(_Dbl2(), False), True),
    ])

    df_local = spark.createDataFrame(
        [(1, "94110", "giant", True, [0.4, 0.5]),
         (2, "94110", "red",  False, [0.7, 0.2])],
        schema=_schema,
    )
    df_local.show()

    # .rdd converts to RDD[Row] — each element is a pyspark.sql.Row
    first = df_local.rdd.first()
    print(first, "→ id:", first["id"], " species:", first["species"])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 16. UDFs

    When the built-in `pyspark.sql.functions` don't cover your case, you can register a Python function as a UDF and use it anywhere a `Column` expression is accepted.

    **The cost.** Python UDFs are a JVM → Python boundary crossing per row. Spark serializes each row, hands it to the Python process, your function runs, and the result is serialized back. For large datasets this can be 2–10× slower than an equivalent SQL expression.

    The remedies, in order of preference:

    1. Check if an existing `F.*` function covers it — they usually do.
    2. Use `F.expr("…")` with a SQL string — same optimizer, no boundary crossing.
    3. Use a **pandas UDF** (`@pandas_udf`) — operates on `pd.Series` batches via Arrow, far lower overhead than scalar UDFs.
    4. Write the UDF in Scala and register it for Python callers — zero serialization cost.

    > **Even with JVM languages, UDFs are generally slower than the equivalent SQL expression** because Catalyst cannot inspect their logic. A UDF is a black box; a SQL expression is a transparent tree the optimizer can push down, reorder, or fold.
    """)
    return


@app.cell
def _(F, otters_flat):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType as _DblUdf

    # Scalar UDF: ratio of attr0 to the sum of both attributes
    @udf(returnType=_DblUdf())
    def dominance(attrs):
        if attrs is None or len(attrs) < 2:
            return None
        total = attrs[0] + attrs[1]
        return attrs[0] / total if total > 0 else 0.0

    # Identical result expressed as a SQL expression — no UDF, no boundary crossing
    dominance_sql = F.col("attributes")[0] / (F.col("attributes")[0] + F.col("attributes")[1])

    otters_flat.select(
        "id",
        F.col("attributes"),
        dominance(F.col("attributes")).alias("dominance_udf"),
        dominance_sql.alias("dominance_sql"),
    ).show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 16.1. Pandas UDFs

    A pandas UDF receives a `pd.Series` (or `pd.DataFrame` for multi-column inputs) instead of one value at a time. Spark transfers data in Apache Arrow columnar batches, so the per-row serialization cost collapses into a per-batch cost. For compute-heavy custom logic, pandas UDFs are the right Python-native choice.

    ```python
    from pyspark.sql.functions import pandas_udf
    import pandas as pd

    @pandas_udf("double")
    def attr0_z_score(series: pd.Series) -> pd.Series:
        return (series - series.mean()) / series.std()

    otters_flat.withColumn(
        "attr0_z",
        attr0_z_score(F.col("attributes")[0])
    ).show()
    ```

    Requires `pyarrow` (`pip install pyarrow`). Python UDAFs that support partial aggregation are also available via `@pandas_udf` with `PandasUDFType.GROUPED_AGG` — the Python answer to Scala's `UserDefinedAggregateFunction`.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 17. Query Optimizer

    Every DataFrame transformation builds a **logical plan** — a tree of relational operators (Project, Filter, Join, Aggregate, …). Catalyst optimizes that tree in several passes before producing a **physical plan** that Spark actually executes.

    The stages, in order:

    1. **Unresolved logical plan** — column names and types are not yet validated.
    2. **Analyzed logical plan** — names resolved against the schema, types checked. This is where schema eagerness lives.
    3. **Optimized logical plan** — rule-based passes: filter pushdown, constant folding, operator collapsing, null simplification, predicate reordering, …
    4. **Physical plan(s)** — one or more candidate execution strategies (sort-merge join vs. broadcast join, etc.), selected by a cost model. AQE may revise the choice at runtime.

    `df.explain(mode=…)` exposes all four. `mode="simple"` prints only the physical plan; `mode="extended"` prints all four.
    """)
    return


@app.cell
def _(F, otters_flat):
    # A chain that gives Catalyst something to work with: filter, groupBy, post-agg filter
    _q = (
        otters_flat
        .filter(F.col("happy"))
        .groupBy("zip")
        .agg(F.avg(F.col("attributes")[0]).alias("avg_attr0"))
        .filter(F.col("avg_attr0") > 0.4)
    )
    _q.explain(mode="extended")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    What to look for:

    - **Optimized logical plan** — the `Filter(happy)` moved as close to the `LocalRelation` scan as possible (pre-aggregation filtering reduces the rows that reach the `groupBy`). The post-aggregation filter on `avg_attr0` stays above — correctly, because it depends on the aggregate result.
    - **Physical plan** — look for `HashAggregate` appearing *twice*: a partial aggregate on each partition (before the shuffle) and a final aggregate after. That's Catalyst rewriting a naïve group-then-reduce into a combiner pattern — the same transformation that makes `groupBy` safe on DataFrames but dangerous on RDDs.
    - **`PushedFilters`** — on file-backed sources (Parquet, ORC, Delta), predicates that can be evaluated at scan time appear here and let Spark skip entire row groups without reading them.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 17.1. Filter pushdown

    With a Parquet-backed source, predicates on primitive columns are pushed to the Parquet row-group statistics. The physical plan makes this concrete.
    """)
    return


@app.cell
def _(F, df_partitioned):
    df_partitioned.filter(
        (F.col("zip") == "94110") & (F.col("score") > 0.5)
    ).explain(mode="simple")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Two kinds of filters in the scan line:

    - **`PartitionFilters`** — directories ruled out by the partition key. Files in those directories are never opened, not even listed.
    - **`DataFilters` / `PushedFilters`** — predicates on non-partition columns evaluated against Parquet row-group min/max statistics. Spark may still re-check them row-by-row after reading (the "residual filter" step), but the row-group skip alone can eliminate substantial I/O.

    Both happen before any data enters the JVM. Writing well-chosen partition columns and keeping row-group statistics intact — don't over-compress or over-coalesce your Parquet — is the single highest-leverage I/O optimization for Parquet-heavy pipelines.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 17.2. Static optimizer rules

    Catalyst's static rules (the ones that run before the job starts, visible in `explain`) fall into three rough families:

    - **Pushdown / reordering** — move anything that reduces data size as early as possible. Filter pushdown is the canonical example; predicate pushdown through joins is another.
    - **Operator collapse** — merge adjacent compatible operators. Two consecutive `filter` calls collapse into one; two consecutive `select` calls are combined. This is why chaining transformations is essentially free — the optimizer flattens them.
    - **Simplification** — fold constants (`1 + 1 → 2`), eliminate dead branches (`CASE WHEN false THEN … → NULL`), strip away `IsNotNull` checks that are implied by a surrounding join.

    These run on the query plan without knowing data sizes. Anything that depends on runtime sizes is deferred to AQE.

    > **Don't outsource the obvious.** Catalyst is good but not omniscient. If you filter early, avoid wide `select *` where you need a handful of columns, and don't `collect_list` over unbounded groups — you will outperform a naïvely written query even before Catalyst gets involved.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 17.3. Adaptive Query Execution (AQE)

    AQE is Catalyst's runtime counterpart. Static rules run before the job starts; AQE runs *during* the job, re-optimizing based on what the data actually looks like at each shuffle boundary.

    The two highest-impact things AQE does:

    1. **Partition coalescing.** After a shuffle, if partitions are small, AQE merges adjacent ones. This eliminates the classic "too many small tasks" overhead that previously required manually tuning `spark.sql.shuffle.partitions`.
    2. **Join strategy switching.** If a join's "large" side turns out to be small enough to broadcast, AQE flips a planned sort-merge join to a broadcast join at runtime — even if the pre-execution statistics didn't predict it.

    AQE is on by default since Spark 3.2. You won't see it in `explain()` output because `explain()` is pre-execution. Run the job and check the Spark UI's SQL tab for the finalized adaptive plan.

    > **One known pitfall.** AQE can attempt to match output partitioning to a target table's partition layout. For skewed data, that undoes careful pre-shuffle work and can cause severe performance regressions. In Iceberg the escape hatch is setting the table property `write.distribution-mode = none` at write time.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 18. Joins

    Joining data is one of the most common — and most expensive — operations in a Spark pipeline. Understanding joins requires distinguishing two separate concepts that are easy to conflate:

    - **Join type** — the *logical* operation: inner, left outer, right outer, full outer, left semi, left anti. The join type determines *which rows appear in the result*.
    - **Join execution technique** — *how* Spark physically computes the join: broadcast hash, shuffle hash, shuffle sort-merge, etc. The technique determines *how much data moves over the network* and therefore the performance.

    The join type is fixed by your logic. The execution technique is chosen by Spark based on data sizes, known partitioners, configuration, and any hints you provide. The rest of this chapter walks through both dimensions.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 18.1. Core Spark Joins

    RDD-level joins require that records sharing a key end up on the same executor so they can be combined locally. If Spark does not already know how the RDDs are partitioned, it must shuffle at least one of them first.

    Three scenarios, ordered from most to least expensive:

    | Scenario | What happens | Network cost |
    |---|---|---|
    | Neither RDD has a known partitioner | Both sides shuffle — a **shuffle join** | Full cross-network sort |
    | One RDD has a known partitioner | The other shuffles to match it | One-sided shuffle |
    | Both share the same partitioner *and* were materialized in the same action | Data is already **co-located** on the same executor | Zero network transfer |

    > **Tip.** Two RDDs are co-located only when they share the same partitioner *and* were shuffled as part of the same action. Having the same partitioner without co-location still avoids a shuffle but not the network transfer.

    The cost of a join scales with the number of keys and the distance records must travel to reach their target partition. Keeping that product small is the central performance lever for RDD joins.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 18.2. RDD join patterns

    The default join in Spark is an **inner join** — only keys present in both RDDs appear in the result. When both sides have duplicates, the result is the full cross-product of matching records, which can cause dramatic data expansion.

    Three practical guidelines before you write a join:

    1. **Duplicate keys → reduce first.** If one or both RDDs contain multiple values per key, `reduceByKey` or `combineByKey` before the join. Joining first and reducing after means shuffling all the duplicates, only to throw most of them away. If the original RDD had *N* records per key, you shuffle *N* times more data than necessary.

    2. **Missing keys → prefer outer join.** An inner join silently drops rows whose key is absent from the other side. Using `leftOuterJoin` (or `rightOuterJoin` / `fullOuterJoin`) keeps those rows with `None` values, making the missing data visible rather than invisible.

    3. **Key subset → filter before joining.** If only a subset of the keys in the large RDD will ever match the small RDD, filter the large RDD down first. You reduce the shuffle volume for data you would discard anyway.

    > **Tip.** Join is one of the most expensive operations you will commonly use in Spark. Shrinking your data *before* the join — by reducing, filtering, or repartitioning — pays for itself immediately in reduced shuffle and memory pressure.
    """)
    return


@app.cell
def _(spark):
    # (otter_id, score) — three scores for otter 1, two for otter 2, one for otter 3
    score_rdd = spark.sparkContext.parallelize([
        (1, 0.4), (1, 0.9), (1, 0.2),
        (2, 0.7), (2, 0.3),
        (3, 0.6),
    ])

    # (otter_id, zip) — otter 4 has an address but no scores
    address_rdd = spark.sparkContext.parallelize([
        (1, "94110"),
        (2, "10001"),
        (4, "M1B 5K7"),
    ])
    return address_rdd, score_rdd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The book contrasts two implementations that produce the same result — the best score per otter paired with its address — but with very different shuffle costs.

    ```scala
    // Scala, Example 6-1 — join first, reduce after (expensive)
    val joinedRDD = scoreRDD.join(addressRDD)
    joinedRDD.reduceByKey((x, y) => if (x._1 > y._1) x else y)

    // Scala, Example 6-2 — reduce first, join after (cheap)
    val bestScoreData = scoreRDD.reduceByKey((x, y) => if (x > y) x else y)
    bestScoreData.join(addressRDD)
    ```

    The second approach shuffles only one row per key instead of all duplicates. If each otter had 1 000 scores the first shuffle would be 1 000× larger than necessary.
    """)
    return


@app.cell
def _(address_rdd, score_rdd):
    # Approach 1 — join then reduce: shuffles all duplicate score rows
    joined_first = score_rdd.join(address_rdd)
    best_via_join_first = joined_first.reduceByKey(lambda x, y: x if x[0] > y[0] else y)
    print("join-then-reduce:", sorted(best_via_join_first.collect()))

    # Approach 2 — reduce then join: only one row per key crosses the wire
    best_score_rdd = score_rdd.reduceByKey(lambda a, b: a if a > b else b)
    best_via_reduce_first = best_score_rdd.join(address_rdd)
    print("reduce-then-join:", sorted(best_via_reduce_first.collect()))
    # otter 3 has no address so it drops out of the inner join in both cases
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    `leftOuterJoin` guarantees every key from the left RDD appears in the result. When no matching key exists in the right RDD the value from the right side is `None` (Scala `Option` → Python `None`).

    ```scala
    // Scala, Example 6-3
    val joinedRDD = scoreRDD.leftOuterJoin(addressRDD)
    joinedRDD.reduceByKey((x, y) => if (x._1 > y._1) x else y)
    ```

    Spark also supports `rightOuterJoin` and `fullOuterJoin` depending on which side you need to preserve.
    """)
    return


@app.cell
def _(address_rdd, score_rdd):
    outer_joined = score_rdd.leftOuterJoin(address_rdd)
    # Each value is (score, Option[zip]) — None where address is absent
    # otter 3 has scores but no address: zip comes back as None
    print("left outer (score, zip?):")
    for k, v in sorted(outer_joined.collect()):
        print(f"  otter {k}: score={v[0]:.1f}, zip={v[1]}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 18.3. RDD execution strategies

    The default RDD join uses a **shuffle hash join**: Spark repartitions both RDDs with the same hash partitioner so records with the same key land on the same partition, then combines them locally. This always works but always costs at least one shuffle.

    Two ways to avoid or reduce that cost:

    **1. Assign a known partitioner before the join.** If you run an operation like `reduceByKey` *with an explicit partitioner* that matches the other RDD's partitioner, Spark recognises the shared partitioning and skips the second shuffle. The trick is to pass the partitioner as an argument to the pre-join aggregation:

    ```scala
    // Scala, Example 6-4
    val addressDataPartitioner = addressRDD.partitioner
      .getOrElse(new HashPartitioner(addressRDD.partitions.length))
    val bestScoreData = scoreRDD.reduceByKey(addressDataPartitioner, (x, y) => if (x > y) x else y)
    bestScoreData.join(addressRDD)
    ```

    **2. Broadcast hash join (manual in core Spark).** If one RDD fits in driver memory, collect it to a `Map`, broadcast that map to every executor, and combine with `mapPartitions` — zero shuffle. Covered in the next cell.
    """)
    return


@app.cell
def _(address_rdd, score_rdd):
    # Pass the same numPartitions so both RDDs use a hash partitioner with the
    # same number of buckets — Spark recognises the shared partitioning and
    # skips the second shuffle when joining.
    _n_parts = address_rdd.getNumPartitions()

    best_score_partitioned = score_rdd.reduceByKey(
        lambda a, b: a if a > b else b,
        numPartitions=_n_parts,
    )
    result_known_partitioner = best_score_partitioned.join(address_rdd)
    print("known-partitioner join:", sorted(result_known_partitioner.collect()))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### 18.3.1. Manual broadcast hash join

    Core Spark has no built-in broadcast join — but you can implement one manually. The pattern:

    1. Collect the smaller RDD to the driver as a Python `dict`.
    2. `sc.broadcast()` that dict so each executor holds exactly one copy.
    3. Use `mapPartitions` on the large RDD to look up each key in the broadcast variable — no shuffle at all.

    ```scala
    // Scala, Example 6-5
    def manualBroadcastHashJoin[K, V1, V2](bigRDD: RDD[(K, V1)], smallRDD: RDD[(K, V2)]) = {
      val smallLocal = smallRDD.collectAsMap()
      val bcast      = bigRDD.sparkContext.broadcast(smallLocal)
      bigRDD.mapPartitions(iter => iter.flatMap { case (k, v1) =>
        bcast.value.get(k).map(v2 => (k, (v1, v2)))
      }, preservesPartitioning = true)
    }
    ```

    The Python translation below mirrors the inner-join semantics: keys absent from the small RDD are dropped.
    """)
    return


@app.cell
def _(address_rdd, score_rdd, spark):
    # Collect the small RDD to the driver
    _small_local = dict(address_rdd.collectAsMap())

    # Broadcast: one copy per executor, not one per task
    _bcast = spark.sparkContext.broadcast(_small_local)

    def _broadcast_join(big_rdd, bcast_var):
        def _lookup(partition):
            lookup = bcast_var.value
            for k, v1 in partition:
                v2 = lookup.get(k)
                if v2 is not None:
                    yield (k, (v1, v2))
        return big_rdd.mapPartitions(_lookup, preservesPartitioning=True)

    bcast_result = _broadcast_join(score_rdd, _bcast)
    print("broadcast hash join:", sorted(bcast_result.collect()))
    # otter 3 dropped (no address); otter 4 dropped (no score) — inner semantics
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 18.4. Spark SQL Joins

    Spark SQL supports the same join types as core Spark, but the optimizer does more work for you automatically:

    - **Filter pushdown and reordering** — Catalyst can move predicates before a join, reducing the rows that enter the shuffle.
    - **Automatic broadcast** — if one side is below `spark.sql.autoBroadcastJoinThreshold` (default ~10 MB), Spark broadcasts it without any hint from you.

    The trade-off: you give up manual partitioner control. You cannot force co-location the way you can with RDDs, and you have less control over when shuffles happen.

    The examples below use two small DataFrames that mirror Tables 6-1 and 6-2 from the book — a table of otters and sizes (left) and a table of otters and zip codes (right).
    """)
    return


@app.cell
def _(spark):
    df_size = spark.createDataFrame(
        [("Happy", 1.0), ("Sad", 0.9), ("Happy", 1.5), ("Coffee", 3.0)],
        "name STRING, size DOUBLE",
    )
    df_zip = spark.createDataFrame(
        [("Happy", "94110"), ("Happy", "94103"), ("Coffee", "10504"), ("Tea", "07012")],
        "name STRING, zip STRING",
    )
    df_size.show()
    df_zip.show()
    return df_size, df_zip


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 18.5. Join types

    All six types share the same call signature: `df1.join(df2, condition, joinType)`.

    | Join type | `joinType` string | What appears in the result |
    |---|---|---|
    | Inner | `"inner"` (default) | Only rows where the key exists in **both** tables |
    | Left outer | `"left_outer"` or `"left"` | All rows from the left table; `null` fills missing right columns |
    | Right outer | `"right_outer"` or `"right"` | All rows from the right table; `null` fills missing left columns |
    | Full outer | `"full_outer"` or `"outer"` | All rows from both tables; `null` fills whichever side is absent |
    | Left semi | `"left_semi"` | Left rows that **have** a matching key in the right — no right columns |
    | Left anti | `"left_anti"` | Left rows that **do not** have a matching key in the right — no right columns |

    > **Warning.** Duplicate keys multiply rows. "Happy" appears twice in `df_size` and twice in `df_zip`, so the inner join produces 2 × 2 = 4 "Happy" rows. If you were not expecting that cross-product, deduplicate before joining.
    """)
    return


@app.cell
def _(df_size, df_zip):
    # Inner join — only keys present in both tables; Happy produces a cross-product
    print("=== inner ===")
    df_size.join(df_zip, df_size["name"] == df_zip["name"], "inner").show()
    return


@app.cell
def _(df_size, df_zip):
    _cond = df_size["name"] == df_zip["name"]

    # Left outer — Sad has no zip; its right columns come back null
    print("=== left outer ===")
    df_size.join(df_zip, _cond, "left_outer").show()

    # Right outer — Tea has no size; its left columns come back null
    print("=== right outer ===")
    df_size.join(df_zip, _cond, "right_outer").show()

    # Full outer — both Sad (no zip) and Tea (no size) are preserved
    print("=== full outer ===")
    df_size.join(df_zip, _cond, "full_outer").show()
    return


@app.cell
def _(df_size, df_zip):
    _cond = df_size["name"] == df_zip["name"]

    # Left semi — filter: keep left rows that have at least one match on the right
    # No right-table columns in the result
    print("=== left semi ===")
    df_size.join(df_zip, _cond, "left_semi").show()

    # Left anti — the complement: left rows with *no* match on the right
    print("=== left anti ===")
    df_size.join(df_zip, _cond, "left_anti").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 18.6. Self and cross joins

    **Self joins** join a DataFrame against itself — useful for finding pairs, hierarchies, or distances within a single table. The catch is that both sides share the same column names, so you must alias the DataFrame before joining; otherwise column references are ambiguous.

    ```scala
    // Scala, Example 6-10
    val joined = df.as("a").join(df.as("b")).where($"a.name" === $"b.name")
    ```

    **Cross joins** (cartesian product) pair every row on the left with every row on the right. A left table of *M* rows and a right table of *N* rows produces *M × N* rows in the result — with our four-row tables that is already 16 rows. On real datasets this explodes quickly.

    > **Warning.** Spark requires an explicit `crossJoin` call (or `joinType="cross"`) rather than accidentally omitting a condition, precisely because the data explosion is so easy to trigger and so hard to recover from downstream.
    """)
    return


@app.cell
def _(df_size, df_zip):
    import pyspark.sql.functions as _F

    # Self join — find all (size_a, size_b) pairs for the same name
    _self = (
        df_size.alias("a")
        .join(df_size.alias("b"), _F.col("a.name") == _F.col("b.name"))
        .select(_F.col("a.name"), _F.col("a.size").alias("size_a"), _F.col("b.size").alias("size_b"))
    )
    print("=== self join ===")
    _self.show()

    # Cross join — every row paired with every row
    _cross_count = df_size.crossJoin(df_zip).count()
    print(f"cross join row count: {df_size.count()} × {df_zip.count()} = {_cross_count}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 18.7. Equi vs non-equi joins

    Spark distributes join work by routing rows with the same key to the same partition. This only works when the join condition is an **equality** on the join key — an **equi-join**. Equality lets Spark use hashing or sorting to guarantee co-location.

    A **non-equi-join** (e.g., `>=`, `BETWEEN`, regex, or a UDF that itself makes the comparison) breaks that guarantee. Spark falls back to a nested-loop-style scan: every partition of one table is compared against every partition of the other. Cost grows as *O(M × N)* instead of *O(M + N)*.

    There is a subtle but important distinction with UDFs:

    - **Cheap (equi-join):** equality is on the *results* of a UDF — `udf_result(left) === udf_result(right)`. Spark applies the UDF per row and then uses the equality to route. Still an equi-join.
    - **Expensive (non-equi-join):** the UDF *is* the comparison — `udf_comparing(left_col, right_col)`. Spark cannot use the output to partition; it must compare all pairs.

    ```scala
    // Scala, Example 6-11 — bad: UDF is the comparison (non-equi)
    val sle = udf((s: String, s2: String) => s.length() == s2.length())
    df1.joinWith(df2, sle(df1("name"), df2("name")))

    // Scala, Example 6-12 — good: equality on UDF results (equi)
    val sl = udf((s: String) => s.length())
    df1.joinWith(df2, sl(df1("name")) === sl(df2("name")))
    ```
    """)
    return


@app.cell
def _(df_size, df_zip):
    from pyspark.sql.functions import udf as _udf
    from pyspark.sql.types import BooleanType as _BT, IntegerType as _IT

    _str_len = _udf(lambda s: len(s), _IT())

    # Good: equality on UDF results — Spark can hash on the result
    _good = df_size.join(df_zip, _str_len(df_size["name"]) == _str_len(df_zip["name"]))
    print("=== good join plan (equi on UDF results) ===")
    _good.explain()

    # Bad: UDF is the predicate — forces all-to-all comparison
    _str_len_eq = _udf(lambda a, b: len(a) == len(b), _BT())
    _bad = df_size.join(df_zip, _str_len_eq(df_size["name"], df_zip["name"]))
    print("=== bad join plan (UDF as predicate) ===")
    _bad.explain()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 18.8. Execution operators

    Spark SQL selects a physical join strategy based on data sizes, join type, join condition, and hints. Table 6-9 from the book, condensed:

    | Strategy | Join types | Conditions | Performance characteristic |
    |---|---|---|---|
    | **Broadcast hash** | All except full outer | equi-join | Fastest when one side is small; no shuffle |
    | **Broadcast nested loop** | All | equi and non-equi | Last resort — iterates one side repeatedly; OOM risk if broadcast side is large |
    | **Shuffle and replicate** (cartesian) | Inner, cross | equi and non-equi | Explodes data; produces narrow dependencies |
    | **Shuffle hash** | All | equi-join | Classic shuffle; requires one partition to fit in memory as a hash map |
    | **Shuffle sort-merge** | All | equi-join on sortable keys | Robust default for large tables; sort + merge avoids hash-map OOM |

    **Automatic broadcast** is controlled by `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). When Spark's size estimates say a table is under that threshold it broadcasts automatically. You can also force a broadcast with the `broadcast()` hint regardless of size.

    AQE adds a sixth strategy at runtime: it can **split skewed partitions** in shuffle hash or sort-merge joins, turning one oversized partition into several balanced ones without rerunning the entire job.

    > **Tip.** Call `.explain()` to see which strategy Spark chose. Look for `BroadcastHashJoin`, `SortMergeJoin`, or `BroadcastNestedLoopJoin` in the physical plan. When the plan is wrong — for example, Spark chose sort-merge but you know one side is tiny — add a `broadcast()` hint.
    """)
    return


@app.cell
def _(df_size, df_zip):
    import pyspark.sql.functions as _Fj

    # Without a hint — with tiny DataFrames Spark will already choose broadcast,
    # but on large tables this would default to SortMergeJoin
    print("=== no hint ===")
    df_size.join(df_zip, df_size["name"] == df_zip["name"]).explain()

    # Explicit broadcast hint — forces BroadcastHashJoin regardless of size estimates
    print("=== broadcast(df_zip) hint ===")
    df_size.join(_Fj.broadcast(df_zip), df_size["name"] == df_zip["name"]).explain()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 18.9. joinWith

    `Dataset.joinWith` is a Scala/Java-only API — it is not available on PySpark DataFrames. In Scala it returns a `Dataset[(LeftType, RightType)]` where each row is a strongly-typed tuple of both sides, preserving type information through the join.

    ```scala
    // Scala, Example 6-13
    val result: Dataset[(RawOtter, CoffeeShop)] =
      otters.joinWith(coffeeShops, otters("zip") === coffeeShops("zip"))

    // Scala, Example 6-14 — self join: each side keeps its own namespace
    val result: Dataset[(RawOtter, RawOtter)] =
      otters.as("l").joinWith(otters.as("r"), $"l.zip" === $"r.zip")
    ```

    In PySpark the closest equivalent is a regular `join` with `.alias()` to prevent column-name collisions, and then selecting the desired fields using the `alias.column` dot notation — which is exactly what the self-join example in 18.6 demonstrates.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---

    ## 19. Narrow vs Wide Transformations

    The book opens Ch. 7 with RDD internals. The same mechanics apply directly to DataFrames — Catalyst compiles your `filter`, `select`, and `join` calls into an RDD execution plan governed by the same rules.

    | Category | Definition | DataFrame examples |
    |---|---|---|
    | **Narrow** | Each output partition depends on exactly one input partition — no data moves between partitions | `filter`, `select`, `withColumn`, `coalesce` (reducing), `mapPartitions` |
    | **Wide** | Output partitions may depend on *many* input partitions — requires a shuffle | `groupBy().agg()`, `orderBy`, `join` (non-broadcast), `repartition`, `distinct` |

    Narrow steps inside one stage share a single pass over the data. Every wide transformation is a **stage boundary**: shuffle files are written to disk, data crosses the network, and the downstream stage cannot begin until the shuffle completes.

    > **Fault tolerance footnote.** Recovering a lost partition is cheap for narrow dependencies (one parent partition re-runs) and potentially very expensive for wide ones (all parent partitions may need to re-run). This is why persisting an RDD or DataFrame before a wide transformation can pay off when the upstream computation is costly.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 19.1. Reading stages in the query plan

    `df.explain()` prints the physical plan. Each `Exchange` node is a shuffle — a wide dependency boundary. Steps between two `Exchange` nodes belong to the same stage and share one data pass.
    """)
    return


@app.cell
def _(F, df_otters):
    # --- narrow only: filter + withColumn stay in one stage (no Exchange) ---
    _narrow_plan = df_otters.filter(F.col("name") != "Grumpy").withColumn("len", F.length("name"))
    print("=== narrow-only plan (no Exchange) ===")
    _narrow_plan.explain()

    # --- wide: groupBy forces a shuffle (Exchange present) ---
    _wide_plan = df_otters.groupBy("name").count()
    print("=== wide plan (Exchange present) ===")
    _wide_plan.explain()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 19.2. `coalesce` vs `repartition`

    Both change partition count, but they behave very differently:

    - **`coalesce(n)`** — narrow transformation. Merges existing partitions locally; no shuffle. The entire preceding stage runs at the *lower* parallelism level (Spark won't split tasks to fill more cores). Use it only when reducing partitions and you want to avoid the shuffle overhead.
    - **`repartition(n)`** — wide transformation. Full shuffle that produces exactly *n* balanced partitions. More expensive, but lets the upstream stage run at full parallelism before the shuffle.

    Rule of thumb: use `coalesce` when writing a small final output; use `repartition` when you need balanced partitions for downstream processing.
    """)
    return


@app.cell
def _(df_otters):
    print(f"original partitions : {df_otters.rdd.getNumPartitions()}")

    coalesced = df_otters.coalesce(1)
    print(f"after coalesce(1)   : {coalesced.rdd.getNumPartitions()}")
    print("coalesce plan (no Exchange):")
    coalesced.explain()

    repartitioned = df_otters.repartition(4)
    print(f"after repartition(4): {repartitioned.rdd.getNumPartitions()}")
    print("repartition plan (Exchange present):")
    repartitioned.explain()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 20. `mapPartitions` — the partition-level escape hatch

    `mapPartitions` is a narrow transformation that hands you an iterator of rows for one partition and expects an iterator back. Because it operates at the partition level rather than the row level, it is the right hook for:

    - **Per-partition setup**: creating a database connection, loading a ML model, or seeding a random number generator — once per partition instead of once per row.
    - **Complex state within a partition**: running totals, stateful parsers, sliding windows over sorted records.

    The book emphasises *iterator-to-iterator* discipline: return a generator or chained iterator rather than materialising the entire partition as a list. That lets Spark spill selectively to disk when a partition is too large for memory.

    > **Python note.** The Scala book warns about object-reuse and GC pressure from creating many short-lived JVM objects. In PySpark, `mapPartitions` runs Python code via Arrow or serialisation — so the JVM GC concern mostly disappears, but keeping the partition as a lazy iterator still avoids building a large in-memory list before Spark can write any rows.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 20.1. Per-partition setup — seeding one RNG per partition
    """)
    return


@app.cell
def _(spark):
    import random as _random
    from pyspark.sql.types import StructType as _ST20, StructField as _SF20, LongType as _LT20, DoubleType as _DT20
    from pyspark.sql import Row as _Row20

    _schema_sample = _ST20([
        _SF20("id", _LT20()),
        _SF20("sampled_score", _DT20()),
    ])

    _df_ids = spark.range(20)  # 20 rows, partitioned by default

    def _sample_partition(rows):
        rng = _random.Random()      # one RNG per partition — not per row
        for row in rows:
            if rng.random() < 0.5:  # 50 % sample
                yield (row.id, rng.gauss(0, 1))

    _sampled_rdd = (
        _df_ids.rdd
        .mapPartitions(_sample_partition)
        .map(lambda t: _Row20(id=t[0], sampled_score=t[1]))
    )
    spark.createDataFrame(_sampled_rdd, schema=_schema_sample).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 20.2. Iterator-to-iterator pattern — running totals without materialising

    The key discipline: never call `list(iterator)` or `len(iterator)` inside `mapPartitions`. Those traverse the iterator eagerly, loading the whole partition into memory. Instead, use generator expressions or chained iterators.

    Below: for each partition, emit only rows where the running word-count exceeds a threshold. The generator processes one row at a time; Spark can spill earlier rows to disk before the later ones are even read.
    """)
    return


@app.cell
def _(spark):
    from pyspark.sql import Row as _Row
    from pyspark.sql.types import StructType as _ST, StructField as _SF, StringType as _StrT, IntegerType as _IntT

    df_reports = spark.createDataFrame([
        ("Alice", "happy students learn well"),
        ("Alice", "the class was very happy today and students did great work"),
        ("Bob",   "students struggled today"),
        ("Bob",   "much better lesson happy to report progress now"),
        ("Bob",   "final happy review was excellent and everyone was very happy"),
    ], ["instructor", "text"])

    def count_happy_iter(rows):
        running = 0
        for row in rows:                        # one row at a time — iterator stays lazy
            words = row.text.split()
            happy = sum(1 for w in words if w.lower() == "happy")
            running += happy
            yield _Row(instructor=row.instructor, text=row.text, happy_so_far=running)

    schema_out = _ST([
        _SF("instructor", _StrT()),
        _SF("text", _StrT()),
        _SF("happy_so_far", _IntT()),
    ])

    result_df = spark.createDataFrame(
        df_reports.rdd.mapPartitions(count_happy_iter),
        schema=schema_out,
    )
    result_df.show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 21. Set Operations on DataFrames

    The book covers RDD set operations and notes several gotchas around duplicates. The same behaviour carries over to DataFrame APIs.

    | Operation | Behaviour | Duplicates in result |
    |---|---|---|
    | `union` / `unionByName` | Concatenates all rows — no deduplication | Yes — same as input |
    | `intersect` | Keys present in both; deduplicates the result | No |
    | `intersectAll` | Like SQL `INTERSECT ALL` — preserves duplicates | Yes |
    | `except` / `subtract` | Keys in left that are not in right; deduplicates | No |
    | `exceptAll` | Like SQL `EXCEPT ALL` — respects duplicate counts | Yes |

    > **Warning (from the book).** Because `intersect` and `subtract` deduplicate, you cannot reconstruct the original DataFrame as `intersect.union(subtract)`. The union of those two is always a *subset* of the original when the input has duplicates or the two sides share duplicate keys.
    """)
    return


@app.cell
def _(spark):
    from pyspark.sql import functions as _Fs

    df_a = spark.createDataFrame([(1,), (2,), (3,), (4,), (4,), (4,), (4,)], ["v"])
    df_b = spark.createDataFrame([(3,), (4,)], ["v"])

    print("=== union — all rows, duplicates preserved ===")
    df_a.union(df_b).show()

    print("=== intersect — values in both, deduplicated ===")
    df_a.intersect(df_b).show()

    print("=== intersectAll — values in both, duplicate counts respected ===")
    df_a.intersectAll(df_b).show()

    print("=== subtract / except — values in a not in b, deduplicated ===")
    df_a.subtract(df_b).show()

    print("=== exceptAll — subtract respecting duplicate counts ===")
    df_a.exceptAll(df_b).show()

    # Demonstrate the reconstruction asymmetry the book warns about
    ix = df_a.intersect(df_b)
    sub = df_a.subtract(df_b)
    reconstructed_count = ix.union(sub).count()
    original_count = df_a.count()
    print(f"original count:      {original_count}")
    print(f"reconstructed count: {reconstructed_count}  ← smaller due to deduplication")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 22. Broadcast Variables

    A broadcast variable ships a read-only copy of a Python object to every executor *once* rather than once per task. The book motivates this for filtering large RDDs against a small lookup table.

    In DataFrame work the most common form is the **broadcast join hint** (already covered in §18.8). But broadcast variables are also useful when you need a lookup dict or ML model inside a UDF — one serialised copy per executor instead of one per task invocation.

    | Approach | Cost | When to use |
    |---|---|---|
    | Closure capture (plain variable) | Serialised with every task | Small objects, infrequent use |
    | `spark.sparkContext.broadcast(obj)` | Serialised once per executor | Large dicts, repeated lookups across many tasks |
    | `broadcast()` join hint | Avoids shuffle for the small side | Small DataFrame joined to a large one |
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 22.1. Broadcasting a lookup dict inside a UDF
    """)
    return


@app.cell
def _(spark):
    from pyspark.sql.functions import udf as _udf
    from pyspark.sql.types import StringType as _StrT2

    # Simulate a "large" external lookup table
    zip_to_city = {
        "94107": "San Francisco",
        "94102": "San Francisco",
        "94110": "San Francisco",
        "10001": "New York",
    }

    # Broadcast it — one copy per executor, not one per task
    bc_lookup = spark.sparkContext.broadcast(zip_to_city)

    @_udf(returnType=_StrT2())
    def lookup_city(zip_code):
        return bc_lookup.value.get(zip_code, "Unknown")

    df_zips = spark.createDataFrame(
        [("Happy", "94107"), ("Sad", "94102"), ("Grumpy", "10001"), ("Lonely", "99999")],
        ["name", "zip"],
    )

    df_zips.withColumn("city", lookup_city("zip")).show()

    # Release when no longer needed
    bc_lookup.unpersist()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 22.2. Broadcast with a non-serialisable object — transient lazy val pattern

    Some objects (database connections, file handles) cannot be pickled and sent over the wire. In Scala the book uses `@transient lazy val` inside a broadcast wrapper so the object is reconstructed on the worker the first time it is needed, rather than serialised from the driver.

    The Python equivalent pattern: broadcast only a lightweight config dict; reconstruct the expensive object inside the UDF using a module-level dict as a lazy cache. The cache key keeps construction to once per worker process rather than once per row.

    > **Notebook caveat.** Marimo wraps each cell in an auto-named function. `functools.lru_cache` applied to a function defined *inside* a cell captures that mangled name, which cloudpickle cannot resolve in the Spark worker process. The pattern below uses an explicit `dict` cache instead — identical semantics, no name-mangling issue.
    """)
    return


@app.cell
def _(spark):
    import random as _rand2
    from pyspark.sql.functions import udf as _udf2
    from pyspark.sql.types import DoubleType as _DblT2

    # Broadcast just the seed — not the RNG object, which isn't picklable
    _bc_config = spark.sparkContext.broadcast({"seed": 42})

    # Module-level dict acts as the lazy cache; keyed by seed so reconstruction is once per process
    _rng_cache: dict = {}

    @_udf2(returnType=_DblT2())
    def noisy_score(v):
        seed = _bc_config.value["seed"]
        if seed not in _rng_cache:
            _rng_cache[seed] = _rand2.Random(seed)
        return float(v) + _rng_cache[seed].gauss(0, 0.1)

    spark.range(5).withColumn("noisy", noisy_score("id")).show()

    _bc_config.unpersist()
    return noisy_score


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 23. Accumulators

    Accumulators are the mirror of broadcast variables: workers write, the driver reads. They are useful for collecting side-channel metrics — record counts, parse error tallies, timing — without launching a separate action.

    **Caveats from the book:**
    - Spark may re-run a task on failure or speculative execution. The accumulator will be incremented again, so counts can be inflated.
    - Accumulators inside transformations are only guaranteed to be updated when an action forces evaluation. Calling `acc.value` before an action returns the accumulator's current (likely zero) value.
    - For large amounts of data (long strings, big collections) use a separate action instead.

    > **Best use:** process-level counters (bytes parsed, tasks completed) where double-counting on retry is acceptable. **Avoid:** business-critical counts where an exact number matters and retries are likely.
    """)
    return


@app.cell
def _(spark):
    # Built-in accumulators: LongAccumulator, DoubleAccumulator, CollectionAccumulator
    parse_errors = spark.sparkContext.accumulator(0)
    rows_seen    = spark.sparkContext.accumulator(0)

    df_raw = spark.createDataFrame([
        ("94107", "3.5"),
        ("94102", "not_a_number"),
        ("94110", "2.1"),
        ("bad_zip", "1.0"),
        ("10001", "4.8"),
    ], ["zip", "score_str"])

    from pyspark.sql.types import DoubleType as _DT2
    from pyspark.sql.functions import udf as _udf3

    @_udf3(returnType=_DT2())
    def parse_score(s):
        rows_seen.add(1)
        try:
            return float(s)
        except (ValueError, TypeError):
            parse_errors.add(1)
            return None

    # Accumulators update only when the action runs
    parsed = df_raw.withColumn("score", parse_score("score_str"))
    parsed.count()   # trigger evaluation

    print(f"rows seen:    {rows_seen.value}")
    print(f"parse errors: {parse_errors.value}")
    parsed.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 24. Caching and Persistence

    Spark does not cache intermediate DataFrames automatically. Each action re-executes the full lineage. Persisting materialises a DataFrame on the executors so subsequent actions can skip the recomputation.

    **When caching pays off:**
    - The same DataFrame is consumed by multiple actions or downstream stages.
    - The transformation chain upstream is expensive (complex joins, UDFs, wide aggregations).
    - An iterative algorithm reads the same base data many times.

    **When caching hurts:**
    - The DataFrame is used only once — caching adds write cost with no read benefit.
    - The cluster is memory-constrained — persisted data competes with execution memory.
    - The upstream computation is cheap (simple filter/select) — recomputing is faster than a cache read.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 24.1. Storage levels

    `cache()` is an alias for `persist()` with `MEMORY_AND_DISK` on DataFrames (note: for RDDs it defaults to `MEMORY_ONLY`). You can pass an explicit `StorageLevel`:

    | Storage level | Memory | Disk | Replicas | When to use |
    |---|---|---|---|---|
    | `MEMORY_ONLY` | Yes | No | 1 | Fits comfortably in memory; fast access |
    | `MEMORY_AND_DISK` | Yes | Overflow | 1 | Default; safe when size is uncertain |
    | `MEMORY_AND_DISK_DESER` | Yes | Overflow | 1 | Same as above with explicit deserialized flag |
    | `DISK_ONLY` | No | Yes | 1 | Recompute is more expensive than disk I/O |
    | `MEMORY_AND_DISK_2` | Yes | Overflow | 2 | Noisy cluster; fast recovery from single node loss |
    | `MEMORY_ONLY_2` | Yes | No | 2 | Small DataFrame; replicated for fast failover |
    | `OFF_HEAP` | Off-heap | — | 1 | Persistent GC pressure; requires Alluxio/Tachyon |

    > **PySpark 4 note.** The `_SER` / `_2_SER` variants from the book (e.g. `MEMORY_AND_DISK_SER`) no longer exist. In Python, all objects pass through pickle regardless, so the serialised/deserialised distinction from the JVM world does not map onto PySpark storage levels the same way.
    """)
    return


@app.cell
def _(spark):
    from pyspark import StorageLevel
    from pyspark.sql import functions as _Fc

    # Simulate an expensive upstream computation
    df_base = (
        spark.range(10_000)
        .withColumn("grp", (_Fc.col("id") % 10).cast("string"))
        .withColumn("val", _Fc.randn(42))
    )

    # cache() — default MEMORY_AND_DISK
    df_base.cache()
    df_base.count()   # materialise

    # Multiple downstream actions now skip recomputation
    print("mean val:", df_base.agg(_Fc.mean("val")).collect()[0][0])
    print("max val: ", df_base.agg(_Fc.max("val")).collect()[0][0])

    df_base.unpersist()   # release when done

    # Explicit storage level — MEMORY_AND_DISK_DESER is the closest to the book's
    # MEMORY_AND_DISK_SER; _SER variants were removed in PySpark 4
    df_base.persist(StorageLevel.MEMORY_AND_DISK_DESER)
    df_base.count()
    print("persisted (MEMORY_AND_DISK_DESER), partitions:", df_base.rdd.getNumPartitions())
    df_base.unpersist()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 24.2. Multiple actions on the same DataFrame — why persist matters

    Without a `cache()` call, each action re-executes the full lineage from scratch. The example below calls `count()` and then `take()` on a sorted DataFrame. Without caching, the sort runs twice.
    """)
    return


@app.cell
def _(spark):
    from pyspark.sql import functions as _Fd

    df_unsorted = spark.range(1_000).withColumn("val", _Fd.randn(0))

    # --- without cache: sort runs twice ---
    sorted_df = df_unsorted.orderBy("val")
    n = sorted_df.count()            # sort #1
    top10 = sorted_df.take(10)       # sort #2

    # --- with cache: sort runs once ---
    sorted_cached = df_unsorted.orderBy("val")
    sorted_cached.cache()
    n_c = sorted_cached.count()       # sort + cache write
    top10_c = sorted_cached.take(10)  # reads from cache

    print(f"count without cache: {n}   with cache: {n_c}")
    print(f"first cached row: {top10_c[0]}")
    sorted_cached.unpersist()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 24.3. Checkpointing

    `checkpoint()` writes the DataFrame to external storage (HDFS, S3, local FS in dev) and **breaks the lineage**. After a checkpoint, Spark treats the saved files as the source — it will not re-run upstream transformations if a partition is lost.

    Use checkpointing when:
    - The lineage is so long that task serialisation itself becomes slow or the DAG overflows driver memory.
    - You are running on a noisy cluster where executor loss mid-job is common and recomputing from scratch is prohibitive.
    - The book's rule of thumb: *persist when jobs are slow, checkpoint when they are failing*.

    > **Local checkpointing** (`localCheckpoint()`) truncates the lineage but stores data on the executor disks rather than external storage — faster, but lost if that executor dies. Not suitable for noisy clusters.
    """)
    return


@app.cell
def _(spark):
    import tempfile as _tf, os as _os

    ckpt_dir = _os.path.join(_tf.gettempdir(), "spark-ckpt-demo")
    spark.sparkContext.setCheckpointDir(ckpt_dir)

    from pyspark.sql import functions as _Fe

    df_long_lineage = spark.range(500).withColumn("v", _Fe.randn(1))

    # Simulate a long lineage with repeated narrow transforms
    for _ in range(5):
        df_long_lineage = df_long_lineage.withColumn("v", _Fe.col("v") * 1.01 + _Fe.randn(1) * 0.001)

    df_long_lineage.checkpoint()          # materialise to disk, break lineage
    print("is checkpointed:", df_long_lineage.isStreaming)  # not streaming

    # After checkpoint the physical plan is much shorter
    print("=== post-checkpoint plan ===")
    df_long_lineage.explain()

    df_long_lineage.count()
    return


if __name__ == "__main__":
    app.run()

