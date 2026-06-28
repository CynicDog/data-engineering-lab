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
    # DataFrames, Datasets, and Spark SQL

    Notes and runnable Python translations from *High Performance Spark*, Ch. 5.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Setup

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
    ## Basics of Schemas

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
    ### Inspecting a schema — `printSchema()` vs `.schema`

    Two views, two audiences:

    - **`df.printSchema()`** — human-readable, indented tree. Use it interactively to *see* what you have, especially for JSON or anything where the layout isn't obvious from the first few rows.
    - **`df.schema`** — programmatic `StructType` you can introspect, pattern-match, or hand to ML pipeline transformers and other schema-aware code.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Inferred from JSON

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
    #### Programmatic — `df.schema`

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
    ### Building a schema by hand

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
    ### `@dataclass` as the `case class` analog

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
    ### Complex Spark SQL types

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
    ## DataFrame API

    The DataFrame API lets you work with structured data without registering temp views or hand-writing SQL strings — though both are still available, and we'll show below where the SQL-string form is actually nicer.

    Like RDDs, the API splits into **transformations** (lazy, return a new DataFrame) and **actions** (eager, force execution). The relational flavor is the point: instead of arbitrary `T => U` lambdas, you build expressions out of `Column` objects, and Catalyst can read those expressions to plan, push down, and prune.

    > **Partially lazy.** Transformations are lazy with respect to *data*, but **schema is computed eagerly** — selecting a non-existent column blows up at the `select(...)` call, not at `.show()`. That's a feature: you find typos earlier than in the RDD world.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Eager schema in action

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
    ### A flat otter dataset for the rest of this section

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
    ### Three ways to reference a column

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
    ### Filtering — the unhappy otters

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
    ### Python operator gotchas

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
    ### A complex filter — multi-column, with array indexing

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
    ### When SQL strings beat the Column DSL

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
    ### Reading the optimizer with `.explain()`

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
    ### Built-in functions — `pyspark.sql.functions`

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
    ### Array functions — `array_contains`, `sort_array`, `explode`

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
    #### Bonus — flattening `df_otters` the way you actually would

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
    ### Operator & function quick reference (Python)

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
    ## Producing new columns — `select` and `withColumn`

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
    ### `withColumn` — additive form

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
    ### Aliasing — taming auto-generated names

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
    ## Conditional values — `when` / `otherwise`

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
    ## Missing and noisy data

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
    ### `df.na` shortcuts
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
    ## Beyond row-by-row — `dropDuplicates`

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
    ## Aggregates and `groupBy`

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
    ### Whole-DataFrame summaries — `describe` and `summary`

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
    ### Aggregate function quick reference

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
    ## Rollup and cube — subtotals in one pass

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
    ### Labelling subtotal rows with `grouping_id`

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
    ## Pivot and unpivot

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
    ### Unpivot via `melt`

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
    ## Windowing

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
    ### Ranking and deduplication via window

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
    ## Sorting and limiting

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
    ## Multi-DataFrame set-like operations

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
    ## Plain SQL and the catalog

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
    ### `selectExpr` and `expr`

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
    ### Querying file paths directly

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
    ## Data Loading and Saving

    Spark SQL has its own I/O layer — `DataFrameReader` (`spark.read`) and `DataFrameWriter` (`df.write`) — separate from the core `SparkContext` file APIs. The reason is pushdown: if the optimizer understands what format and predicate you have, it can ask the storage layer to skip reading unnecessary rows or columns before any data enters the JVM. That contract only works if both sides speak the same language.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### JSON — schema inference and `samplingRatio`

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
    ### Parquet — reading and writing

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
    ### Save modes

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
        "id INT, zip STRING, species STRING, happy BOOLEAN, score DOUBLE",
    )
    extra.write.mode("append").format("parquet").save(parquet_path)

    # Six rows now — four originals plus two appended
    spark.read.format("parquet").load(parquet_path).orderBy("id").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Partitioned writes — `partitionBy`

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
    #### Partition pruning in the physical plan

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
    ### From local collections and to RDDs

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
    ## UDFs — extending Spark SQL with Python functions

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
    ### Pandas UDFs — vectorized, much lower overhead

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
    ## Query Optimizer — Catalyst

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
    #### What to look for in the plan

    - **Optimized logical plan** — the `Filter(happy)` moved as close to the `LocalRelation` scan as possible (pre-aggregation filtering reduces the rows that reach the `groupBy`). The post-aggregation filter on `avg_attr0` stays above — correctly, because it depends on the aggregate result.
    - **Physical plan** — look for `HashAggregate` appearing *twice*: a partial aggregate on each partition (before the shuffle) and a final aggregate after. That's Catalyst rewriting a naïve group-then-reduce into a combiner pattern — the same transformation that makes `groupBy` safe on DataFrames but dangerous on RDDs.
    - **`PushedFilters`** — on file-backed sources (Parquet, ORC, Delta), predicates that can be evaluated at scan time appear here and let Spark skip entire row groups without reading them.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Filter pushdown — seeing it in the physical plan

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
    ### Static optimizer rules — the broad categories

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
    ### Adaptive Query Execution (AQE)

    AQE is Catalyst's runtime counterpart. Static rules run before the job starts; AQE runs *during* the job, re-optimizing based on what the data actually looks like at each shuffle boundary.

    The two highest-impact things AQE does:

    1. **Partition coalescing.** After a shuffle, if partitions are small, AQE merges adjacent ones. This eliminates the classic "too many small tasks" overhead that previously required manually tuning `spark.sql.shuffle.partitions`.
    2. **Join strategy switching.** If a join's "large" side turns out to be small enough to broadcast, AQE flips a planned sort-merge join to a broadcast join at runtime — even if the pre-execution statistics didn't predict it.

    AQE is on by default since Spark 3.2. You won't see it in `explain()` output because `explain()` is pre-execution. Run the job and check the Spark UI's SQL tab for the finalized adaptive plan.

    > **One known pitfall.** AQE can attempt to match output partitioning to a target table's partition layout. For skewed data, that undoes careful pre-shuffle work and can cause severe performance regressions. In Iceberg the escape hatch is setting the table property `write.distribution-mode = none` at write time.
    """)
    return


if __name__ == "__main__":
    app.run()
