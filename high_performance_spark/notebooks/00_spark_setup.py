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
    # Spark setup

    This notebook boots a local `SparkSession` against a pinned JDK 17 (Temurin).
    The shell default JDK on this machine is 25, which Spark 4 does not officially support — so we override `JAVA_HOME` *before* importing `pyspark`.
    """)
    return


@app.cell
def _():
    import os
    import tempfile
    from pathlib import Path
    from textwrap import dedent

    JAVA_HOME = Path(
        "/Users/eunsang/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home"
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


@app.cell
def _(spark):
    df = spark.range(0, 10).toDF("n")
    df.selectExpr("n", "n * n as n_squared").show()
    return


if __name__ == "__main__":
    app.run()
