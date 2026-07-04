import marimo

__generated_with = "0.23.13"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # HDFS, hands-on

    A tour of the Hadoop Distributed Filesystem (HDFS) against a **real, tiny
    cluster** — one namenode and three datanodes, started by the
    `docker-compose.yml` next to this notebook. Every command below actually
    runs; nothing is simulated. (Concepts and structure follow Chapter 3 of
    *Hadoop: The Definitive Guide*, White.)
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Why a distributed filesystem

    A single machine's disk has a hard ceiling: once a dataset outgrows it, the
    only way forward is to spread the data across a *network* of machines. The
    moment storage becomes network-based, all the usual headaches of network
    programming show up — and one in particular dominates the design of any
    distributed filesystem: **nodes fail**, routinely, and the filesystem has
    to keep working (and keep the data) anyway.

    HDFS is Hadoop's answer to this. It's built for:

    - **Very large files** — hundreds of megabytes up to terabytes; production
      clusters store petabytes.
    - **Streaming, write-once/read-many access** — a dataset is written once
      and then scanned in full, over and over, by different analyses. Total
      scan throughput matters far more than the latency of the first byte.
    - **Commodity hardware** — ordinary machines, not exotic, highly reliable
      ones. HDFS assumes failure is normal and designs around it rather than
      trying to prevent it.

    And it's a poor fit for:

    - **Low-latency access** (tens of milliseconds) — HDFS optimizes for
      throughput, not latency. HBase exists for this.
    - **Lots of small files** — the namenode keeps all filesystem metadata in
      RAM (~150 bytes per file/directory/block), so file *count*, not total
      bytes, is what limits a namenode.
    - **Multiple writers or in-place edits** — one writer per file, and only
      appends at the end. No random-offset writes.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. The lab cluster

    `docker-compose.yml` runs:

    | Service | Role | Notable config |
    |---|---|---|
    | `namenode` | filesystem metadata, block-location map | web UI + WebHDFS on `:9870` |
    | `datanode1/2/3` | actual block storage | 3 of them, so replication factor 3 is meaningful |

    Two settings are deliberately *not* production defaults, so the concepts
    below are visible without needing huge files or a long wait:

    - `dfs.blocksize = 1 MiB` (real default: 128 MiB) — a few-megabyte file is
      enough to see a file split across multiple blocks.
    - `dfs.namenode.heartbeat.recheck-interval` and `dfs.heartbeat.interval`
      are both shortened — dead-datanode detection is ~30 seconds here instead
      of the default ~10.5 minutes.

    Start it (from a terminal, once, before running this notebook):

    ```bash
    docker compose up -d
    ```
    """)
    return


@app.cell
def _():
    import subprocess

    NAMENODE = "hdfs-hatchery-namenode"

    def hdfs(*args: str) -> str:
        """Run a command inside the namenode container and return its stdout.

        This is the real `hadoop`/`hdfs` CLI, executed where it can resolve
        the cluster's internal Docker hostnames — see §7 for why that matters.
        """
        result = subprocess.run(
            ["docker", "exec", NAMENODE, *args],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        return result.stdout

    def docker_ctl(action: str, container: str) -> str:
        result = subprocess.run(
            ["docker", action, container], capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        return result.stdout.strip()

    return docker_ctl, hdfs, subprocess


@app.cell
def _(hdfs):
    # Sanity check: is the cluster up, and has the namenode left safe mode?
    print(hdfs("hdfs", "dfsadmin", "-report"))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Skim the report above: `Configured Capacity`, then a `Live datanodes (3)`
    section listing `datanode1`, `datanode2`, `datanode3` by their internal
    Docker hostnames and IPs. That list — which datanodes exist and whether
    they're alive — is exactly the state the namenode needs to answer "where
    are this file's blocks?"
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Blocks

    ### 3.1. Why blocks, and why so large

    A disk filesystem deals in blocks a few KB in size. HDFS blocks are
    enormous by comparison — 128 MiB by default — because the goal is to make
    **seek time negligible next to transfer time**. If a disk seeks in ~10 ms
    and transfers at ~100 MB/s, a block needs to be around 100 MB before the
    seek is only ~1% of the time spent reading it:
    """)
    return


@app.cell
def _():
    seek_time_ms = 10
    transfer_rate_mb_s = 100
    target_seek_fraction = 0.01

    # time to transfer X MB, in ms, should be ~100x the seek time
    block_size_mb = (seek_time_ms / target_seek_fraction) / 1000 * transfer_rate_mb_s
    block_size_mb
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    That's where the (real-world) 128 MiB default comes from — a figure that
    keeps creeping up as disks get faster. A large block also simplifies the
    storage layer (fixed-size units are easy to place and account for) and
    plays well with replication: each block, not each file, is the unit that
    gets copied around for fault tolerance.

    This lab cluster uses a 1 MiB block instead, purely so the next two
    sections don't need a multi-gigabyte file to make their point.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 3.2. A small file doesn't consume a full block

    Unlike a single-disk filesystem, a file smaller than one HDFS block does
    **not** reserve the full block's worth of space.
    """)
    return


@app.cell
def _(hdfs, subprocess):
    import tempfile
    from pathlib import Path

    def put(local_path: Path, hdfs_path: str) -> None:
        """Copy a local file into HDFS via the real `hadoop fs -copyFromLocal`."""
        container_tmp = f"/tmp/{local_path.name}"
        subprocess.run(
            ["docker", "cp", str(local_path), f"hdfs-hatchery-namenode:{container_tmp}"],
            check=True,
        )
        hdfs("hadoop", "fs", "-copyFromLocal", "-f", container_tmp, hdfs_path)

    hdfs("hadoop", "fs", "-mkdir", "-p", "/user/root")

    lab_dir = Path(tempfile.mkdtemp(prefix="hdfs_hatchery_"))
    small_file = lab_dir / "small.txt"
    small_file.write_text("hello hdfs\n")
    put(small_file, "/user/root/small.txt")

    print(hdfs("hadoop", "fs", "-du", "-h", "/user/root/small.txt"))
    return lab_dir, put


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The two numbers are the file's actual length and the total disk space it
    occupies *across all replicas* (length × replication factor) — a handful
    of bytes, not 1 MiB × 3. Blocks are an accounting unit for *large* files;
    a small file just uses what it needs.

    ### 3.3. A file made of several blocks

    Now a file big enough to actually split, at our shrunk 1 MiB block size.
    """)
    return


@app.cell
def _(hdfs, lab_dir, put):
    import random
    import string

    random.seed(42)
    multiblock_file = lab_dir / "multiblock.txt"
    with multiblock_file.open("w") as f:
        for _ in range(50_000):
            f.write("".join(random.choices(string.ascii_lowercase + " ", k=80)) + "\n")

    put(multiblock_file, "/user/root/multiblock.txt")
    print(hdfs("hadoop", "fs", "-du", "-h", "/user/root/multiblock.txt"))
    return


@app.cell
def _(hdfs):
    print(
        hdfs(
            "hdfs", "fsck", "/user/root/multiblock.txt",
            "-files", "-blocks", "-locations",
        )
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    A ~4 MB file becomes four blocks (three full 1 MiB blocks plus a
    remainder), and `fsck` lists, for each one, which datanodes hold a copy
    and confirms `Live_repl=3` — every block replicated on all three
    datanodes we have. This is the file-to-block mapping the namenode keeps
    in memory, made visible.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Namenodes and datanodes

    HDFS is master–worker: **one namenode** holds the filesystem tree and all
    metadata (owners, permissions, and — critically — which blocks make up
    each file) in memory, persisting it to disk as a namespace image plus an
    edit log. **Datanodes** are the workhorses: they store block contents and
    periodically send the namenode heartbeats and block reports.

    The namenode is a single point of failure by construction — lose it, and
    there's no way to reconstruct which blocks belong to which files, even
    though the block bytes themselves are still sitting safely on the
    datanodes. (Production HDFS addresses this with a standby namenode and a
    shared edit log — HDFS High Availability. Our lab cluster skips this: one
    namenode, no HA, on purpose, to keep the setup small.)

    We already saw the datanode side of this relationship in the
    `dfsadmin -report` output above: three entries, one per datanode, each
    with its own capacity and block count — that's the namenode's live view
    of its workers.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. Replication and fault tolerance

    Every block above was replicated three times — this cluster's
    `dfs.replication` is `3`, the same "typically three" default the book
    describes. (Note: the book's own pseudo-distributed, single-datanode setup
    uses `dfs.replication = 1` instead, for the obvious reason that one
    datanode can't hold three copies of anything. Our three-datanode cluster
    exists specifically so replication has somewhere to go.)

    Let's kill a datanode and watch the cluster notice — and watch the file
    stay readable regardless.
    """)
    return


@app.cell
def _(docker_ctl):
    docker_ctl("stop", "hdfs-hatchery-datanode2")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Right after the stop, the namenode hasn't missed a heartbeat yet — it
    still believes `datanode2` is live and may hand its address out as a
    read location, which would make a `cat` right now a coin flip (it either
    lands on a surviving replica, or on the now-unreachable one and fails).
    That race is itself informative: it's why HA and dead-node detection
    exist. We wait it out properly instead — polling `dfsadmin -report`
    until the namenode has actually noticed:
    """)
    return


@app.cell
def _(hdfs):
    import time

    def live_and_dead_counts() -> tuple[int, int]:
        # dfsadmin omits the "Dead datanodes (N):" header entirely when N is 0.
        report = hdfs("hdfs", "dfsadmin", "-report")
        live = int(report.split("Live datanodes (")[1].split(")")[0])
        dead = (
            int(report.split("Dead datanodes (")[1].split(")")[0])
            if "Dead datanodes (" in report
            else 0
        )
        return live, dead

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        live, dead = live_and_dead_counts()
        print(f"live={live} dead={dead}")
        if dead > 0:
            break
        time.sleep(3)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    With our shortened `heartbeat.recheck-interval`, that takes roughly 30
    seconds (production defaults would take ~10.5 minutes). Now that
    `datanode2` is confirmed dead, the namenode excludes it from any new
    block-location answer — so the read below only ever gets pointed at
    surviving replicas:
    """)
    return


@app.cell
def _(hdfs):
    print(hdfs("hadoop", "fs", "-cat", "/user/root/small.txt"))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    That `cat` succeeding with a datanode down *is* the point: the client
    never knows or cares that one of the three copies is unreachable — the
    namenode simply stops handing out the dead location. This is also why
    the `fsck` below still reports `HEALTHY` rather than under-replicated —
    with two live datanodes and no third to take over the missing replica,
    HDFS holds what it has rather than failing outright, and would
    re-replicate the block onto `datanode2` (or a new node) once contact
    resumes.
    """)
    return


@app.cell
def _(hdfs):
    print(hdfs("hdfs", "fsck", "/user/root/multiblock.txt").splitlines()[-1])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Bring it back and confirm it rejoins:
    """)
    return


@app.cell
def _(docker_ctl):
    docker_ctl("start", "hdfs-hatchery-datanode2")
    return


@app.cell
def _(hdfs):
    print(hdfs("hdfs", "dfsadmin", "-report").split("-------")[0])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 6. The command-line interface & permissions

    The classic `hadoop fs` workflow — the same commands the book walks
    through against its own pseudo-distributed setup, run here for real. A
    round trip: copy a local file in, list it, copy it back out, and diff.
    """)
    return


@app.cell
def _(hdfs, lab_dir, put):
    import hashlib

    quote_file = lab_dir / "quangle.txt"
    quote_file.write_text(
        "On the top of the Crumpetty Tree\n"
        "The Quangle Wangle sat,\n"
        "But his face you could not see,\n"
        "On account of his Beaver Hat.\n"
    )
    put(quote_file, "quangle.txt")  # relative path -> lands in /user/root, our home dir

    hdfs("hadoop", "fs", "-mkdir", "-p", "books")
    print(hdfs("hadoop", "fs", "-ls", "."))
    return hashlib, quote_file


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Reading the listing left to right: file **mode**, **replication factor**
    (blank for `books/` — replication doesn't apply to directories, which are
    pure namenode metadata, not datanode-stored blocks), **owner**,
    **group**, **size in bytes**, **modification time**, and finally the
    **name**. Permissions follow the same rwx model as a POSIX filesystem —
    read to list/open, write to create/delete entries, execute (on
    directories only) to traverse into them.
    """)
    return


@app.cell
def _(hdfs, lab_dir, subprocess):
    roundtrip_file = lab_dir / "quangle.copy.txt"
    hdfs("hadoop", "fs", "-copyToLocal", "-f", "quangle.txt", "/tmp/quangle.copy.txt")
    subprocess.run(
        [
            "docker", "cp",
            "hdfs-hatchery-namenode:/tmp/quangle.copy.txt",
            str(roundtrip_file),
        ],
        check=True,
    )
    return (roundtrip_file,)


@app.cell
def _(hashlib, quote_file, roundtrip_file):
    original_digest = hashlib.md5(quote_file.read_bytes()).hexdigest()
    roundtrip_digest = hashlib.md5(roundtrip_file.read_bytes()).hexdigest()
    original_digest, roundtrip_digest, original_digest == roundtrip_digest
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Same digest on both ends — the file made the round trip to HDFS and back
    intact, exactly the check the book runs with `md5`.

    ## 7. WebHDFS — the same filesystem, over HTTP

    The CLI above works because it runs *inside* the namenode container,
    where Docker's internal DNS resolves `namenode`, `datanode1`, etc. A
    Python process on the host has no such luxury — but WebHDFS, HDFS's REST
    API, is reachable directly, because the namenode's HTTP port (`9870`) is
    published to the host.

    **Metadata** operations — the ones the namenode can answer entirely on
    its own — work straightforwardly:
    """)
    return


@app.cell
def _():
    import requests

    WEBHDFS = "http://localhost:9870/webhdfs/v1"

    def webhdfs(path: str, op: str, **params):
        response = requests.get(f"{WEBHDFS}{path}", params={"op": op, **params})
        response.raise_for_status()
        return response.json()

    return (webhdfs,)


@app.cell
def _(webhdfs):
    webhdfs("/user/root/small.txt", "GETFILESTATUS")
    return


@app.cell
def _(webhdfs):
    webhdfs("/user/root", "LISTSTATUS")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    `GETFILESTATUS` mirrors the `FileStatus` object from Hadoop's Java
    `FileSystem` API: length, block size, replication, owner, permission —
    the same fields, over HTTP, from any language with an HTTP client. Notice
    `blockSize: 1048576` and `replication: 3` — our compose-file settings,
    confirmed independently through a completely different interface.

    **Data** operations — reading or writing actual bytes — work
    differently, and this difference *is* the mechanism behind HDFS's read
    path. Watch what happens when we ask to open a file over WebHDFS:
    """)
    return


@app.cell
def _():
    import requests as _requests

    _resp = _requests.get(
        "http://localhost:9870/webhdfs/v1/user/root/small.txt",
        params={"op": "OPEN"},
        allow_redirects=False,
    )
    _resp.status_code, _resp.headers.get("Location")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    A `307 Temporary Redirect` pointing at `datanode3:9864` (a *different*
    port, on a *different* host, chosen by the namenode). That hostname is
    internal to the Docker network this lab created — it won't resolve from
    the host machine at all, which is exactly why the CLI section above ran
    inside the namenode container instead of out here.

    This redirect is not an accident of our setup; it's the whole point of
    HDFS's read path:

    1. The client asks the **namenode** to open a file.
    2. The namenode returns, for each block, the addresses of the datanodes
       holding a copy — ordered by proximity to the client.
    3. The client then talks **directly** to those datanodes to stream the
       actual bytes, never routing data through the namenode.

    We watched step 2 happen just now — WebHDFS's redirect *is* step 2, made
    visible as an HTTP response. Step 3 is why datanode addresses, not
    namenode addresses, matter for throughput: with many clients, the data
    traffic fans out across every datanode in the cluster, and the namenode
    is left only answering lightweight "where is it" questions — which is
    also why it can hold that metadata in memory rather than serving data
    from disk itself.

    Writing follows the mirror image: the client streams each block to the
    first datanode in a pipeline, which forwards it to the second, which
    forwards it to the third — three datanodes, three replicas, one write.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 8. The coherency model, briefly

    One more subtlety worth knowing even without a live demo: writing bytes
    to an HDFS file and even flushing the stream does **not** guarantee
    another reader sees them yet. Visibility is guaranteed only:

    - after a full block has been written, for that block, or
    - after an explicit `hflush()` (visible to readers, may still only be in
      the datanodes' memory) or `hsync()` (guaranteed durable to disk), or
    - after the file is **closed**, which implies an `hflush()`.

    In other words: readers racing a writer may see a shorter file than
    what's actually been sent — never a corrupt one. This is the trade-off
    HDFS makes for throughput over strict POSIX semantics.

    ## 9. Cleanup

    ```bash
    docker compose down -v
    ```

    drops the namenode and datanode volumes along with the containers, so the
    next `up` starts from a freshly formatted, empty filesystem.
    """)
    return


if __name__ == "__main__":
    app.run()
