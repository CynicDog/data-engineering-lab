import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    <h1>In Memory Analytics with Apache Arrow 🏹</h1>
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Dependencies
    """)
    return


@app.cell
def _():
    import marimo as mo
    import pyarrow as pa
    import numpy as np 

    return mo, pa


@app.class_definition
class SVGResource:
    def __init__(self, path):
        self.path = "./images/" + path

    def _repr_svg_(self):
        try:
            with open(self.path, "r") as f:
                return f.read()
        except FileNotFoundError:
            return f"<text>File {self.path} not found.</text>"


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Chapter 1
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Physical Layouts
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Primitive fixed-length value arrays
    """)
    return


@app.cell(hide_code=True)
def _():
    SVGResource("physical_layout_primitive_fixed_array.svg")
    return


@app.cell
def _(pa):
    data_list = [1, None, 2, 4, 8] 

    data = [pa.array([val]) for val in data_list]
    cols = ['c' + str(i) for i in range(5)]

    pa.RecordBatch.from_arrays(data, cols).schema
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Variable-length binary arrays
    """)
    return


@app.cell
def _():
    SVGResource("physical_layout_variable_length_binary_array.svg")
    return


@app.cell
def _(pa):
    pa.array([b"Water", b"Rising"], type=pa.binary())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Variable-length binary view arrays
    """)
    return


@app.cell
def _():
    SVGResource("physical_layout_variable_length_binary_view_array.svg")
    return


@app.cell
def _(pa):
    pa.array([b"Hello", b"Penny the cat", b"and welcome"], type=pa.binary_view())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### List arrays
    """)
    return


@app.cell
def _():
    SVGResource("physical_layout_list_array.svg")
    return


@app.cell
def _(pa):
    pa.array([[12, -7, 25], None, [0, -127, 127, 50], []], type=pa.list_view(pa.int8()))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Fixed-Size List arrays
    """)
    return


@app.cell
def _():
    SVGResource("physical_layout_fixed_size_list_array.svg")
    return


@app.cell
def _(pa):
    pa.array([[10, None], None, [0, 5]], type=pa.list_(pa.int64(), 2))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### List View arrays
    """)
    return


@app.cell
def _():
    SVGResource("physical_layout_list_view_array.svg")
    return


@app.cell
def _(pa):
    pa.array([[12, -7, 25], None, [0, -127, 127, 50], [], [50, 12]], type=pa.list_view(pa.int8()))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ###
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Dictionary-encoded arrays
    """)
    return


@app.cell(hide_code=True)
def _():
    SVGResource("physical_layout_dictionary_encoded_arrays.svg")
    return


@app.cell
def _(pa):
    values = ["foo", "bar", "foo", "barc", None, "baz"]

    pa.array(values).dictionary_encode()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Using PyArrow for Python
    """)
    return


@app.cell
def _(pa):
    archer_list = [{
        'archer': 'Legolas',
        'location': 'Mirkwood',
        'year': 1954,
    },{
        'archer': 'Oliver',
        'location': 'Star City',
        'year': 1941,
    }, {
        'archer': 'Merida',
        'location': 'Scotland',
        'year': 2012,    
    },
    {
        'archer': 'Lara',
        'location': 'London',
        'year': 1996, 
    },
    {
        'archer': 'Artemis',
        'location': 'Greece',
        'year': -600, 
    }]

    archer_type = pa.struct([
        ('archer', pa.utf8()),
        ('location', pa.utf8()), 
        ('year', pa.int16())
    ])

    archers = pa.array(archer_list, type=archer_type)
    archers
    return (archers,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    To exemplify how we can optimize memory usage when utilizing Arrow, we can take the arrays from the struct array we created and easily flatten them into a record batch without any copies being made.
    """)
    return


@app.cell
def _(archers, pa):
    archer_rb= pa.RecordBatch.from_arrays(archers.flatten(), ['archer', 'location', 'year'])

    archer_rb
    return (archer_rb,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The record batch we created holds references to the same arrays we created for the struct array, not copies, which makes this a very efficient operation, even for very large datasets.
    """)
    return


@app.cell
def _(archer_rb, archers):
    struct_archer_column = archers.field('archer') # Get the 'archer' child array from the StructArray
    rb_archer_column = archer_rb.column(0) # Get the 'archer' column from the RecordBatch

    print(f"Struct Buffer Address: {struct_archer_column.buffers()[1].address}")
    print(f"RecordBatch Buffer Address: {rb_archer_column.buffers()[1].address}")

    assert struct_archer_column.buffers()[1].address == rb_archer_column.buffers()[1].address

    print("Proof: The memory addresses are identical. Zero-copy confirmed!")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    When you slice an array, Arrow creates a new array object with a different offset and length, but it points to the exact same memory address as the original.
    """)
    return


@app.cell
def _(archers):
    archer_slice = archers.slice(1, 2) # Create a slice (e.g., from index 1 to 3)

    original_year_buffer = archers.field(2).buffers()[1]
    slice_year_buffer = archer_slice.field(2).buffers()[1]

    print(f"Original Buffer Address: {original_year_buffer.address}")
    print(f"Slice Buffer Address:    {slice_year_buffer.address}")

    assert original_year_buffer.address == slice_year_buffer.address

    print("Proof: Slicing is zero-copy! Both objects point to the same physical RAM.")
    return


@app.cell
def _():
    import pandas as _pd

    df = _pd.DataFrame({'years': [2020, 2021, 2022, 2023, 2024]})
    df_slice = df.iloc[1:3].copy() 

    original_address = df['years'].values.ctypes.data
    slice_address = df_slice['years'].values.ctypes.data

    print(f"Original Pandas Address: {original_address}")
    print(f"Slice Pandas Address:    {slice_address}")

    assert original_address != slice_address

    print("Proof: Pandas is using double the RAM! These are two distinct memory blocks.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Chapter 2
    """)
    return


@app.cell
def _():
    from pyarrow import fs 
    import pyarrow.csv 

    import urllib.request

    return fs, urllib


@app.cell
def _(fs):
    local_fs = fs.LocalFileSystem()
    f, p = fs.FileSystem.from_uri('file:///Users/ginsenglee')

    print(f)
    print(p)
    return


@app.cell
def _(pa, urllib):
    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/forest-fires/forestfires.csv"

    with urllib.request.urlopen(url) as _f:
        forestfires = pa.csv.read_csv(_f)

    forestfires
    return (forestfires,)


@app.cell
def _(forestfires):
    print(forestfires.column(0).num_chunks)
    print(forestfires.column(0).chunks)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Chapter 3
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
