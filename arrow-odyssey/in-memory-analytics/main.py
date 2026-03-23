import marimo

__generated_with = "0.21.1"
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

    return mo, np, pa


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
    # Chapter 4
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## ArrowSchema
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Apache Arrow's C Data Interface unifies table and column metadata by treating a full record batch as a single **Struct** data type. Using the recursive `children` pointer within the `ArrowSchema` structure, a table is represented as a root node with the format string `+s`, where each child pointer simply represents an individual column. This design allows any function capable of processing nested data to automatically handle entire tables, reducing the binary interface to a single, self-referential structure that can describe any dataset complexity through a single memory pointer.
    """)
    return


@app.cell
def _(pa):
    from cffi import FFI
    import struct

    # Initialize FFI and the Shared Map
    ffi_provider = FFI()
    ffi_provider.cdef("""
        struct ArrowSchema {
          const char* format;
          const char* name;
          const char* metadata;
          int64_t flags;
          int64_t n_children;
          struct ArrowSchema** children;
          struct ArrowSchema* dictionary;
          void (*release)(struct ArrowSchema*);
          void* private_data;
        };

        struct ArrowArray {
          int64_t length;
          int64_t null_count;
          int64_t offset;
          int64_t n_buffers;
          int64_t n_children;
          const void** buffers;
          struct ArrowArray** children;
          struct ArrowArray* dictionary;
          void (*release)(struct ArrowArray*);
          void* private_data;
        };
    """)


    def decode_c_string(c_str):
        return ffi_provider.string(c_str).decode('utf-8') if c_str != ffi_provider.NULL else "None"


    def print_schema_recursive(node, indent=0):
        pref = "  " * indent
        name = decode_c_string(node.name)
        fmt = decode_c_string(node.format)
        print(f"{pref}* Node: {name}")
        print(f"{pref}  - Format: {fmt}")
        print(f"{pref}  - Children: {node.n_children}")
    
        for i in range(node.n_children):
            print_schema_recursive(node.children[i], indent + 2)

    _data_payload = [
        pa.array([1, 2, 3], type=pa.int64()),
        pa.array([["apple"], ["orange", "banana"], []], type=pa.list_(pa.string()))
    ]
    _batch_payload = pa.RecordBatch.from_arrays(_data_payload, names=["id", "tags"])

    shared_c_schema = ffi_provider.new("struct ArrowSchema*")
    shared_c_array = ffi_provider.new("struct ArrowArray*")

    _batch_payload._export_to_c(
        int(ffi_provider.cast("uintptr_t", shared_c_array)),
        int(ffi_provider.cast("uintptr_t", shared_c_schema))
    )


    print("[ Apache ArrowSchema Hierarchy ]\n")
    print_schema_recursive(shared_c_schema)
    return (
        FFI,
        decode_c_string,
        ffi_provider,
        shared_c_array,
        shared_c_schema,
        struct,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## ArrowArray
    """)
    return


@app.cell
def _(decode_c_string, ffi_provider, shared_c_array, shared_c_schema, struct):
    def print_array_deep_recursive(schema_node, array_node, indent=0):
        pref = "  " * indent
        sub_pref = "  " * (indent + 1)
        buf_pref = "  " * (indent + 2)
    
        name = decode_c_string(schema_node.name)
        fmt = decode_c_string(schema_node.format)
    
        # 1. Header & Metadata
        print(f"{pref}- Column: [{name}] (Format: {fmt})")
        print(f"{sub_pref}- Length: {array_node.length}")
        print(f"{sub_pref}- Nulls: {array_node.null_count}")
    
        # 2. Buffers Section
        if array_node.n_buffers > 0:
            print(f"{sub_pref}- Buffers")
            for i in range(array_node.n_buffers):
                ptr = array_node.buffers[i]
                print(f"{buf_pref}- Buffer {i}")
            
                if ptr == ffi_provider.NULL:
                    print(f"{buf_pref}  - Address: NULL")
                else:
                    print(f"{buf_pref}  - Address: {ptr}")
                    # Peek at raw bytes for every non-null buffer
                    peek_size = 32
                    raw_peek = ffi_provider.buffer(ptr, peek_size)[:]
                    print(f"{buf_pref}  - Value: {raw_peek}")

        # 3. Final Value Decoding Logic (Logical View)
        if fmt in ('l', 'q'):
            ptr = array_node.buffers[1]
            if ptr != ffi_provider.NULL:
                byte_size = 4 if fmt == 'l' else 8
                struct_fmt = 'i' if fmt == 'l' else 'q'
                raw = ffi_provider.buffer(ptr, array_node.length * byte_size)
                values = struct.unpack(f'<{array_node.length}{struct_fmt}', raw)
                print(f"{sub_pref}- Decoded Values: {list(values)}")

        elif fmt == '+l':
            ptr = array_node.buffers[1]
            if ptr != ffi_provider.NULL:
                off_raw = ffi_provider.buffer(ptr, (array_node.length + 1) * 4)
                offsets = struct.unpack(f'<{array_node.length + 1}i', off_raw)
                print(f"{sub_pref}- Decoded Offsets: {list(offsets)}")

        elif fmt == 'u':
            off_ptr = array_node.buffers[1]
            data_ptr = array_node.buffers[2]
            if off_ptr != ffi_provider.NULL and data_ptr != ffi_provider.NULL:
                off_raw = ffi_provider.buffer(off_ptr, (array_node.length + 1) * 4)
                offsets = struct.unpack(f'<{array_node.length + 1}i', off_raw)
                total_bytes = offsets[-1]
                string_data = ffi_provider.buffer(data_ptr, total_bytes)[:]
                decoded = [string_data[offsets[i]:offsets[i+1]].decode('utf-8') 
                          for i in range(array_node.length)]
                print(f"{sub_pref}- Decoded Values: {decoded}")

        # 4. Recursion for Children
        if array_node.n_children > 0:
            print(f"{sub_pref}- Children ({array_node.n_children}):")
            for i in range(array_node.n_children):
                print_array_deep_recursive(schema_node.children[i], array_node.children[i], indent + 4)

    print("[ Apache Arrow Data Physical Layout and Decoded Values ]\n")
    print_array_deep_recursive(shared_c_schema, shared_c_array)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## ArrowDeviceArray
    """)
    return


@app.cell
def _(FFI, np, pa):
    ffi_dev = FFI()
    ffi_dev.cdef("""
        typedef int32_t ArrowDeviceType;

        struct ArrowArray {
            int64_t length;
            int64_t null_count;
            int64_t offset;
            int64_t n_buffers;
            int64_t n_children;
            const void** buffers;
            struct ArrowArray** children;
            struct ArrowArray* dictionary;
            void (*release)(struct ArrowArray*);
            void* private_data;
        };

        struct ArrowDeviceArray {
            struct ArrowArray array;
            int64_t device_id;
            ArrowDeviceType device_type;
            void* sync_event;
            int64_t reserved[3];
        };
    """)

    # Create CPU-based device data 
    data_raw = np.array([100, 200, 300], dtype=np.int64)
    arrow_arr = pa.array(data_raw)

    # We allocate the larger 'ArrowDeviceArray' struct
    c_device_array = ffi_dev.new("struct ArrowDeviceArray*")

    # Exporting specifically to the Device C-interface
    arrow_arr._export_to_c_device(int(ffi_dev.cast("uintptr_t", c_device_array)))

    # Inspect the wrapper (ArrowDeviceArray)
    print("1. Device Wrapper Layer")
    print(f"Device Type : {c_device_array.device_type}")
    print(f"Device ID   : {c_device_array.device_id}")
    print(f"Sync Event  : {c_device_array.sync_event}")
    print(f"Reserved    : {list(c_device_array.reserved)}")

    # Inspect the embedded data 
    inner = c_device_array.array
    print("\n2. Embedded Data Layer")
    print(f"Length : {inner.length}")
    print(f"Buffers: {inner.n_buffers}")

    for i in range(inner.n_buffers):
        ptr = inner.buffers[i]
        print(f"  [Buffer {i} Address]: {ptr}")

    # Clean up 
    if inner.release != ffi_dev.NULL:
        inner.release(ffi_dev.addressof(inner))
    return ffi_dev, inner


@app.cell
def _(ffi_dev, inner, struct):
    # Access the data buffer 
    # inner.buffers[1] is the pointer to our actual integers
    data_ptr = inner.buffers[1]

    # Create a virtual view
    # We map 24 bytes (3 elements * 8 bytes each) starting at that address
    raw_bytes = ffi_dev.buffer(data_ptr, 24)

    # Interpret the bytes 
    # '<3q' means: Little-endian, 3 elements, signed long long (int64)
    decoded_values = struct.unpack('<3q', raw_bytes)

    print(f"Address          : {data_ptr}")
    print(f"Raw Bytes (Hex)  : {raw_bytes[:].hex()}")
    print(f"Decoded from RAM : {decoded_values}")
    return


if __name__ == "__main__":
    app.run()
