import pyarrow as pa
import os
import psutil
import gc


def prove_real_mmap():
    file_name = "huge_data.arrow"
    file_size = 1024 * 1024 * 1024  # 1 GB

    # Create the file if it doesn't exist, then EXIT the function to clear RAM
    if not os.path.exists(file_name):
        print(f"[*] Creating {file_size / 1024 ** 2:.0f}MB file...")
        with open(file_name, "wb") as f:
            f.write(b"\x00" * file_size)
        print("[*] File created. Please RUN THIS SCRIPT AGAIN to see the mapping proof.")
        return

    # Measure Initial RAM (File already exists on SSD)
    gc.collect()
    proc = psutil.Process(os.getpid())
    initial_rss = proc.memory_info().rss / 1024 ** 2
    print(f"\n[1] Initial RAM usage: {initial_rss:.2f} MB")

    print(f" (Memory mapping 1GB file...)")

    # Open as read-only to ensure we are just 'viewing' the disk
    mmap_file = pa.memory_map(file_name)
    buffer = mmap_file.read_buffer(file_size)

    post_map_rss = proc.memory_info().rss / 1024 ** 2
    print(f"\n[2] RAM usage after mapping: {post_map_rss:.2f} MB")

    print(f" (Accessing 1 byte...)")
    _ = buffer[file_size // 2]

    final_rss = proc.memory_info().rss / 1024 ** 2
    print(f"\n[3] RAM usage after access: {final_rss:.2f} MB")

    mmap_file.close()
    if os.path.exists(file_name):
        os.remove(file_name)
        print(" (File 'huge_data.arrow' has been deleted.)")


if __name__ == "__main__":
    prove_real_mmap()