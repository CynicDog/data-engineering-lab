# Arrow's Memory Mapping

## Virtual Memory 
Virtual memory is a system-level "magic trick" that gives each running process the illusion of having a large, 
continuous block of memory regardless of physical RAM limits. The operating system manages this by dividing data 
into small 4 KB "pages" and using a mapping table to track whether those pages are currently in the fast RAM or 
stored on the disk. This architecture allows for increased security through process isolation and enables a system 
to conceptually manipulate more memory than is physically available.

## Memory Mapping
Memory mapping (POSIX `mmap`) allows a process to block out a chunk of its virtual address space and point it directly 
at a file on the disk. Think of **Virtual Memory** as the "Infrastructure" and **Memory Mapping** as a "Feature" that 
uses that infrastructure—they are essentially two sides of the same coin. By mapping a file to the kernel's page cache,
the system eliminates the need for expensive system calls and data copying, treating the file as if it were already in 
the computer's memory. Multiple processes can map the same file simultaneously, providing a high-performance way 
to share data through a single physical memory space.

## `pa.memory_map()`
The `pyarrow.memory_map()` function leverages these OS properties to provide "zero-copy" access to large datasets
without allocating the full file size in RAM. It utilizes **lazy loading**, meaning the library only triggers a 
"page fault" to pull data from the SSD when your code specifically accesses a memory address, such as reading 
a schema or calculating a column mean. This allows you to handle 1.6 GB Arrow files instantly and efficiently, 
as only the specific chunks required for your computation are ever materialized in physical memory.

### Run the PoC Script 
```bash 
uv run python main.py
```
