# Lance Performance Guide

This guide provides tips and tricks for optimizing the performance of your Lance applications.

## Trace Events

Lance uses tracing to log events. If you are running `pylance` then these events will be emitted to
as log messages. For Rust connections you can use the `tracing` crate to capture these events.

### File Audit

File audit events are emitted when significant files are created or deleted.

| Event               | Parameter | Description                                                                |
|---------------------|-----------|----------------------------------------------------------------------------|
| `lance::file_audit` | `mode`    | The mode of I/O operation (create, delete, delete_unverified)              |
| `lance::file_audit` | `type`    | The type of file affected (manifest, data file, index file, deletion file) |

### I/O Events

I/O events are emitted when significant I/O operations are performed, particularly
those related to indices. These events are NOT emitted when the index is loaded from
the in-memory cache. Correct cache utilization is important for performance and these
events are intended to help you debug cache usage.

| Event              | Parameter | Description                                                                                          |
|--------------------|-----------|------------------------------------------------------------------------------------------------------|
| `lance::io_events` | `type`    | The type of I/O operation (open_scalar_index, open_vector_index, load_vector_part, load_scalar_part) |

### Execution Events

Execution events are emitted when an execution plan is run. These events are useful for
debugging query performance.

| Event              | Parameter           | Description                                                    |
|--------------------|---------------------|----------------------------------------------------------------|
| `lance::execution` | `type`              | The type of execution event (plan_run is the only type today)  |
| `lance::execution` | `output_rows`       | The number of rows in the output of the plan                   |
| `lance::execution` | `iops`              | The number of I/O operations performed by the plan             |
| `lance::execution` | `bytes_read`        | The number of bytes read by the plan                           |
| `lance::execution` | `indices_loaded`    | The number of indices loaded by the plan                       |
| `lance::execution` | `parts_loaded`      | The number of index partitions loaded by the plan              |
| `lance::execution` | `index_comparisons` | The number of comparisons performed inside the various indices |

## Threading Model

Lance is designed to be thread-safe and performant. Lance APIs can be called concurrently unless
explicitly stated otherwise. Users may create multiple tables and share tables between threads.
Operations may run in parallel on the same table, but some operations may lead to conflicts. For
details see [conflict resolution](../format/index.md#conflict-resolution).

Most Lance operations will use multiple threads to perform work in parallel. There are two thread
pools in lance: the IO thread pool and the compute thread pool. The IO thread pool is used for
reading and writing data from disk. The compute thread pool is used for performing computations
on data. The number of threads in each pool can be configured by the user.

The IO thread pool is used for reading and writing data from disk. The number of threads in the IO
thread pool is determined by the object store that the operation is working with. Local object stores
will use 8 threads by default. Cloud object stores will use 64 threads by default. This is a fairly
conservative default and you may need 128 or 256 threads to saturate network bandwidth on some cloud
providers. The `LANCE_IO_THREADS` environment variable can be used to override the number of IO
threads. If you increase this variable you may also want to increase the `io_buffer_size`.

The compute thread pool is used for performing computations on data. The number of threads in the
compute thread pool is determined by the number of cores on the machine. The number of threads in
the compute thread pool can be overridden by setting the `LANCE_CPU_THREADS` environment variable.
This is commonly done when running multiple Lance processes on the same machine (e.g when working with
tools like Ray). Keep in mind that decoding data is a compute intensive operation, even if a workload
seems I/O bound (like scanning a table) it may still need quite a few compute threads to achieve peak
performance.

## Memory Requirements

Lance is designed to be memory efficient. Operations should stream data from disk and not require
loading the entire dataset into memory. However, there are a few components of Lance that can use
a lot of memory.

### Index Cache

Lance uses an index cache to speed up queries. This caches vector and scalar indices in memory. The
max size of this cache can be configured when creating a `LanceDataset` using the `index_cache_size`
parameter. This cache is an LRU cached that is sized by "number of entries". The size of each entry
and the number of entries needed depends on the index in question. For example, an IVF/PQ vector index
contains 1 header entry and 1 entry for each partition. The size of each entry is determined by the
number of vectors and the PQ parameters (e.g. number of subvectors). You can view the size of this cache
by inspecting the result of `dataset.session().size_bytes()`.

The index cache is not shared between tables. For best performance you should create a single table and
share it across your application.

### Scanning Data

Searches (e.g. vector search, full text search) do not use a lot of memory to hold data because they don't
typically return a lot of data. However, scanning data can use a lot of memory. Scanning is a streaming
operation but we need enough memory to hold the data that we are scanning. The amount of memory needed is
largely determined by the `io_buffer_size` and the `batch_size` variables.

Each I/O thread should have enough memory to buffer an entire page of data. Pages today are typically between
8 and 32 MB. This means, as a rule of thumb, you should generally have about 32MB of memory per I/O thread.
The default `io_buffer_size` is 2GB which is enough to buffer 64 pages of data. If you increase the number
of I/O threads you should also increase the `io_buffer_size`.

Scans will also decode data (and run any filtering or compute) in parallel on CPU threads. The amount of data
decoded at any one time is determined by the `batch_size` and the size of your rows. Each CPU thread will
need enough memory to hold one batch. Once batches are delivered to your application, they are no longer tracked
by Lance and so if memory is a concern then you should also be careful not to accumulate memory in your own
application (e.g. by running `to_table` or otherwise collecting all batches in memory.)

The default `batch_size` is 8192 rows. When you are working with mostly scalar data you want to keep batches
around 1MB and so the amount of memory needed by the compute threads is fairly small. However, when working with
large data you may need to turn down the `batch_size` to keep memory usage under control. For example, when
working with 1024-dimensional vector embeddings (e.g. 32-bit floats) then 8192 rows would be 32MB of data. If you
spread that across 16 CPU threads then you would need 512MB of compute memory per scan. You might find working
with 1024 rows per batch is more appropriate.

In summary, scans could use up to `(2 * io_buffer_size) + (batch_size * num_compute_threads)` bytes of memory.
Keep in mind that `io_buffer_size` is a soft limit (e.g. we cannot read less than one page at a time right now)
and so it is not necessarily a bug if you see memory usage exceed this limit by a small margin.

The above limits refer to limits per-scan. There is an additional limit on the number of IOPS that is applied
across the entire process. This limit is specified by the `LANCE_PROCESS_IO_THREADS_LIMIT` environment variable.
The default is 128 which is more than enough for most workloads. You can increase this limit if you are working
with a high-throughput workload. You can even disable this limit entirely by setting it to zero. Note that this
can often lead to issues with excessive retries and timeouts from the object store. 