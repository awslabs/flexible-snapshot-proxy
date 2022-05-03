The memory utilization of ebs.py depends on several factors - type of command, size of instance, and size of snapshot/volume. It is designed to auto-scale the concurrency based on available resources and spread the workload across many child processes and threads, with each child typically consuming a fixed amount of memory for large data movement operations.

This is a breakdown of operations by type, and profiled requirements for each type.

The first step in all commands that read from the EBS Direct API - today, that is every command except "getfroms3" and "upload" - is building an index of all snapshot blocks that ebs.py will be operating on. We build this index using ListSnapshotBlocks (single snapshot, used for list/copy/download type operations) or ListChangedBlocks (delta between two snapshots of the same lineage, used for diff, sync type operations) API calls.

The index contains metadata about all allocated blocks in the snapshot, and the overall memory requirement for the index is approximately 420 bytes per block. The actual entry size is 268 bytes per block, but there is overhead in maintaining a dictionary and related objects. The measured values below show **real** memory utilization of the entire script for "list"/"diff", including the index and all overhead.

| Snapshot Size | Allocated Blocks | Total Memory, Base + Index |
| --- | --- | --- |
| 4,107,459,362,816 bytes | 7,834,357 | 3,230 MiB |
| 8,272,514,383,872 bytes | 15,778,569 | 6,309 MiB |
| 16,419,947,806,720 bytes | 31,318,565 | 12,688 MiB |

These are the worst case scenario numbers for an almost fully allocated snapshot.

Building the index is a single-threaded operation. An example of memory utilization over time for listing an 8TiB allocated snapshot (line 2 in the table above) is below. It's possible to multithread this operation in the future.

<img width="837" alt="Memory used for listing an 8TiB snapshot" src="https://user-images.githubusercontent.com/1688932/166822175-a940860a-7b68-460a-8bf8-231e6f191c21.png">

In practical terms, considering the recommendations outlined in the ebs.py comment block and typical system memory amounts, a system with 8GB of memory should be able to list a 10 TiB allocated snapshot, and 16 GiB of memory is optimal for listing 16 TiB snapshots. This part scales linearly, so to handle a 64TiB allocated snapshot from an io2 Block Express volume (64 TiB is the maximum volume size supported today), you will need over 48 GiB of memory, and therefore should use a system with 64 GiB or more.

Once the list/diff is complete, additional memory is needed to perform further operations on the snapshots. 

After the index is built, it is split into a number of segments that depends on the NUM_JOBS tuneable, which controls concurrency everywhere in the tool. By default, it's split into 16 segments for same-region operations, and 27 segments for cross-region operations, with the extra concurrency for cross-region operations required to achieve maximum throughput. Its value should be a power of 2, a power of 3, or a combination of the two (e.g. 3 * 2 ^ 3 = 24).

For a same-region 16TiB "download" or "multiclone" operation, we need an extra 905 MiB per parent thread (multiplied by NUM_JOBS) for the segment index, download buffers and overhead. The actual snapshot block data is written to disk immediately after GetSnapshotBlock operation finishes for the block, and is then garbage-collected.

Total memory consumed by "download" of a 16TiB snapshot is therefore 12,688 MiB (index) + 905 MiB * 16 (segment index * NUM_JOBS) ~= 28 GiB, as can be seen in the trace below.

<img width="976" alt="Memory used for downloading a 16TiB snapshot" src="https://user-images.githubusercontent.com/1688932/166829766-29d18560-42d0-4289-a70b-7fb2059d89f8.png">

This is where the 32 GiB memory recommendation comes from. Multiclone does not significantly alter the memory utilization. If you don't plan to download snapshots > 10 TiB, you can use a system with 16 GiB of memory; for a 64 TiB snapshot, you will need 128 GiB.

NOTE: There are easy optimizations possible in the script that can help reduce the memory usage. 50% savings are possible by optimizing the index data structure, and an estimated further 60-70% savings are possible by compressing the index in-memory. These options are not utilized today to keep memory requirements constant and only dependent on snapshot size, as well as to reduce complexity of the script. In practice, network bandwidth and number of vCPUs are more important for the intended use cases, and when running on cloud instances, memory scales with vCPU.

Other datapoints:

Copying a 16 TiB allocated snapshot across regions requires ~ 42 GiB of total memory due to higher concurrency of 27 NUM_JOBS.

<img width="932" alt="Memory used when copying a 16TiB snapshot to another region" src="https://user-images.githubusercontent.com/1688932/166854795-e8ec571d-24c3-44c7-b422-8aa3c23c0063.png">


Copying a 16 TiB allocated snapshot to S3 (same region) requires ~ 28 GiB of total memory. Note that different concurrency logic is used for S3 copies, resulting in 128 child processes for this task on the test system.

<img width="935" alt="Memory used when copying a 16TiB snapshot to S3" src="https://user-images.githubusercontent.com/1688932/166855045-61d47bed-17ec-4c4d-b380-8714d79e5605.png">

The test system used for memory profiling was an r5b.12xlarge (48 vCPU, 384 GiB memory, 10Gbps network) running AL2. It was oversized intentionally and does not represent a recommended configuration.








