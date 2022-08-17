Flexible Snapshot Proxy follows a nested threadpool model using [joblib.Parallel](https://joblib.readthedocs.io/en/latest/generated/joblib.Parallel.html) and `delayed()` for asynchronous execution. 

Threading is controlled by the [num_jobs](https://github.com/awslabs/flexible-snapshot-proxy/blob/bf817314551d3fe904efc08ac32da799135c91b7/src/main.py#L299) variable, which by default is 16 for single-region operations and 27 for multi-region operations. Reasoning for those values is in the comment block.

`num_jobs` effectively provides an upper limit for how many threads are used. `joblib.Parallel` has its own limit logic, which will cap threads to a smaller number on a system with very few CPU cores in order to prevent resource exhaustion. `num_jobs` is just a hint to Parallel, which it is free to reduce.

Most parts of the solution nest thread pools within thread pools with the goal to "divide and conquer" the task. Once we build the snapshot index, the index is broken up into smaller parts that are processed concurrently. Each part will then process individual chunks, also concurrently and asynchronously. The thread complexity is therefore `O(N^2)` where `N=n_jobs`, and `n_jobs` is capped at `num_jobs`. 

So, for single region operations on a system with sufficient CPU cores, up to 256 threads are used by default. For multi-region operations, where network latency is typically higher, we effectively use up to 729 threads. On a system with no Network, CPU or Memory constraints, FSP is able to sustain close to 500 MiB/s per snapshot stream, which is the practical limit described in the [EBS Direct API User Guide](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebsapi-performance.html). 

It is not advisable to change the defaults without a complete understanding of the solution's performance envelope. If the value of `num_jobs` is increased, you may encounter API throttling from the various APIs we use. If it is decreased, FSP will use fewer resources (and less memory), but may be slower. Asynchronous parallel execution of small tasks effectively helps mitigate network and disk latency at the expense of memory.

TODO: Provide actual CPU utilization profiles for a few typical use cases.