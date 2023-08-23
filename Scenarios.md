# Flexible Snapshot Proxy Usage Scenarios

## Upload an exact live (not Crash-consistent) replica of a local raw block device (/dev/sda) from an AWS Snowcone to an EBS Snapshot

```bash
[root@snowcone ]# lsblk -o NAME,FSTYPE,SIZE,MOUNTPOINT
NAME     FSTYPE SIZE MOUNTPOINT
sda               8G 
├─sda1   xfs      8G /
└─sda128          1M 
[root@snowcone]# python3 src/main.py -nodeps upload /dev/sda
Size of /dev/sda is 8589934592 bytes and 16384 chunks
/dev/sda took 148.26 seconds at 17925063.15 bytes/sec.
Total chunks uploaded 5069
snap-09c5ff1da81c4fb7a
```

The last line in the output is the name of the resulting EBS Snapshot.

NOTE: This approach is useful for making replicas of read-only block devices, such as attached iSCSI LUNs, USB disks or image files. It will result in an Application-inconsistent Snapshot if the block device has write I/O at the time of the transfer.

This approach can be combined with `fsfreeze` on Linux systems to freeze write I/O to the target device prior to the transfer. Note that since
we use a Lock() for our shared counter, we need to change the TMPDIR to something tmpfs-based for the script to function on a frozen / filesystem, and use -B and -nodeps to prevent everyone from writing:

```bash
[root@ip-34-223-14-193 flexible-snapshot-proxy]# xfs_freeze -f /; export TMPDIR=/run; python3 -B src/main.py -nodeps upload /dev/sda; unset TMPDIR; xfs_freeze -u /
Size of /dev/sda is 8589934592 bytes and 16384 chunks
/dev/sda took 145.12 seconds at 18312320.8 bytes/sec.
Total chunks uploaded 5069
snap-0028a1524e96e05b4
```

The example above was run on an AWS Snowcone snc1.medium instance, and was used to make a backup of the instance's root disk to us-east-1.

## Copy resulting EBS Snapshot to s3://bucketname/ via an EC2 Instance in the region

```bash
[root@ec2 ]# python3 src/main.py -nodeps movetos3 snap-09c5ff1da81c4fb7a bucketname
Snapshot snap-09c5ff1da81c4fb7a contains 5069 chunks and 2657615872 bytes, took 0.25 seconds.
movetos3 took 18.14 seconds at 146479006.72 bytes/sec.
```

## Determine the size of snapshots and deltas, and estimate savings from deleting old snapshots

You can utilize the `list` and `diff` calls to gain insight into how large your snapshots are, and analyze rate of data change in your environment. This data can then be utilized for cost optimization, as it provides a way to estimate savings from changing the snapshot retention policy, among other things. 

In this example, we take an existing snapshot chain, and perform such calls to get the full size of every snapshot as well as the deltas between each two subsequent snapshots. For 5 snapshots, we effectively get 9 numbers, and can compute the rest from there:

```bash
% python3.10 src/main.py list snap-0668eefa53e9ab533
Snapshot snap-0668eefa53e9ab533 contains 11171 chunks and 5856821248 bytes, took 1.02 seconds.
% python3.10 src/main.py list snap-0e4fed7f9271160b5
Snapshot snap-0e4fed7f9271160b5 contains 11173 chunks and 5857869824 bytes, took 1.01 seconds.
% python3.10 src/main.py list snap-08e18f70bec285993
Snapshot snap-08e18f70bec285993 contains 11185 chunks and 5864161280 bytes, took 1.25 seconds.
% python3.10 src/main.py list snap-0016a2ad9124da55c
Snapshot snap-0016a2ad9124da55c contains 63199 chunks and 33134477312 bytes, took 2.06 seconds.
% python3.10 src/main.py list snap-007e2f49d0b83a51e
Snapshot snap-007e2f49d0b83a51e contains 63522 chunks and 33303822336 bytes, took 2.42 seconds.
% python3.10 src/main.py diff snap-0668eefa53e9ab533 snap-0e4fed7f9271160b5
Changes between snap-0668eefa53e9ab533 and snap-0e4fed7f9271160b5 contain 172 chunks and 90177536 bytes, took 0.87 seconds.
% python3.10 src/main.py diff snap-0e4fed7f9271160b5 snap-08e18f70bec285993
Changes between snap-0e4fed7f9271160b5 and snap-08e18f70bec285993 contain 418 chunks and 219152384 bytes, took 1.12 seconds.
% python3.10 src/main.py diff snap-08e18f70bec285993 snap-0016a2ad9124da55c
Changes between snap-08e18f70bec285993 and snap-0016a2ad9124da55c contain 56263 chunks and 29498015744 bytes, took 4.13 seconds.
% python3.10 src/main.py diff snap-0016a2ad9124da55c snap-007e2f49d0b83a51e
Changes between snap-0016a2ad9124da55c and snap-007e2f49d0b83a51e contain 8478 chunks and 4444913664 bytes, took 2.35 seconds.
```

The below table summarizes the sizes we got in bytes.

| Snapshot ID | Full size | Changes from previous snapshot |
| --- | --- | --- |
| snap-0668eefa53e9ab533 | 5856821248 | 0 |
| snap-0e4fed7f9271160b5 | 5857869824 | 90177536 |
| snap-08e18f70bec285993 | 5864161280 | 219152384 |
| snap-0016a2ad9124da55c | 33134477312 | 29498015744 |
| snap-007e2f49d0b83a51e | 33303822336 | 4444913664 |

We can do additional math with it by subtracting the size of each two snapshots from each other:

| Snapshot ID | Full size | Changes from previous snapshot | Difference in full size |
| --- | --- | --- | --- |
| snap-0668eefa53e9ab533 | 5856821248 | 0 | 
| snap-0e4fed7f9271160b5 | 5857869824 | 90177536 | 1048756 |
| snap-08e18f70bec285993 | 5864161280 | 219152384 | 6291456 |
| snap-0016a2ad9124da55c | 33134477312 | 29498015744 | 27270316032 |
| snap-007e2f49d0b83a51e | 33303822336 | 4444913664 | 169345024 |

This third number tells us the amount of **new** blocks that were allocated between snapshots, i.e. new writes. If we substract the new writes from the delta (second number), we get the amount of overwritten blocks in the same time period. Distinguishing the two is important - if we delete the first snapshot, the newly written chunks are not released and effectively move to the second snapshot, but the old chunks in the first snapshot that were overwritten are deleted, releasing space. 

| Snapshot ID | Full size | Changes from previous snapshot | New writes | Overwrites |
| --- | --- | --- | --- | --- |
| snap-0668eefa53e9ab533 | 5856821248 | 0 | 0 | 0 |
| snap-0e4fed7f9271160b5 | 5857869824 | 90177536 | 1048756 | 89128780 |
| snap-08e18f70bec285993 | 5864161280 | 219152384 | 6291456 | 212860928 |
| snap-0016a2ad9124da55c | 33134477312 | 29498015744 | 27270316032 | 2227699712 |
| snap-007e2f49d0b83a51e | 33303822336 | 4444913664 | 169345024 | 4275568640 |

Based on this data, we can perform a hypothetical exercise. If we delete the first three snapshots, we will release 89128780 + 212860928 + 4275568640 = 4577558348 bytes of data, thus saving $2.29/month with the current price of snapshots in a region like us-east-1.

To summarize this as a formula, for each two snapshots, the tool provides us three numbers - let's call them S1, S2 and D12. We compute a fourth number, C12 = D12 - (S2 - S1), and C12 * Price/GB is our savings from deleting S1. 

Changes from deleting an intermediate snapshot (i.e. not the oldest) are harder to estimate since we would need to figure out double overwrites - that is, chunks that have been overwritten in both the intermediate and the third snapshot. You cannot get this information without comparing the actual chunk map of all three snapshots. While Flexible Snapshot Proxy does build a chunk map of every snapshot during "list" requests, it currently does not have the capability to compare the chunk maps for this purpose.