# Flexible Snapshot Proxy Usage Scenarios

## Upload an exact live (not Crash-consistent) replica of a local raw block device (/dev/sda) from an AWS Snowcone to an EBS Snapshot.

```
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

This approach can be combined with `fsfreeze` on Linux systems to freeze write I/O to the target device prior to the transfer.

The example above was run on an AWS Snowcone snc1.medium instance, and was used to make a backup of the instance's root disk to us-east-1.

## Copy resulting EBS Snapshot to s3://bucketname/ via an EC2 Instance in the region:

```
[root@ec2 ]# python3 src/main.py -nodeps movetos3 snap-09c5ff1da81c4fb7a bucketname
Snapshot snap-09c5ff1da81c4fb7a contains 5069 chunks and 2657615872 bytes, took 0.25 seconds.
movetos3 took 18.14 seconds at 146479006.72 bytes/sec.
```