# Flexible Snapshot Proxy

High-performance open-source orchestration utility that utilizes EBS Direct APIs to efficiently clone, copy and migrate EBS snapshots to and from arbitrary File, Block or Object destinations.

## Basic Usage

Help is available by running `src/main.py -h`.

Some usage examples are available as full-stack canaries in `src/test_functional.py`. 

The below one-liner will generate a list of all commands for which test cases exist, and show their syntax.

> % cat test/test_functional.py|grep python3 | cut -d "/" -f 2-3 | awk -F"[{']" '{print $1 $3 " " $6 " " $9}'

> src/main.py list ORG_SNAP  
> src/main.py download snapshotId PATH_TO_RAW_DEVICE 
> src/main.py deltadownload snapshot1 snapshot2 PATH_TO_RAW_DEVICE
> src/main.py upload UPLOAD_BLOCKS  
> src/main.py copy snapshotId  
> src/main.py diff snapshotId_1 snapshotId_2 
> src/main.py sync snapshotId_1 snapshotId_2 snapshotId_parent
> src/main.py multiclone snapshotId PATH_TO_TEMP_DIRECTORY 
> src/main.py fanout UPLOAD_BLOCKS PATH_TO_PROJECT_DIRECTORY 
> src/main.py movetos3 snapshotId DEST_S3_BUCKET 
> src/main.py getfroms3 snapshotId DEST_S3_BUCKET 

## Installation

Currently, the utility will enumerate all Python package dependencies on runtime, and install necessary packages via `pip3` if they are not already installed on the system. It will show no indication of progress, and will not ask
the user for permission to install additional packages. 

> TODO: Ask user for permission, print a list of packages it is going to install.
> TODO: Get the package into PyPI so it could be installed via `pip3`.

## Configuration

Configuration of the transfer is performed at runtime with the following CLI arguments:

> Optional arguments:
>   -h, --help            show this help message and exit
>   -o ORIGIN_REGION, --origin_region ORIGIN_REGION
>                         AWS Origin Region. Source of Snapshot copies. (default: .aws/config then us-east-1)
>   -d, --dry_run         Preform a dry run of FSP operation to check valid AWS permissions. (default: false)
>   -q, --quiet           quiet output
>   -v, --verbosity       output verbosity. (Pass/Fail blocks per region)
>   -vv                   increased output verbosity. (Pass/Fail for individual blocks)
>   -vvv                  Maximum output verbosity. (All individual block retries will be recorded)

Additional advanced tuneables are currently in the source itself. 

>					NUM_JOBS controls the parallelism
>					FULL_COPY enables transfer of sparse chunks, which are skipped by default
>					offset in chunk_and_align() controls the maximum size of S3 Objects generated. 64 chunks = 32 MB.

TODO: implement a `setup.py` script for CLI configuration/installation.

## Features

Flexible Snapshot Proxy supports the following commands:

> list                Returns accurate size of an EBS Snapshot by enumerating
>                     actual consumed/allocated space. 
> 
> diff                Returns accurate size of an EBS Snapshot Delta by
>                     enumerating the incremental block-level difference 
>                     between 2 Snapshots with a common parent.
> 
> download            Transfers an EBS Snapshot to an arbitrary file or
>                     block device.
> 
> deltadownload       Downloads the delta between any two snapshots with a
>                     common parent on top of an arbitrary file or block device.
>                     Can be used for synchronizing existing volumes created from
>                     the parent.
> 
> upload              Transfers an arbitrary file or block device to a new
>                     EBS Snapshot.

> copy                Transfers an EBS Snapshot to another EBS Direct API
>                     Endpoint. Intended use case: copy EBS Snapshots across
>                     accounts and/or regions)
> 
> sync                Synchronizes the incremental difference between 2
>                     EBS Snapshots, delta(A,B) to Snapshot C (clone of A),
>                     resulting in Snapshot D (clone of B). Intended use case:
>                     `copy` the parent snapshot, then use `sync` to synchronize
>                     changes.
> 
> movetos3            Transfers an EBS Snapshot or an arbitrary image file / block 
> (TODO: verify		  device to a customer-owned S3 Bucket (any S3 Storage Class, or 
> block->S3 path)	  Snow Family), with zstandard compression, tuneable object 
> 					  size and an independent segment checksum.
> 
> getfroms3           Transfers a Snapshot stored in a customer-owned S3
>                     Bucket to a new block volume or file.
> 
> multiclone          Same functionality as `download`, but writing to
>                     multiple destinations in parallel. Intended use case: clone a
>                     snapshot to multiple volumes.
> 
> fanout              Upload from arbitrary file or block device to 
> 					  multiple EBS Snapshot(s) in parallel, provided a list 
> 					  of regions. 

## Design Overview

todo: Outline the design choices of high parallelization and sharing memory completing the same job in different regions (e.g. `multiclone` and `fanout`)

## System requirements

[Memory](Memory.md)

[CPU](src/fsp.py#L7)

[Network](src/fsp.py#L8)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
