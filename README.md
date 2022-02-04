# ebs-playground
Playground for EBS Direct API experiments

Pre-requisite install:

```
pip3 install boto3 numpy joblib
```

Usage examples:

To enumerate all deltas for a volume, do something like this: 
 
```
for i in `aws ec2 describe-snapshots --filters Name=volume-id,Values=vol-0315a1e77f4850ff4 | grep SnapshotId | \ 
cut -d "\"" -f 4 `; do python3 ebs.py diff $old $i; old=$i; done | \ 
awk '{chunks+=$7; size+=$10} END{printf "Snapshot deltas contain %s chunks and %s bytes\n", chunks, size;}'; \ 
python3 ebs.py list $i
```
