# Windows specifc instruction 

## Installation

Currently, the utility will enumerate all Python package dependencies on runtime, and install necessary packages via `pip3` if they are not already installed on the system. It will show no indication of progress, and will not ask the user for permission to install additional packages.

Open CMD using 'Run as Administrator'.

Download and Install Chocolatey using the following command.
```
@"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -InputFormat None -ExecutionPolicy Bypass -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))" && SET "PATH=%PATH%;%ALLUSERSPROFILE%\chocolatey\bin"
```
Download and install python using the following command.
```
choco install -y python3
```
You can check the version to verify if Python was successfully installed as follows.
```
python --version
```
configure AWS Account 
```
AWS  configure 
```

update pip
```
python -m pip install --upgrade pip
```


Install dependencies by running src/main.py for the first time such as 
```

src/main.py list ORG_SNAP 
```

Automount is enabled by default in Windows. When enabled, Windows automatically mounts the file system for a new volume (disk or drive) when it is added (connected) to the system, and then assigns a drive letter to the volume. Disk corruption can occur if data is changed on the drive while sanpshots deltas are being downloaded. To prevent this disable automounting of drives. 

1. Open CMD using 'Run as Administrator'. 

2. Type diskpart into the elevated command prompt, and press Enter. (see screenshot below)

3. Type automount disable into the elevated command prompt, and press Enter.

After the snapshots have been downloaded assing drive letters to each of the volume.


## License

This project is licensed under the Apache-2.0 License.
