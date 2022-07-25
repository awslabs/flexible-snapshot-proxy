# Must run in sudo mode
import subprocess

def main():
    print("Creating Loop Device...")
    with open("/tmp/zeroes", "w") as outfile:
        print(subprocess.run(["head", "-c" , "1G", "/dev/zero"], stdout=outfile))
    LOOP_FILE = subprocess.run(["sudo", "losetup", "-f"], capture_output=True).stdout.decode("utf-8").strip()
    a = subprocess.run(["sudo", "losetup", LOOP_FILE, "/tmp/zeroes"], capture_output=True)
    print(a)
    print(a.stderr)
    print(a.stdout)

    print("Loopback device created successfully")

    print("Seeking, writing and reading...")
    loop = open(LOOP_FILE, 'rb+')
    print("Starting: ", loop.tell())
    print("Ending: ", loop.seek(0, 2))
    print(loop.seek(512, 0))
    print(loop.write((b'0xFF') * 64))
    print(loop.tell())
    print(loop.seek(512,0))
    print(loop.read(64))

    print(subprocess.run(["sudo", "losetup", "-d", LOOP_FILE]))
    print("Loopback device deleted successfully")

if __name__ ==  "__main__":
    main()