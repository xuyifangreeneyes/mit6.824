import subprocess

if __name__ == "__main__":
    for i in range(10):
        f = open("out.txt", "w")
        if subprocess.run(["go", "test", "-run", "2A", "-race"], stdout=f, stderr=f).returncode != 0:
            print("[{}] failed".format(i))
            break
        if "warning" in open("out.txt", "r").read():
            print("[{}] warning occurred".format(i))
            break
        print("[{}] passed".format(i))