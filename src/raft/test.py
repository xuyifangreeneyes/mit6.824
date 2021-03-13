import subprocess


def go_test(iter_num, name):
    for i in range(iter_num):
        f = open("out.txt", "w")
        if subprocess.run(["go", "test", "-run", name, "-race"], stdout=f, stderr=f).returncode != 0:
            print("[{}] failed".format(i))
            break
        if "warning" in open("out.txt", "r").read():
            print("[{}] warning occurred".format(i))
            break
        print("[{}] passed".format(i))


if __name__ == "__main__":
    # go_test(500, "2A")
    go_test(100, "2B")