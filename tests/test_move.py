from util import data_transfer
import sys

# source="/tmp/sub-a/ses-1/"
# target="/tmp/sub-a"

# tarfile=data_transfer._tar_dir(source)
# print(tarfile)


# source="/tmp/sub-a/ses-1"
# tarfile=data_transfer._tar_dir(source)
# print(tarfile)


# source="/tmp/sub-a/"
# tarfile=data_transfer._tar_dir(source)
# print(tarfile)

# source="/tmp/sub-a"
# tarfile=data_transfer._tar_dir(source)
# print(tarfile)


try:
    source="/tmp/sub-a/ses-1/abc.tar"
    tarfile=data_transfer._tar_dir(source)
    print(tarfile)
except Exception as e:
    print(f"Exception thrown as expected {e}")
sys.exit(0)

try:
    source="/tmp/sub-a.tar"
    tarfile=data_transfer._tar_dir(source)
    print(tarfile)
except Exception as e:
    print(f"Exception thrown as expected{e}")

try:
    source="/tmp/sub-x.tar"
    tarfile=data_transfer._tar_dir(source)
    print(tarfile)
except Exception as e:
    print(f"Exception thrown as expected{e}")

try:
    source="/tmp/sub-a/sub-a_ses-1.tar"
    tarfile=data_transfer._tar_dir(source)
    print(tarfile)
except Exception as e:
    print(f"Exception thrown as expected{e}")


try:
    source="/tmp/sub-a/abc"
    tarfile=data_transfer._tar_dir(source)
    print(tarfile)
except Exception as e:
    print(f"Exception thrown as expected{e}")

try:
    source="/tmp/sub-a/xyz"
    tarfile=data_transfer._tar_dir(source)
    print(tarfile)
except Exception as e:
    print(f"Exception thrown as expected{e}")

try:
    source="/tmp"
    tarfile=data_transfer._tar_dir(source)
    print(tarfile)
except Exception as e:
    print(f"Exception thrown as expected{e}")    