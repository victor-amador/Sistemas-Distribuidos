import os

os.environ.setdefault("WORKER_UUID", "W-SECONDARY")
os.environ.setdefault("ORIGINAL_SERVER_UUID", "")

from worker1 import main


if __name__ == "__main__":
    main()

