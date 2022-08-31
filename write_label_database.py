import argparse
from pathlib import Path
import json
import shutil
from time import time
import lmdb
import pickle


def write_database(d: dict, database: Path):
    # Remove any existing database.
    database.parent.mkdir(parents=True, exist_ok=True)
    if database.exists():
        shutil.rmtree(database)

    # For condor usage, we create a local database on the disk.
    tmp_dir = Path("/tmp") / f"TEMP_{time()}"
    tmp_dir.mkdir(parents=True)

    tmp_database = tmp_dir / f"{database.name}"

    # Create the database.
    with lmdb.open(path=f"{tmp_database}", map_size=2**40) as env:
        # Add the protocol to the database.
        with env.begin(write=True) as txn:
            key = "protocol".encode("ascii")
            value = pickle.dumps(pickle.DEFAULT_PROTOCOL)
            txn.put(key=key, value=value, dupdata=False)
        # Add the keys to the database.
        with env.begin(write=True) as txn:
            key = pickle.dumps("keys")
            value = pickle.dumps(sorted(d.keys()))
            txn.put(key=key, value=value, dupdata=False)
        # Add the values to the database.
        with env.begin(write=True) as txn:
            for key, value in sorted(d.items()):
                key = pickle.dumps(key)
                value = pickle.dumps(value)
                txn.put(key=key, value=value, dupdata=False)

    # Move the database to its destination.
    shutil.move(f"{tmp_database}", database)

    # Remove the temporary directories.
    shutil.rmtree(tmp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src_labels", type=Path, required=True)
    parser.add_argument("--dst_database", type=Path, required=True)
    args = parser.parse_args()

    src_labels = args.src_labels
    dst_database = args.dst_database

    # Customize here how labels are found and organise them as a dictionary.
    # Here we just open a JSON file, but it could be a CSV or a MAT file.
    with src_labels.open() as file:
        data = json.load(file)
    data = {key: datum for key, datum in enumerate(data)}

    write_database(data, dst_database)
