"""Disposable PostgreSQL cluster; never connects to an existing database."""
import os
from contextlib import contextmanager
from pathlib import Path
import shutil
import subprocess
import tempfile


@contextmanager
def database():
    for executable in ("initdb", "pg_ctl"):
        if not shutil.which(executable):
            raise RuntimeError(f"{executable} is required on PATH")
    with tempfile.TemporaryDirectory(prefix="deltasync-", dir="/tmp") as directory:
        root = Path(directory)
        data = root / "data"
        subprocess.run(["initdb", "-D", str(data), "-U", "deltasync", "-A", "trust", "--no-locale"],
                       check=True, stdout=subprocess.DEVNULL)
        started = False
        try:
            subprocess.run(["pg_ctl", "-D", str(data), "-l", str(root / "postgres.log"),
                            "-o", f"-k {root} -h '' -p 5432", "-w", "start"],
                           check=True, stdout=subprocess.DEVNULL)
            started = True
            yield f"host={root} port=5432 dbname=postgres user=deltasync"
        finally:
            if started:
                subprocess.run(["pg_ctl", "-D", str(data), "-m", "immediate", "-w", "stop"],
                               check=True, stdout=subprocess.DEVNULL)


def worker_environment(dsn):
    environment = os.environ.copy()
    environment["DELTASYNC_DSN"] = dsn
    return environment
