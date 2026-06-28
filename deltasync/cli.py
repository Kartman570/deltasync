import argparse
import json
import os
import sys
from pathlib import Path
import time

from .source import serve
from .store import connect, initialize, state
from .sync import reconcile, sync


def main():
    parser = argparse.ArgumentParser(description="Synchronize a generated API into PostgreSQL")
    parser.add_argument("command", choices=["demo", "test", "serve", "init", "sync", "reconcile", "status"])
    parser.add_argument("--api", default="http://127.0.0.1:8080")
    parser.add_argument("--page-size", type=int, default=7)
    parser.add_argument("--pause-at", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--ready-file", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        if args.command == "demo":
            from .demo import run
            print(json.dumps(run(), indent=2))
            return
        if args.command == "test":
            import subprocess
            from .local import database, worker_environment
            with database() as dsn:
                result = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
                                        env=worker_environment(dsn))
            raise SystemExit(result.returncode)
        if args.command == "serve":
            with serve() as url:
                print(url, flush=True)
                while True:
                    time.sleep(1)
        with connect(os.environ["DELTASYNC_DSN"]) as conn:
            if args.command == "init":
                initialize(conn)
            elif args.command == "status":
                print(json.dumps(state(conn), sort_keys=True))
            elif args.command == "reconcile":
                print(reconcile(conn, args.api, args.page_size))
            else:
                def pause(item):
                    if item["seq"] == args.pause_at:
                        args.ready_file.write_text(str(item["seq"]))
                        while True:
                            time.sleep(0.1)
                print(sync(conn, args.api, args.page_size, pause))
    except (ValueError, RuntimeError, KeyError) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
