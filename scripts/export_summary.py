from __future__ import annotations

import argparse

from src.reporting.summary_writer import write_summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_dir", required=True)
    args = ap.parse_args()

    write_summary(args.run_dir)
    print(f"wrote {args.run_dir}/summary.json")


if __name__ == "__main__":
    main()
