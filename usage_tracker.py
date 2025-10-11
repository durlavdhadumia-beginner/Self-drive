# save as usage_tracker.py
import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path

LOG_PATH = Path("chatgpt_usage_log.csv")
HEADERS = ["timestamp_iso", "weekly_percent", "five_hour_percent", "notes"]


def ensure_log():
    if not LOG_PATH.exists():
        with LOG_PATH.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADERS)


def add_entry(args):
    ensure_log()
    timestamp = args.timestamp or datetime.now().isoformat(timespec="minutes")
    row = [timestamp, args.weekly, args.five_hour, args.notes]
    with LOG_PATH.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)
    print(f"Added entry: {row}")


def show_summary(args):
    ensure_log()
    cutoff = datetime.now() - timedelta(days=7)
    rows = []
    with LOG_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dt = datetime.fromisoformat(row["timestamp_iso"])
            except ValueError:
                continue
            if dt >= cutoff:
                rows.append(row)

    if not rows:
        print("No entries in the last 7 days.")
        return

    rows.sort(key=lambda r: r["timestamp_iso"])
    print("Last 7 days:")
    for row in rows:
        print(f"- {row['timestamp_iso']}: weekly {row['weekly_percent']}%, "
              f"5h {row['five_hour_percent']}%, notes: {row['notes'] or '-'}")

    weekly_values = [
        float(r["weekly_percent"]) for r in rows if r["weekly_percent"]
    ]
    if weekly_values:
        print(f"Average weekly usage: {sum(weekly_values)/len(weekly_values):.1f}%")
        print(f"Latest weekly value : {weekly_values[-1]:.1f}%")


def plot_entries(_args):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is needed for plotting. Install it with `pip install matplotlib`.")
        return

    ensure_log()
    timestamps, weekly_vals = [], []
    with LOG_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row["weekly_percent"]:
                continue
            try:
                timestamps.append(datetime.fromisoformat(row["timestamp_iso"]))
                weekly_vals.append(float(row["weekly_percent"]))
            except ValueError:
                continue

    if not timestamps:
        print("No data to plot yet.")
        return

    plt.figure(figsize=(8, 4))
    plt.plot(timestamps, weekly_vals, marker="o")
    plt.title("ChatGPT weekly usage (rolling entries)")
    plt.xlabel("Timestamp")
    plt.ylabel("Weekly percentage used")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def main():
    parser = argparse.ArgumentParser(description="Log ChatGPT Plus usage.")
    sub = parser.add_subparsers(dest="cmd")

    add = sub.add_parser("add", help="Add a usage snapshot.")
    add.add_argument("--weekly", type=float, required=True,
                     help="Weekly percentage used (from the ChatGPT panel).")
    add.add_argument("--five-hour", type=float, default=0.0,
                     help="5-hour percentage used (optional).")
    add.add_argument("--timestamp", help="ISO timestamp. Defaults to now.")
    add.add_argument("--notes", default="", help="Optional note.")
    add.set_defaults(func=add_entry)

    show = sub.add_parser("show", help="Show last 7 days of snapshots.")
    show.set_defaults(func=show_summary)

    plot = sub.add_parser("plot", help="Plot weekly usage (requires matplotlib).")
    plot.set_defaults(func=plot_entries)

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
