#!/usr/bin/env python3
"""Compare derived metrics in two perf-lab suite JSON reports (stdlib only)."""

import argparse
import fnmatch
import json
import math
import sys
from pathlib import Path


def load_report(path):
    with Path(path).open(encoding="utf-8") as source:
        suite = json.load(source)
    if not isinstance(suite, dict) or not isinstance(suite.get("reports"), list):
        raise ValueError(f"{path}: expected a suite object with a reports array")
    reports = {}
    for report in suite["reports"]:
        if not isinstance(report, dict):
            raise ValueError(f"{path}: each report must be an object")
        case = report.get("case")
        metrics = report.get("derived")
        if not isinstance(case, str) or not case:
            raise ValueError(f"{path}: missing or invalid case name")
        if case in reports:
            raise ValueError(f"{path}: duplicate case {case!r}")
        if not isinstance(metrics, dict):
            raise ValueError(f"{path}: {case}: derived must be an object")
        for metric, value in metrics.items():
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError(f"{path}: {case}: {metric}: expected a finite number")
        reports[case] = metrics
    return suite.get("run_id"), reports


def matches(name, patterns):
    return not patterns or any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)


def compare(before, after, cases=(), metrics=(), changed_only=False):
    rows = []
    for case in sorted(before.keys() | after.keys()):
        if not matches(case, cases):
            continue
        old_metrics = before.get(case, {})
        new_metrics = after.get(case, {})
        for metric in sorted(old_metrics.keys() | new_metrics.keys()):
            if not matches(metric, metrics):
                continue
            old = old_metrics.get(metric)
            new = new_metrics.get(metric)
            delta = None
            percent = None
            if old is None:
                status = "added"
            elif new is None:
                status = "removed"
            else:
                delta = new - old
                percent = delta / abs(old) * 100 if old != 0 else (0 if new == 0 else None)
                status = "same" if new == old else "changed"
            if changed_only and status == "same":
                continue
            rows.append(dict(case=case, metric=metric, before=old, after=new,
                             delta=delta, delta_percent=percent, status=status))
    return rows


def number(value, signed=False):
    if value is None:
        return "-"
    # Significant digits preserve small failure rates as well as large QPS values.
    return format(value, "+.6g" if signed else ".6g")


def print_table(rows, markdown=False):
    headers = ["Case", "Metric", "Before", "After", "Delta", "Change", "Status"]
    cells = [headers]
    for row in rows:
        percent = row["delta_percent"]
        change = f"{percent:+.2f}%" if percent is not None else "n/a"
        cells.append([row["case"], row["metric"], number(row["before"]),
                      number(row["after"]), number(row["delta"], signed=True),
                      change, row["status"]])
    if markdown:
        def escape(value):
            return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", "<br>")
        print("| " + " | ".join(headers) + " |")
        print("| " + " | ".join(["---", "---", "---:", "---:", "---:", "---:", "---"]) + " |")
        for cell in cells[1:]:
            print("| " + " | ".join(map(escape, cell)) + " |")
    else:
        widths = [max(len(cell[i]) for cell in cells) for i in range(len(headers))]
        for index, cell in enumerate(cells):
            print("  ".join(value.rjust(widths[i]) if 2 <= i <= 5 else value.ljust(widths[i])
                            for i, value in enumerate(cell)).rstrip())
            if index == 0:
                print("  ".join("-" * width for width in widths))


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="Delta = after - before; Change = Delta / abs(before) * 100. "
               "A zero before value with a nonzero after value has no percentage (n/a). "
               "Positive QPS change is better; positive latency change is worse.",
    )
    parser.add_argument("before", help="earlier suite JSON (or compact baseline)")
    parser.add_argument("after", help="later suite JSON")
    parser.add_argument("--case", action="append", default=[], metavar="GLOB",
                        help="case name pattern; repeat to include multiple patterns")
    parser.add_argument("--metric", action="append", default=[], metavar="GLOB",
                        help="derived metric pattern, e.g. '*.qps' (repeatable)")
    parser.add_argument("--changed-only", action="store_true", help="hide equal values")
    parser.add_argument("--format", choices=["table", "markdown", "json"], default="table")
    args = parser.parse_args()
    try:
        before_id, before = load_report(args.before)
        after_id, after = load_report(args.after)
        rows = compare(before, after, args.case, args.metric, args.changed_only)
    except (OSError, ValueError, OverflowError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    if args.format == "json":
        print(json.dumps(dict(before_run_id=before_id, after_run_id=after_id, rows=rows),
                         indent=2, ensure_ascii=False, allow_nan=False))
    else:
        print(f"Before: {args.before} (run_id={before_id})")
        print(f"After:  {args.after} (run_id={after_id})")
        print("Change = (after - before) / abs(before); zero baseline: n/a.\n")
        print_table(rows, markdown=args.format == "markdown")
        print(f"\n{len(rows)} metric(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
