#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  scripts/perf-lab-to-baseline.sh [--force] <suite-report.json> [baseline.json]

Convert a perf-lab suite report into a compact baseline. The output defaults to
conf/perf-lab-baseline.json. Existing output is not overwritten unless --force
is specified.

Examples:
  scripts/perf-lab-to-baseline.sh target/perf-lab/suite-123.json /tmp/baseline.json
  scripts/perf-lab-to-baseline.sh --force target/perf-lab/suite-123.json
EOF
}

force=false
args=()
for arg in "$@"; do
    case "$arg" in
        -f|--force)
            force=true
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            ;;
        -* )
            echo "unknown option: $arg" >&2
            usage >&2
            exit 2
            ;;
        *)
            args+=("$arg")
            ;;
    esac
done

if (( ${#args[@]} < 1 || ${#args[@]} > 2 )); then
    usage >&2
    exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required but was not found in PATH" >&2
    exit 1
fi

input=${args[0]}
output=${args[1]:-conf/perf-lab-baseline.json}

if [[ ! -f "$input" ]]; then
    echo "suite report does not exist: $input" >&2
    exit 1
fi

if [[ -e "$output" && "$force" != true ]]; then
    echo "output already exists: $output (pass --force to replace it)" >&2
    exit 1
fi

if ! jq -e '
    type == "object" and (.schema_version == 2) and
    ((.errors // {}) | length == 0) and
    (.run_id | type == "string" or type == "number") and
    (.reports | type == "array" and length > 0) and
    all(.reports[];
        type == "object" and
        (.case | type == "string" and length > 0) and
        (.run_id | type == "string" or type == "number") and
        (.derived | type == "object" and length > 0) and
        all(.derived[]; type == "number")
    ) and
    ([.reports[].case] | length) == ([.reports[].case] | unique | length)
' "$input" >/dev/null; then
    echo "invalid perf-lab suite report: $input" >&2
    exit 1
fi

output_dir=$(dirname "$output")
mkdir -p "$output_dir"
tmp=$(mktemp "${output}.tmp.XXXXXX")
trap 'rm -f "$tmp"' EXIT

jq '{
    schema_version,
    run_id: ("baseline-" + (.run_id | tostring)),
    reports: [
        .reports[] | {
            case,
            run_id: (.run_id | tostring),
            config,
            workloads: [],
            derived,
            metric_intervals: []
        }
    ]
}' "$input" >"$tmp"

chmod 0644 "$tmp"
mv "$tmp" "$output"
trap - EXIT

report_count=$(jq '.reports | length' "$output")
echo "perf-lab baseline: $output ($report_count cases)"
