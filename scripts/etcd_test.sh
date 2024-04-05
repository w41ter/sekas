#!/bin/bash
set -euo pipefail

function etcd_verify() {
    local args=("$@")
    local expect=${args[-1]}
    unset args[-1]

    local inputs=$(echo "${args[@]}")
    printf "etcdctl %-64s\n" "${inputs}"
    # diff -E -Z -b -w -B <(etcdctl ${args[@]}) <(echo -e "${expect}")
    if diff -E -Z -b -w -B <(etcdctl ${args[@]}) <(echo -e "${expect}"); then
        # move the cursor to previous lines.
        printf '\033[1A'
        printf "etcdctl %-64s [PASS]\n" "${inputs}"
    else
        exit -1
    fi
}

# === put related operations ===

# put
etcd_verify put foo bar "OK"
etcd_verify put far boo "OK"

# === get related operations ===

# get
etcd_verify get foo "foo\nbar"
etcd_verify get far "far\nboo"
etcd_verify get bar ""

# get by prefix
etcd_verify get f --prefix "far\nboo\nfoo\nbar"

# get by range
etcd_verify get a z "far\nboo\nfoo\nbar"

# get by range without from key
# FIXME(walter):
# etcd_verify get --from-key=false far z "foo\nbar"

# get by range with limit
etcd_verify get --limit=1 far z "far\nboo"

# get by range with keys only
etcd_verify get --keys-only=true far z "far\nfoo"

# === delete related operations ===

# delete
etcd_verify put test test "OK"
etcd_verify put k1 value1 "OK"
etcd_verify put k2 value2 "OK"

# delete with prefix
etcd_verify del k --prefix "0"
etcd_verify del test "0"

etcd_verify get test ""
etcd_verify get k1 ""
etcd_verify get k2 ""

# delete entire range
etcd_verify del --range a z "0"
etcd_verify get a z ""
