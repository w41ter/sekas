#!/bin/bash
set -euo pipefail

# See https://github.com/etcd-io/etcd/blob/main/etcdctl/README.md.

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

# clear all data before testing.
etcdctl del --range a z >/dev/null 2>&1

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

# === txn releated operations ===

echo "begin txn releated operations ..."

# put if not exists
#
# CreateRevision 0 means key was not exists
# See https://github.com/etcd-io/etcd/issues/6740 for details.
etcdctl txn <<EOF
c("foo") = "0"

put bar foo


EOF
etcd_verify get bar "bar\nfoo"

# test version
etcdctl txn <<EOF
ver("foo") = "0"
ver("bar") = "1"
ver("bar") > "0"
ver("foo") < "1"

get bar


EOF

# delete if exists
etcdctl txn <<EOF
m("bar") > "0"

del bar


EOF
etcd_verify get bar ""

# batch put and get
etcdctl txn <<EOF
c("foo") = "0"
c("bar") = "0"

put far bar
put foo boo


EOF

etcd_verify get foo "foo\nboo"
etcd_verify get far "far\nbar"

# failure
etcdctl txn <<EOF
c("foo") = "0"

put success success

put failure failure

EOF

etcd_verify get success ""
etcd_verify get failure "failure\nfailure"
etcd_verify del --range a z "0"
