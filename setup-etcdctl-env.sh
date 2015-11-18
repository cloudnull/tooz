#!/bin/bash
set -xe

ETCD_RELEASE="https://github.com/coreos/etcd/releases/download/v2.2.1/etcd-v2.2.1-linux-amd64.tar.gz"
ETCD_FILE_NAME="$(basename ${ETCD_RELEASE})"

function clean_exit(){
    local error_code="$?"
    local spawned=$(jobs -p)
    if [ -n "$spawned" ]; then
        kill $(jobs -p)
    fi
    return $error_code
}

trap "clean_exit" EXIT

if [ ! -d "${ETCD_FILE_NAME}" ];then
    curl -L ${ETCD_RELEASE} -o ${ETCD_FILE_NAME}
    tar -xvf ${ETCD_FILE_NAME}
fi

pushd "${ETCD_FILE_NAME%%.tar.gz}"
    ./etcd &
popd

export TOOZ_TEST_ETCD_URL="http://127.0.0.1:2379/v2"
# Yield execution to venv command
$*
