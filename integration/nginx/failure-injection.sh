#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
test_dir=$(mktemp -d)

compose() {
    docker compose -f "$script_dir/docker-compose.yml" "$@"
}

cleanup() {
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$test_dir"
}
trap cleanup EXIT INT TERM

wait_for_url() {
    url=$1
    attempts=0
    until curl --fail --silent --output /dev/null "$url"; do
        attempts=$((attempts + 1))
        if [ "$attempts" -ge 60 ]; then
            echo "timed out waiting for $url" >&2
            return 1
        fi
        sleep 0.25
    done
}

request_count() {
    id=$1
    curl --fail --silent "http://127.0.0.1:18081/admin/count?id=$id" | sed -n 's/.*"count":\([0-9][0-9]*\).*/\1/p'
}

compose up --build --detach
wait_for_url http://127.0.0.1:18081/ready
wait_for_url http://127.0.0.1:18080/ready

status=$(curl --silent --output "$test_dir/application-500.body" --write-out '%{http_code}' \
    --request POST --header 'X-Test-ID: application-500' --data 'body' \
    http://127.0.0.1:18080/application-500)
test "$status" = "500"
test "$(request_count application-500)" = "1"

compose stop backend
curl --silent --output "$test_dir/deployment.body" --write-out '%{http_code}' \
    --request POST --header 'X-Test-ID: deployment' --data 'held-body' \
    http://127.0.0.1:18080/deployment >"$test_dir/deployment.status" &
held_pid=$!
sleep 1
if ! kill -0 "$held_pid" 2>/dev/null; then
    echo "deployment request completed instead of waiting for readiness" >&2
    exit 1
fi
compose start backend
wait_for_url http://127.0.0.1:18081/ready
wait "$held_pid"
test "$(cat "$test_dir/deployment.status")" = "201"
test "$(cat "$test_dir/deployment.body")" = "held-body"
test "$(request_count deployment)" = "1"

status=$(curl --silent --output "$test_dir/ambiguous.body" --write-out '%{http_code}' \
    --request POST --header 'X-Test-ID: ambiguous' --data 'processed-body' \
    http://127.0.0.1:18080/ambiguous)
test "$status" = "502"
test "$(request_count ambiguous)" = "1"

echo "Nginx failure-injection checks passed"
