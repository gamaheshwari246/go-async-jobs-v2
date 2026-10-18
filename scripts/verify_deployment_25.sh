#!/usr/bin/env bash
# 25 executable smoke and functional checks for go-async-jobs.
# Runs 23 safe public API tests. Tests 24-25 additionally require SSH_HOST and
# RUN_PRIVILEGED=1 and use SSH, not the unauthenticated public API, for admin.
set -Eeuo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8082}"
BASE_URL="${BASE_URL%/}"
POLL_SECONDS="${POLL_SECONDS:-40}"
SSH_HOST="${SSH_HOST:-}"
RUN_PRIVILEGED="${RUN_PRIVILEGED:-0}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-/root/go-async-jobs}"

for command in curl jq awk; do
  if ! command -v "$command" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$command" >&2
    exit 2
  fi
done

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT
RESPONSE_FILE="$WORK_DIR/body"
HTTP_STATUS=''
HTTP_SECONDS=''
PASS=0
FAIL=0
SKIP=0
FIRST_JOB_ID=''
IDEM_KEY="demo-25-$(date +%s)-$$-$RANDOM"
IDEM_JOB_ID=''

request() {
  local method="$1" path="$2" data="${3:-}" key="${4:-}"
  local -a args=(--silent --show-error --connect-timeout 5 --max-time 15
    --output "$RESPONSE_FILE" --write-out '%{http_code} %{time_total}'
    --request "$method" "$BASE_URL$path")
  if [[ "$method" == 'POST' ]]; then
    args+=(-H 'Content-Type: application/json' --data "$data")
  fi
  if [[ -n "$key" ]]; then
    args+=(-H "Idempotency-Key: $key")
  fi
  local stats
  stats="$(curl "${args[@]}")" || return 1
  read -r HTTP_STATUS HTTP_SECONDS <<< "$stats"
}

require_status() {
  local expected="$1"
  if [[ "$HTTP_STATUS" != "$expected" ]]; then
    printf 'Expected HTTP %s, got %s; response: %s\n' "$expected" "$HTTP_STATUS" "$(head -c 250 "$RESPONSE_FILE")" >&2
    return 1
  fi
}

require_json() {
  if ! jq -e "$1" "$RESPONSE_FILE" >/dev/null; then
    printf 'JSON condition failed: %s; response: %s\n' "$1" "$(head -c 350 "$RESPONSE_FILE")" >&2
    return 1
  fi
}

poll_status() {
  local id="$1" expected="$2" timeout="${3:-$POLL_SECONDS}" start now last=''
  start=$(date +%s)
  while true; do
    request GET "/jobs/$id" || return 1
    require_status 200 || return 1
    last="$(jq -r '.status // "unknown"' "$RESPONSE_FILE")"
    [[ "$last" == "$expected" ]] && return 0
    if [[ "$last" == 'succeeded' || "$last" == 'dead_lettered' ]]; then
      printf 'Job %s reached %s, expected %s\n' "$id" "$last" "$expected" >&2
      return 1
    fi
    now=$(date +%s)
    if (( now - start >= timeout )); then
      printf 'Timed out waiting for %s; last status %s\n' "$id" "$last" >&2
      return 1
    fi
    sleep 0.3
  done
}

submit_job() {
  local payload="$1" key="${2:-}"
  request POST /jobs "$payload" "$key" || return 1
  require_status 202 || return 1
  require_json '.job_id | type == "string" and length > 0' || return 1
}

run_test() {
  local number="$1" label="$2" function="$3"
  printf '%02d/25 %-57s ' "$number" "$label"
  local code=0
  "$function" || code=$?
  if (( code == 0 )); then
    printf 'PASS\n'
    ((PASS+=1))
  elif (( code == 77 )); then
    printf 'SKIP\n'
    ((SKIP+=1))
  else
    printf 'FAIL\n'
    ((FAIL+=1))
  fi
}

# ---- 1 through 9: health and basic lifecycle ----
t01() { request GET /healthz && require_status 200 && require_json '.status == "up"'; }
t02() { request GET /readyz && require_status 200 && require_json '.status == "ready"'; }
t03() {
  submit_job '{"type":"demo.sleep","priority":9,"max_retries":0,"payload":{"duration_ms":100}}' || return 1
  FIRST_JOB_ID="$(jq -r '.job_id' "$RESPONSE_FILE")"
  require_json '.queue == "critical" and .status == "queued" and .priority == 9'
}
t04() {
  [[ -n "$FIRST_JOB_ID" ]] || { echo 'Test 3 did not return a job ID' >&2; return 1; }
  poll_status "$FIRST_JOB_ID" succeeded
}
t05() {
  [[ -n "$FIRST_JOB_ID" ]] || return 1
  request GET "/jobs/$FIRST_JOB_ID" && require_status 200 &&
    require_json '.status == "succeeded" and .attempts == 1 and .result.ok == true and .result.job_id == .job_id'
}
t06() {
  request GET /queue/depth && require_status 200 &&
    require_json '(.waiting | type) == "number" and (.active | type) == "number" and (.archived | type) == "number" and (.by_queue | has("low") and has("default") and has("critical"))'
}
t07() {
  local priority expected
  for priority in 2 5 9; do
    case "$priority" in 2) expected=low;; 5) expected=default;; 9) expected=critical;; esac
    submit_job "{\"type\":\"demo.sleep\",\"priority\":$priority,\"payload\":{\"duration_ms\":50}}" || return 1
    require_json ".queue == \"$expected\" and .priority == $priority" || return 1
  done
}
t08() {
  submit_job '{"type":"demo.sleep","payload":{"duration_ms":50}}' || return 1
  local id="$(jq -r '.job_id' "$RESPONSE_FILE")"
  poll_status "$id" succeeded || return 1
  request GET "/jobs/$id" && require_json '.priority == 5 and .max_retries == 3'
}
t09() {
  submit_job '{"type":"demo.sleep","payload":{"duration_ms":3000},"max_retries":0}' || return 1
  local id="$(jq -r '.job_id' "$RESPONSE_FILE")"
  awk -v n="$HTTP_SECONDS" 'BEGIN {exit !(n < 2.0)}' || {
    printf 'Submission took %ss; expected <2s for 3s handler\n' "$HTTP_SECONDS" >&2
    return 1
  }
  poll_status "$id" succeeded
}

# ---- 10 through 14: failures, retries, dead-letter and idempotent effect ----
t10() {
  submit_job '{"type":"demo.sleep","priority":9,"max_retries":3,"timeout_seconds":10,"payload":{"duration_ms":30,"fail_first":2}}' || return 1
  local id="$(jq -r '.job_id' "$RESPONSE_FILE")"
  poll_status "$id" succeeded || return 1
  require_json '.attempts == 3 and .max_retries == 3'
}
t11() {
  # Demonstrates the aggregate timing of 1s then 2s scheduled retries.
  # It does not assert exact dispatcher wake-up time.
  local start elapsed
  start=$(date +%s)
  submit_job '{"type":"demo.sleep","priority":9,"max_retries":2,"payload":{"duration_ms":20,"fail_first":2}}' || return 1
  local id="$(jq -r '.job_id' "$RESPONSE_FILE")"
  poll_status "$id" succeeded || return 1
  require_json '.attempts == 3' || return 1
  elapsed=$(( $(date +%s) - start ))
  if (( elapsed < 2 )); then
    printf 'Two backoffs finished in %ss, expected >=2s to show non-immediate retries\n' "$elapsed" >&2
    return 1
  fi
}
t12() {
  submit_job '{"type":"demo.sleep","max_retries":1,"payload":{"duration_ms":20,"fail_first":5}}' || return 1
  local id="$(jq -r '.job_id' "$RESPONSE_FILE")"
  poll_status "$id" dead_lettered || return 1
  require_json '.attempts == 2 and (.last_error | length > 0)'
}
t13() {
  submit_job '{"type":"demo.sleep","timeout_seconds":1,"max_retries":0,"payload":{"duration_ms":5000}}' || return 1
  local id="$(jq -r '.job_id' "$RESPONSE_FILE")"
  poll_status "$id" dead_lettered || return 1
  require_json '.attempts == 1'
}
t14() {
  submit_job '{"type":"demo.effect","max_retries":1,"payload":{}}' || return 1
  local id="$(jq -r '.job_id' "$RESPONSE_FILE")"
  poll_status "$id" succeeded || return 1
  require_json '.attempts == 2 and .result.ok == true and .result.effect_performed_on_this_attempt == false'
}

# ---- 15 through 17: HTTP duplicate-submission protection ----
t15() {
  local data='{"type":"demo.sleep","priority":5,"payload":{"duration_ms":80}}'
  submit_job "$data" "$IDEM_KEY" || return 1
  IDEM_JOB_ID="$(jq -r '.job_id' "$RESPONSE_FILE")"
  request POST /jobs "$data" "$IDEM_KEY" || return 1
  require_status 200 && require_json ".job_id == \"$IDEM_JOB_ID\" and .deduplicated == true"
}
t16() {
  [[ -n "$IDEM_JOB_ID" ]] || return 1
  request POST /jobs '{"type":"demo.sleep","priority":5,"payload":{"duration_ms":81}}' "$IDEM_KEY" && require_status 409
}
t17() {
  [[ -n "$IDEM_JOB_ID" ]] || return 1
  request POST /jobs '{"type":"demo.sleep","priority":9,"payload":{"duration_ms":80}}' "$IDEM_KEY" && require_status 409
}

# ---- 18 through 23: invalid API inputs ----
t18() { request POST /jobs '{"type":"demo.sleep","priority":10,"payload":{}}' && require_status 400; }
t19() { request POST /jobs '{"type":"demo.sleep","max_retries":11,"payload":{}}' && require_status 400; }
t20() { request POST /jobs '{"type":"demo.sleep","timeout_seconds":301,"payload":{}}' && require_status 400; }
t21() { request POST /jobs '{"type":"unknown.handler","payload":{}}' && require_status 400; }
t22() { request POST /jobs '{"type":"demo.sleep","payload":[]}' && require_status 400; }
t23() { request GET '/jobs/unknown-job-id-25-suite' && require_status 404; }

remote_available() {
  if [[ "$RUN_PRIVILEGED" != '1' || -z "$SSH_HOST" ]]; then
    printf '\n       Requires RUN_PRIVILEGED=1 SSH_HOST=root@DROPLET_IP; ' >&2
    return 1
  fi
  if ! command -v ssh >/dev/null; then
    printf '\n       SSH is unavailable; ' >&2
    return 1
  fi
}

# ---- 24: verify independently scaled workers. Read-only remote inspection. ----
t24() {
  remote_available || return 77
  ssh -o BatchMode=yes -o ConnectTimeout=7 "$SSH_HOST" bash -s -- "$REMOTE_APP_DIR" <<'REMOTE'
set -euo pipefail
cd "$1"
api_count=$(docker compose ps -q api | wc -l)
worker_count=$(docker compose ps -q worker | wc -l)
printf '\n       Remote containers: API=%s workers=%s (expected API=1, workers>=4)\n       ' "$api_count" "$worker_count"
[[ "$api_count" -eq 1 && "$worker_count" -ge 4 ]]
REMOTE
}

# ---- 25: authenticated drain/resume over SSH to localhost only. ----
t25() {
  remote_available || return 77
  ssh -o BatchMode=yes -o ConnectTimeout=7 "$SSH_HOST" bash -s -- "$REMOTE_APP_DIR" <<'REMOTE'
set -euo pipefail
cd "$1"
api_container=$(docker compose ps -q api)
[[ -n "$api_container" ]] || { echo 'Missing API container' >&2; exit 1; }
# Fetch token on the remote host only; never print or send it over public HTTP.
token=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$api_container" | sed -n 's/^ADMIN_TOKEN=//p')
if [[ -z "$token" || "$token" == 'local-dev-token' ]]; then
  echo 'Refusing privileged test with missing/default admin token. Set a private ADMIN_TOKEN and redeploy first.' >&2
  exit 77
fi
body='{"type":"demo.sleep","payload":{"duration_ms":10}}'
drain_enabled=0
resume() {
  if [[ "$drain_enabled" -eq 1 ]]; then
    curl -sS --max-time 8 -o /dev/null -X POST 'http://127.0.0.1:8080/admin/resume' -H "Authorization: Bearer $token" || true
  fi
}
trap resume EXIT
status=$(curl -sS --max-time 8 -o /dev/null -w '%{http_code}' -X POST 'http://127.0.0.1:8080/admin/drain' -H "Authorization: Bearer $token")
[[ "$status" == 202 ]] || { echo "Drain expected 202, got $status" >&2; exit 1; }
drain_enabled=1
status=$(curl -sS --max-time 8 -o /dev/null -w '%{http_code}' -X POST 'http://127.0.0.1:8080/jobs' -H 'Content-Type: application/json' --data "$body")
[[ "$status" == 503 ]] || { echo "Drain admission expected 503, got $status" >&2; exit 1; }
status=$(curl -sS --max-time 8 -o /dev/null -w '%{http_code}' -X POST 'http://127.0.0.1:8080/admin/resume' -H "Authorization: Bearer $token")
[[ "$status" == 200 ]] || { echo "Resume expected 200, got $status" >&2; exit 1; }
drain_enabled=0
status=$(curl -sS --max-time 8 -o /dev/null -w '%{http_code}' -X POST 'http://127.0.0.1:8080/jobs' -H 'Content-Type: application/json' --data "$body")
[[ "$status" == 202 ]] || { echo "Post-resume submission expected 202, got $status" >&2; exit 1; }
REMOTE
}

printf '25-test deployment runner for %s\n' "$BASE_URL"
printf 'Tests 24-25 require explicit SSH opt-in. No destructive Redis/fault-injection tests.\n\n'

run_test 1  'API liveness GET /healthz' t01
run_test 2  'API readiness GET /readyz' t02
run_test 3  'Job submission returns 202 and job ID' t03
run_test 4  'Job completes successfully' t04
run_test 5  'Result and first-attempt count' t05
run_test 6  'Queue depth and per-priority statistics' t06
run_test 7  'Priority routes to low/default/critical' t07
run_test 8  'Default priority and retry settings' t08
run_test 9  'Async POST returns before 3-second work' t09
run_test 10 'Transient failures recover after 2 retries' t10
run_test 11 'Exponential retry backoff has observable delay' t11
run_test 12 'Exhausted retries move job to dead letter' t12
run_test 13 'Per-attempt timeout moves job to dead letter' t13
run_test 14 'Redis-local effect committed just once' t14
run_test 15 'Matching Idempotency-Key returns same job ID' t15
run_test 16 'Idempotency-Key conflicting payload returns 409' t16
run_test 17 'Idempotency-Key cross-queue reuse returns 409' t17
run_test 18 'Invalid priority rejected with 400' t18
run_test 19 'Invalid retry limit rejected with 400' t19
run_test 20 'Invalid timeout rejected with 400' t20
run_test 21 'Unknown handler rejected with 400' t21
run_test 22 'Non-object payload rejected with 400' t22
run_test 23 'Unknown job returns 404' t23
run_test 24 'One API with four independently scaled workers' t24
run_test 25 'Authenticated admin drain/reject/resume over SSH' t25

printf '\nRESULT: %d passed, %d failed, %d skipped (25 total)\n' "$PASS" "$FAIL" "$SKIP"
if (( FAIL > 0 )); then exit 1; fi
