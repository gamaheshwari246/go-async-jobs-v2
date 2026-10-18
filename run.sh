#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose is required. Enable Docker Desktop WSL integration or install Docker Engine with Compose." >&2
  exit 1
fi

usage() {
  cat <<'EOF'
Usage: bash run.sh [command]

  start    Build and start API, scheduler, two workers, and Redis (default)
  build    Compile Docker images without starting the services
  test     Run gofmt check, unit/integration tests, vet, and build in Docker
  logs     Follow API and worker logs
  status   Show running containers
  stop     Stop services (preserves Redis data)
  help     Show this help

No local Go installation is required. Docker Compose must work in Ubuntu/WSL. V2 uses project go-async-jobs-v2 and host API port 8082.
EOF
}

case "${1:-start}" in
  start)
    docker compose --profile scheduler up --build -d --scale worker=2
    echo
    echo "V2 API: http://localhost:8082/healthz"
    echo "Logs: bash run.sh logs"
    ;;
  build)
    docker compose build api worker
    ;;
  test)
    docker compose --profile test run --rm test
    ;;
  logs)
    docker compose logs -f api worker
    ;;
  status)
    docker compose ps
    ;;
  stop)
    docker compose down
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
