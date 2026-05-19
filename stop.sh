docker rm -f $(docker ps -aq) 2>/dev/null || true
pkill -9 -f "go run ./cmd/|.*_main|echo '--- encerrado ---'" 2>/dev/null || true
