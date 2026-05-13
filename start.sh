#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# HormuzNet — start.sh
#
# Sobe todos os componentes em terminais separados (gnome-terminal).
#
# Uso:
#   ./start.sh                        # localhost, configuração padrão
#   ./start.sh 192.168.1.10           # Máquina A — sobe brokers + bases
#   ./start.sh 192.168.1.10 sensor    # Máquina B — sobe só os sensores
#
# Pré-requisito: go build ./... já executado
# ═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

MODE="${2:-all}"        # all | broker | base | sensor
IP_A="${1:-localhost}"  # IP da Máquina A (brokers)
IP_B="${1:-localhost}"  # IP da Máquina B (bases/sensores) — mesmo IP se local

# ── Detecta o binário Go ───────────────────────────────────────────────────────
if ! command -v go &>/dev/null; then
  echo "[ERRO] 'go' não encontrado no PATH"
  exit 1
fi

# ── Detecta emulador de terminal disponível ───────────────────────────────────
open_term() {
  local title="$1"
  shift
  local cmd="$*"

  if command -v gnome-terminal &>/dev/null; then
    gnome-terminal --title="$title" -- bash -c "$cmd; echo '--- encerrado ---'; read" &
  elif command -v xterm &>/dev/null; then
    xterm -title "$title" -e bash -c "$cmd; echo '--- encerrado ---'; read" &
  elif command -v konsole &>/dev/null; then
    konsole --new-tab -p tabtitle="$title" -e bash -c "$cmd; echo '--- encerrado ---'; read" &
  else
    echo "[AVISO] Nenhum emulador de terminal encontrado. Rodando em background: $title"
    bash -c "$cmd" > "logs/${title}.log" 2>&1 &
    echo "  → log: logs/${title}.log"
  fi
}

ROOT="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$ROOT/logs"
cd "$ROOT"

echo "╔══════════════════════════════════════════════════════════╗"
echo "║              HormuzNet — Inicialização                   ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "  Modo   : $MODE"
echo "  IP_A   : $IP_A"
echo "  Projeto: $ROOT"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── Brokers ───────────────────────────────────────────────────────────────────
start_brokers() {
  echo "→ Subindo Broker 1 (Setor_Norte udp=224.0.0.1:8080 tcp=6000)..."
  open_term "Broker-B1" \
    "cd '$ROOT' && go run ./cmd/broker/ \
      -id B1 \
      -setor Setor_Norte \
      -udp 224.0.0.1:8080 \
      -tcp 0.0.0.0:6000 \
      -vizinhos ${IP_A}:6001"

  sleep 1

  echo "→ Subindo Broker 2 (Setor_Sul udp=224.0.0.1:8080 tcp=6001)..."
  open_term "Broker-B2" \
    "cd '$ROOT' && go run ./cmd/broker/ \
      -id B2 \
      -setor Setor_Sul \
      -udp 224.0.0.1:8080 \
      -tcp 0.0.0.0:6001 \
      -vizinhos ${IP_A}:6000"

  sleep 1
}

# ── Drones ────────────────────────────────────────────────────────────────────
start_drones() {
  echo "→ Subindo Drone Norte 1..."
  open_term "Drone-Norte1" \
    "cd '$ROOT' && go run ./cmd/drone/ \
      -id Drone_Norte1 \
      -brokers ${IP_A}:6000,${IP_A}:6001 \
      -x 150 -y 150"

  sleep 1

  echo "→ Subindo Drone Sul 1..."
  open_term "Drone-Sul1" \
    "cd '$ROOT' && go run ./cmd/drone/ \
      -id Drone_Sul1 \
      -brokers ${IP_A}:6001,${IP_A}:6000 \
      -x 350 -y 350"

  sleep 1
}

# ── Sensores ──────────────────────────────────────────────────────────────────
start_sensores() {
  echo "→ Subindo sensores (Multicast UDP)..."

  open_term "Sensor-radar" \
    "cd '$ROOT' && go run ./cmd/sensor/ \
      -id radar_norte_01 -tipo radar \
      -setor Setor_Norte \
      -broker 224.0.0.1:8080 \
      -intervalo 1000 -x 100 -y 100"

  open_term "Sensor-boia" \
    "cd '$ROOT' && go run ./cmd/sensor/ \
      -id boia_sul_01 -tipo boia \
      -setor Setor_Sul \
      -broker 224.0.0.1:8080 \
      -intervalo 1000 -x 400 -y 400"
}

# ── Execução por modo ─────────────────────────────────────────────────────────
case "$MODE" in
  all)
    start_brokers
    sleep 2
    start_drones
    sleep 2
    start_sensores
    ;;
  broker)
    start_brokers
    ;;
  drone)
    start_drones
    ;;
  sensor)
    start_sensores
    ;;
  *)
    echo "[ERRO] Modo inválido: $MODE"
    echo "Modos válidos: all | broker | drone | sensor"
    exit 1
    ;;
esac

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "  Componentes iniciados!"
echo ""
echo "  Para testar destruição de broker:"
echo "    Feche a janela 'Broker-B1' — Base Norte reconecta em B2"
echo ""
echo "  Para parar tudo:"
echo "    ./stop.sh"
echo "╚══════════════════════════════════════════════════════════╝"
