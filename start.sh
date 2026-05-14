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
  echo "→ Subindo Brokers..."
  open_term "Broker-B1" "cd '$ROOT' && go run ./cmd/broker/ -id B1 -setor Setor_Noroeste -udp 224.0.0.1:8080 -tcp 0.0.0.0:6000 -vizinhos ${IP_A}:6001,${IP_A}:6007"
  sleep 0.5
  open_term "Broker-B2" "cd '$ROOT' && go run ./cmd/broker/ -id B2 -setor Setor_Norte -udp 224.0.0.1:8080 -tcp 0.0.0.0:6001 -vizinhos ${IP_A}:6000,${IP_A}:6002"
  sleep 0.5
  open_term "Broker-B3" "cd '$ROOT' && go run ./cmd/broker/ -id B3 -setor Setor_Nordeste -udp 224.0.0.1:8080 -tcp 0.0.0.0:6002 -vizinhos ${IP_A}:6001,${IP_A}:6003"
  sleep 0.5
  open_term "Broker-B4" "cd '$ROOT' && go run ./cmd/broker/ -id B4 -setor Setor_Leste -udp 224.0.0.1:8080 -tcp 0.0.0.0:6003 -vizinhos ${IP_A}:6002,${IP_A}:6004"
  sleep 0.5
  open_term "Broker-B5" "cd '$ROOT' && go run ./cmd/broker/ -id B5 -setor Setor_Sudeste -udp 224.0.0.1:8080 -tcp 0.0.0.0:6004 -vizinhos ${IP_A}:6003,${IP_A}:6005"
  sleep 0.5
  open_term "Broker-B6" "cd '$ROOT' && go run ./cmd/broker/ -id B6 -setor Setor_Sul -udp 224.0.0.1:8080 -tcp 0.0.0.0:6005 -vizinhos ${IP_A}:6004,${IP_A}:6006"
  sleep 0.5
  open_term "Broker-B7" "cd '$ROOT' && go run ./cmd/broker/ -id B7 -setor Setor_Sudoeste -udp 224.0.0.1:8080 -tcp 0.0.0.0:6006 -vizinhos ${IP_A}:6005,${IP_A}:6007,${IP_A}:6008"
  sleep 0.5
  open_term "Broker-B8" "cd '$ROOT' && go run ./cmd/broker/ -id B8 -setor Setor_Oeste -udp 224.0.0.1:8080 -tcp 0.0.0.0:6007 -vizinhos ${IP_A}:6006,${IP_A}:6000,${IP_A}:6008"
  sleep 0.5
  open_term "Broker-B9" "cd '$ROOT' && go run ./cmd/broker/ -id B9 -setor Setor_Centro -udp 224.0.0.1:8080 -tcp 0.0.0.0:6008 -vizinhos ${IP_A}:6000,${IP_A}:6002,${IP_A}:6004,${IP_A}:6006"
  sleep 1
}

# ── Drones ────────────────────────────────────────────────────────────────────
start_drones() {
  echo "→ Subindo Drones..."
  open_term "Drone-NW" "cd '$ROOT' && go run ./cmd/drone/ -id Drone_NW_1 -brokers ${IP_A}:6000,${IP_A}:6007 -x 250 -y 250"
  open_term "Drone-N"  "cd '$ROOT' && go run ./cmd/drone/ -id Drone_N_1 -brokers ${IP_A}:6001,${IP_A}:6002 -x 500 -y 250"
  open_term "Drone-NE" "cd '$ROOT' && go run ./cmd/drone/ -id Drone_NE_1 -brokers ${IP_A}:6002,${IP_A}:6003 -x 750 -y 250"
  open_term "Drone-E"  "cd '$ROOT' && go run ./cmd/drone/ -id Drone_E_1 -brokers ${IP_A}:6003,${IP_A}:6004 -x 750 -y 500"
  open_term "Drone-SW" "cd '$ROOT' && go run ./cmd/drone/ -id Drone_SW_1 -brokers ${IP_A}:6006,${IP_A}:6005 -x 250 -y 750"
  open_term "Drone-SE" "cd '$ROOT' && go run ./cmd/drone/ -id Drone_SE_1 -brokers ${IP_A}:6004,${IP_A}:6005 -x 750 -y 750"
  open_term "Drone-C"  "cd '$ROOT' && go run ./cmd/drone/ -id Drone_C_1 -brokers ${IP_A}:6008,${IP_A}:6000 -x 500 -y 500"
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
      -intervalo 20000 -x 100 -y 100"

  open_term "Sensor-boia" \
    "cd '$ROOT' && go run ./cmd/sensor/ \
      -id boia_sul_01 -tipo boia \
      -setor Setor_Sul \
      -broker 224.0.0.1:8080 \
      -intervalo 20000 -x 400 -y 400"

  open_term "Sensor-Centro" \
    "cd '$ROOT' && go run ./cmd/sensor/ \
      -id Sensor_Centro -tipo visual -setor Setor_Centro -broker 224.0.0.1:8080 \
      -intervalo 20000 -x 500 -y 500"
}

# ── Observadores ──────────────────────────────────────────────────────────────
start_observers() {
  echo "→ Subindo Observadores CLI (Terminais separados)..."
  open_term "Obs-Noroeste" "cd '$ROOT' && go run ./cmd/observer/ -setor Setor_Noroeste -broker 224.0.0.1:8080"
  open_term "Obs-Norte"    "cd '$ROOT' && go run ./cmd/observer/ -setor Setor_Norte -broker 224.0.0.1:8080"
  open_term "Obs-Nordeste" "cd '$ROOT' && go run ./cmd/observer/ -setor Setor_Nordeste -broker 224.0.0.1:8080"
  open_term "Obs-Leste"    "cd '$ROOT' && go run ./cmd/observer/ -setor Setor_Leste -broker 224.0.0.1:8080"
  open_term "Obs-Sudeste"  "cd '$ROOT' && go run ./cmd/observer/ -setor Setor_Sudeste -broker 224.0.0.1:8080"
  open_term "Obs-Sul"      "cd '$ROOT' && go run ./cmd/observer/ -setor Setor_Sul -broker 224.0.0.1:8080"
  open_term "Obs-Sudoeste" "cd '$ROOT' && go run ./cmd/observer/ -setor Setor_Sudoeste -broker 224.0.0.1:8080"
  open_term "Obs-Oeste"    "cd '$ROOT' && go run ./cmd/observer/ -setor Setor_Oeste -broker 224.0.0.1:8080"
  open_term "Obs-Centro"   "cd '$ROOT' && go run ./cmd/observer/ -setor Setor_Centro -broker 224.0.0.1:8080"
}

# ── Execução por modo ─────────────────────────────────────────────────────────
case "$MODE" in
  all)
    start_brokers
    sleep 2
    start_drones
    sleep 2
    start_sensores
    sleep 2
    start_observers
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
  observer)
    start_observers
    ;;
  *)
    echo "[ERRO] Modo inválido: $MODE"
    echo "Modos válidos: all | broker | drone | sensor | observer"
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
