#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# HormuzNet — stop.sh
# Encerra todos os processos broker, base e sensor em execução.
# ═══════════════════════════════════════════════════════════════════════════════

echo "Encerrando processos HormuzNet..."

KILLED=0

for PROC in broker_main base_main sensor_main; do
  PIDS=$(pgrep -f "go run ./cmd/${PROC%_main}/" 2>/dev/null || true)
  PIDS2=$(pgrep -f "${PROC}" 2>/dev/null || true)
  for PID in $PIDS $PIDS2; do
    if kill "$PID" 2>/dev/null; then
      echo "  Encerrado PID $PID ($PROC)"
      KILLED=$((KILLED + 1))
    fi
  done
done

if [ "$KILLED" -eq 0 ]; then
  echo "  Nenhum processo HormuzNet encontrado."
else
  echo "  $KILLED processo(s) encerrado(s)."
fi
