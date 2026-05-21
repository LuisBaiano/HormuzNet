# ═══════════════════════════════════════════════════════════════════════════════
# HormuzNet — generate_dynamic.py
# Gerador dinâmico de docker-compose para implantação distribuída (multi-host).
# Chamado pelo menu.sh para criar um docker-compose-temp.yml contendo apenas
# os serviços selecionados para o PC atual, apontando para o IP do Broker Líder.
#
# Modos disponíveis (--mode):
#   lider    → Gera apenas o Broker Líder (B9 / Setor_Centro)
#   brokers  → Gera N brokers seguidores (B1–B8) conectados ao líder remoto
#   monitor  → Gera o Monitor (dashboard web na porta 8085)
#   drones   → Gera N drones autônomos conectados ao líder remoto
#   sensores → Gera 2 sensores por setor solicitado (transmissão via UDP Multicast)
#
# Argumentos:
#   --mode   Modo de geração (obrigatório)
#   --count  Quantidade de instâncias (default: 1)
#   --lider  IP do Broker Líder remoto (ex: 192.168.1.10)
#
# Saída: docker-compose-temp.yml na raiz do projeto
# ═══════════════════════════════════════════════════════════════════════════════
import argparse
import random
import yaml
import os
import socket
import json
import time

def detectar_ativos(lider_ip):
    # Retorna (portas_ativas, drones_ativos)
    portas_ativas = set()
    drones_ativos = set()
    if not lider_ip:
        return portas_ativas, drones_ativos

    # Se informou lider, assume que a porta dele (6008) está ativa
    portas_ativas.add(6008)

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1.0)
        s.connect((lider_ip, 6008))

        # Envia registro como monitor temporário para receber peer list e sincronização de drones
        msg = {
            "tipo": "REGISTRO",
            "broker_id": "MONITOR-AUTO-DISCOVER"
        }
        s.sendall((json.dumps(msg) + "\n").encode('utf-8'))

        # Lendo respostas
        data = b""
        start_time = time.time()
        while time.time() - start_time < 1.0:
            try:
                chunk = s.recv(4096)
                if not chunk:
                    break
                data += chunk
                lines = data.split(b"\n")
                # Mantém a última linha incompleta no buffer
                data = lines[-1]
                for line in lines[:-1]:
                    if not line:
                        continue
                    try:
                        res = json.loads(line.decode('utf-8'))
                        # Verifica se é SINC_DRONE
                        if res.get("tipo") == "SINC_DRONE" and "drone" in res:
                            drone_info = res["drone"]
                            drone_id = drone_info.get("drone_id")
                            if drone_id:
                                parts = drone_id.split("_")
                                if parts:
                                    try:
                                        drones_ativos.add(int(parts[-1]))
                                    except ValueError:
                                        pass
                        # Verifica se é PEER_LIST
                        elif res.get("tipo") == "PEER_LIST" and "peers" in res:
                            peers = res["peers"]
                            for peer in peers:
                                if ":" in peer:
                                    try:
                                        port = int(peer.split(":")[-1])
                                        portas_ativas.add(port)
                                    except ValueError:
                                        pass
                    except json.JSONDecodeError:
                        pass
            except socket.timeout:
                break
        s.close()
    except Exception:
        pass

    return portas_ativas, drones_ativos

def sugerir_start(mode, count, lider_ip):
    portas_ativas, drones_ativos = detectar_ativos(lider_ip)
    
    if mode == "brokers" or mode == "sensores":
        active_indices = set()
        for p in portas_ativas:
            if 6000 <= p <= 6007:
                active_indices.add(p - 6000 + 1)
        
        # Acha primeiro bloco contíguo de tamanho count livre
        for i in range(1, 9):
            if i + count - 1 > 8:
                break
            if all(j not in active_indices for j in range(i, i + count)):
                return i
        # Se não achou bloco contíguo, retorna o primeiro índice livre isolado
        for i in range(1, 9):
            if i not in active_indices:
                return i
        return 1
        
    elif mode == "drones":
        i = 1
        while True:
            if all(j not in drones_ativos for j in range(i, i + count)):
                return i
            i += 1
            if i > 100:
                break
        return i
        
    return 1

def gerar_yaml(mode, count, lider_ip, start_index=-1):
    if start_index == -1:
        start_index = sugerir_start(mode, count, lider_ip)

    services = {}

    brokers_base = [
        {"id": "B1", "setor": "Setor_Noroeste", "port": 6000},
        {"id": "B2", "setor": "Setor_Norte",    "port": 6001},
        {"id": "B3", "setor": "Setor_Nordeste", "port": 6002},
        {"id": "B4", "setor": "Setor_Leste",    "port": 6003},
        {"id": "B5", "setor": "Setor_Sudeste",  "port": 6004},
        {"id": "B6", "setor": "Setor_Sul",      "port": 6005},
        {"id": "B7", "setor": "Setor_Sudoeste", "port": 6006},
        {"id": "B8", "setor": "Setor_Oeste",    "port": 6007},
        {"id": "B9", "setor": "Setor_Centro",   "port": 6008}
    ]

    drones_base = [
        {"x": 250, "y": 250, "nome": "nw"},
        {"x": 500, "y": 250, "nome": "n"},
        {"x": 750, "y": 250, "nome": "ne"},
        {"x": 750, "y": 500, "nome": "e"},
        {"x": 250, "y": 750, "nome": "sw"},
        {"x": 750, "y": 750, "nome": "se"},
        {"x": 500, "y": 500, "nome": "c"}
    ]
    
    types = ["radar", "sonar", "boia", "visual", "meteo"]

    if mode == "lider":
        b = brokers_base[8] # B9
        services["broker9"] = {
            "build": {"context": "./code", "dockerfile": "Dockerfile.broker"},
            "container_name": "hormuznet_broker9",
            "network_mode": "host",
            "command": [f"-id={b['id']}", f"-setor={b['setor']}", "-udp=224.1.2.3:9876", f"-tcp=0.0.0.0:{b['port']}"],
            "restart": "on-failure"
        }

    elif mode == "brokers":
        for i in range(start_index - 1, min(start_index - 1 + count, 8)):
            b = brokers_base[i]
            services[f"broker{i+1}"] = {
                "build": {"context": "./code", "dockerfile": "Dockerfile.broker"},
                "container_name": f"hormuznet_broker{i+1}",
                "network_mode": "host",
                "command": [f"-id={b['id']}", f"-setor={b['setor']}", "-udp=224.1.2.3:9876", f"-tcp=0.0.0.0:{b['port']}", f"-lider={lider_ip}:6008"],
                "restart": "on-failure"
            }

    elif mode == "monitor":
        services["monitor"] = {
            "build": {"context": "./code", "dockerfile": "Dockerfile.monitor"},
            "container_name": "hormuznet_monitor",
            "network_mode": "host",
            "command": [f"-brokers={lider_ip}:6008", "-porta=8085"],
            "restart": "on-failure"
        }

    elif mode == "drones":
        for i in range(start_index - 1, start_index - 1 + count):
            d = drones_base[i % len(drones_base)]
            id_drone = f"Drone_{d['nome'].upper()}_{i+1}"
            services[f"drone{i+1}"] = {
                "build": {"context": "./code", "dockerfile": "Dockerfile.drone"},
                "container_name": f"hormuznet_{id_drone.lower()}",
                "network_mode": "host",
                "command": [f"-id={id_drone}", f"-brokers={lider_ip}:6008", f"-x={d['x']}", f"-y={d['y']}"],
                "restart": "on-failure"
            }

    elif mode == "sensores":
        # Gera 2 sensores para cada broker solicitado a partir de start_index
        s_count = (start_index - 1) * 2 + 1
        for i in range(start_index - 1, min(start_index - 1 + count, 9)):
            b = brokers_base[i]
            for j in range(2):
                t = random.choice(types)
                x = random.randint(100, 900)
                y = random.randint(100, 900)
                id_sensor = f"{t}_{b['setor'].split('_')[1].lower()}_{s_count}"
                
                services[f"sensor_{s_count}"] = {
                    "build": {"context": "./code", "dockerfile": "Dockerfile.sensor"},
                    "container_name": f"hormuznet_{id_sensor}",
                    "network_mode": "host",
                    "command": [f"-id={id_sensor}", f"-tipo={t}", f"-setor={b['setor']}", "-broker=224.1.2.3:9876", "-intervalo=20000", f"-x={x}", f"-y={y}"],
                    "restart": "on-failure"
                }
                s_count += 1

    compose_dict = {
        "version": "3.8",
        "services": services
    }

    with open("docker-compose-temp.yml", "w") as f:
        yaml.dump(compose_dict, f, sort_keys=False, default_flow_style=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gera docker-compose dinâmico para HormuzNet")
    parser.add_argument("--mode", choices=["lider", "brokers", "monitor", "drones", "sensores"], required=True)
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--lider", type=str, default="")
    parser.add_argument("--start", type=int, default=-1, help="ID/Indice de inicio da numeracao (-1 para autodetectar)")
    args = parser.parse_args()

    gerar_yaml(args.mode, args.count, args.lider, args.start)
