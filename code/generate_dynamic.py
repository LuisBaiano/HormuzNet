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

def gerar_yaml(mode, count, lider_ip):
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
        for i in range(min(count, 8)):
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
        for i in range(count):
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
        # Gera 2 sensores para cada broker solicitado
        s_count = 1
        for i in range(min(count, 9)):
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
    args = parser.parse_args()

    gerar_yaml(args.mode, args.count, args.lider)
