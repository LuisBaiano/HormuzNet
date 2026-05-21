# ═══════════════════════════════════════════════════════════════════════════════
# HormuzNet — generate_all.py
# Gerador de docker-compose completo (ambiente monolítico / tudo em um só PC).
# Cria o arquivo docker-compose-all.yml com TODOS os serviços da simulação:
#   - 1 Broker Líder (B9 / Setor_Centro)
#   - 8 Brokers Seguidores (B1–B8)
#   - 1 Monitor (dashboard web na porta 8085)
#   - 7 Drones distribuídos pelos setores cardinais e central
#   - 18 Sensores (2 por broker), com tipo e posição gerados aleatoriamente
# Útil para testes locais completos sem distribuição entre múltiplos hosts.
# Uso: python3 generate_all.py   →  gera docker-compose-all.yml na raiz
# ═══════════════════════════════════════════════════════════════════════════════
import random
import yaml

def gerar_yaml_completo():
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

    # 1. Lider (B9)
    b = brokers_base[8]
    services["broker9"] = {
        "build": {"context": "./code", "dockerfile": "Dockerfile.broker"},
        "container_name": "hormuznet_broker9",
        "network_mode": "host",
        "command": [f"-id={b['id']}", f"-setor={b['setor']}", "-udp=224.1.2.3:9876", f"-tcp=0.0.0.0:{b['port']}"],
        "restart": "on-failure"
    }

    # 2. Seguidores (B1-B8)
    for i in range(8):
        b = brokers_base[i]
        services[f"broker{i+1}"] = {
            "build": {"context": "./code", "dockerfile": "Dockerfile.broker"},
            "container_name": f"hormuznet_broker{i+1}",
            "network_mode": "host",
            "command": [f"-id={b['id']}", f"-setor={b['setor']}", "-udp=224.1.2.3:9876", f"-tcp=0.0.0.0:{b['port']}", "-lider=127.0.0.1:6008"],
            "restart": "on-failure"
        }

    # 3. Monitor
    services["monitor"] = {
        "build": {"context": "./code", "dockerfile": "Dockerfile.monitor"},
        "container_name": "hormuznet_monitor",
        "network_mode": "host",
        "command": ["-brokers=127.0.0.1:6008", "-porta=8085"],
        "restart": "on-failure"
    }

    # 4. Drones (7 drones)
    for i in range(7):
        d = drones_base[i % len(drones_base)]
        id_drone = f"Drone_{d['nome'].upper()}_{i+1}"
        services[f"drone{i+1}"] = {
            "build": {"context": "./code", "dockerfile": "Dockerfile.drone"},
            "container_name": f"hormuznet_{id_drone.lower()}",
            "network_mode": "host",
            "command": [f"-id={id_drone}", "-brokers=127.0.0.1:6008", f"-x={d['x']}", f"-y={d['y']}"],
            "restart": "on-failure"
        }

    # 5. Sensores (2 para cada broker = 18)
    s_count = 1
    for i in range(9):
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

    with open("docker-compose-all.yml", "w") as f:
        yaml.dump(compose_dict, f, sort_keys=False, default_flow_style=False)

if __name__ == "__main__":
    gerar_yaml_completo()
