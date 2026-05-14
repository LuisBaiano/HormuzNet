import sys

brokers = [
    {"id": "B1", "setor": "Setor_Noroeste", "port": 6000, "viz": "broker2:6001,broker8:6007,broker9:6008"},
    {"id": "B2", "setor": "Setor_Norte",    "port": 6001, "viz": "broker1:6000,broker3:6002,broker9:6008"},
    {"id": "B3", "setor": "Setor_Nordeste", "port": 6002, "viz": "broker2:6001,broker4:6003,broker9:6008"},
    {"id": "B4", "setor": "Setor_Leste",    "port": 6003, "viz": "broker3:6002,broker5:6004,broker9:6008"},
    {"id": "B5", "setor": "Setor_Sudeste",  "port": 6004, "viz": "broker4:6003,broker6:6005,broker9:6008"},
    {"id": "B6", "setor": "Setor_Sul",      "port": 6005, "viz": "broker5:6004,broker7:6006,broker9:6008"},
    {"id": "B7", "setor": "Setor_Sudoeste", "port": 6006, "viz": "broker6:6005,broker8:6007,broker9:6008"},
    {"id": "B8", "setor": "Setor_Oeste",    "port": 6007, "viz": "broker7:6006,broker1:6000,broker9:6008"},
    {"id": "B9", "setor": "Setor_Centro",   "port": 6008, "viz": "broker1:6000,broker3:6002,broker5:6004,broker7:6006"}
]

drones = [
    {"id": "Drone_NW_1", "brokers": "broker1:6000,broker8:6007", "x": 250, "y": 250},
    {"id": "Drone_N_1",  "brokers": "broker2:6001,broker3:6002", "x": 500, "y": 250},
    {"id": "Drone_NE_1", "brokers": "broker3:6002,broker4:6003", "x": 750, "y": 250},
    {"id": "Drone_E_1",  "brokers": "broker4:6003,broker5:6004", "x": 750, "y": 500},
    {"id": "Drone_SW_1", "brokers": "broker7:6006,broker6:6005", "x": 250, "y": 750},
    {"id": "Drone_SE_1", "brokers": "broker5:6004,broker6:6005", "x": 750, "y": 750},
    {"id": "Drone_C_1",  "brokers": "broker9:6008,broker1:6000", "x": 500, "y": 500}
]

import random
types = ["radar", "sonar", "boia", "visual", "meteo"]

sensors = []
for i, b in enumerate(brokers):
    for j in range(5):
        t = random.choice(types)
        if "Noroeste" in b["setor"]: x = random.randint(100, 300); y = random.randint(100, 300); short = "nw"
        elif "Norte" in b["setor"]: x = random.randint(400, 600); y = random.randint(100, 300); short = "n"
        elif "Nordeste" in b["setor"]: x = random.randint(700, 900); y = random.randint(100, 300); short = "ne"
        elif "Leste" in b["setor"]: x = random.randint(700, 900); y = random.randint(400, 600); short = "e"
        elif "Sudeste" in b["setor"]: x = random.randint(700, 900); y = random.randint(700, 900); short = "se"
        elif "Sul" in b["setor"]: x = random.randint(400, 600); y = random.randint(700, 900); short = "s"
        elif "Sudoeste" in b["setor"]: x = random.randint(100, 300); y = random.randint(700, 900); short = "sw"
        elif "Oeste" in b["setor"]: x = random.randint(100, 300); y = random.randint(400, 600); short = "w"
        elif "Centro" in b["setor"]: x = random.randint(400, 600); y = random.randint(400, 600); short = "c"
        
        sensors.append({
            "id": f"{t}_{short}_{j+1}",
            "tipo": t,
            "setor": b["setor"],
            "x": x,
            "y": y
        })

out = "version: '3.8'\n\nservices:\n"

# Brokers
for b in brokers:
    out += f"""  {b['id'].lower().replace('b','broker')}:
    build:
      context: ./code
      dockerfile: Dockerfile.broker
    container_name: hormuznet_{b['id'].lower().replace('b','broker')}
    command: ["-id={b['id']}","-setor={b['setor']}","-udp=224.0.0.1:8080","-tcp=0.0.0.0:{b['port']}","-vizinhos={b['viz']}"]
    networks:
      - hormuznet
    restart: on-failure

"""

# Drones
for i, d in enumerate(drones):
    out += f"""  drone{i+1}:
    build:
      context: ./code
      dockerfile: Dockerfile.drone
    container_name: hormuznet_{d['id'].lower()}
    command: ["-id={d['id']}","-brokers={d['brokers']}","-x={d['x']}","-y={d['y']}"]
    networks:
      - hormuznet
    restart: on-failure
    depends_on:
      - broker1
      - broker2
      - broker3
      - broker4
      - broker5
      - broker6
      - broker7
      - broker8
      - broker9

"""

# Monitor
out += """  monitor:
    build:
      context: ./code
      dockerfile: Dockerfile.monitor
    container_name: hormuznet_monitor
    command: ["-brokers=broker1:6000,broker2:6001,broker3:6002,broker4:6003,broker5:6004,broker6:6005,broker7:6006,broker8:6007,broker9:6008", "-porta=8082"]
    ports:
      - "8082:8082"
    networks:
      - hormuznet
    restart: on-failure
    depends_on:
      - broker1
      - broker2
      - broker3
      - broker4
      - broker5
      - broker6
      - broker7
      - broker8

"""

# Sensors
for i, s in enumerate(sensors):
    out += f"""  sensor_{i+1}:
    build:
      context: ./code
      dockerfile: Dockerfile.sensor
    container_name: hormuznet_{s['id'].lower()}
    command: ["-id={s['id']}","-tipo={s['tipo']}","-setor={s['setor']}","-broker=224.0.0.1:8080","-intervalo=20000", "-x={s['x']}", "-y={s['y']}"]
    networks:
      - hormuznet
    restart: on-failure
    depends_on:
      - broker1

"""

out += """networks:
  hormuznet:
    name: hormuznet
    driver: bridge
"""

with open("docker-compose.yml", "w") as f:
    f.write(out)

