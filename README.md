# HormuzNet: Monitoramento Marítimo Descentralizado e Autônomo

```mermaid
graph TD
    classDef broker fill:#2a9d8f,stroke:#264653,stroke-width:2px,color:#fff;
    classDef sensor fill:#e76f51,stroke:#264653,stroke-width:2px,color:#fff;
    classDef drone fill:#e9c46a,stroke:#264653,stroke-width:2px,color:#000;
    classDef monitor fill:#457b9d,stroke:#1d3557,stroke-width:2px,color:#fff;
    classDef multicast fill:#a8dadc,stroke:#457b9d,stroke-width:2px,color:#000;

    subgraph Camada_Sensores [Camada de Aquisição]
        S1[Sensor Radar]:::sensor
        S2[Sensor Sonar]:::sensor
        S3[Sensor Boia]:::sensor
    end

    MGroup((Grupo Multicast UDP<br>224.1.2.3:9876)):::multicast

    subgraph Camada_Core [Camada de Sincronização P2P]
        B9[Líder de Descoberta<br>B9 - Setor Centro]:::broker
        B1[Broker B1<br>Setor Noroeste]:::broker
        B2[Broker B2<br>Setor Norte]:::broker
        B_others[Brokers B3..B8<br>Outros Setores]:::broker
    end

    subgraph Camada_Atuacao [Camada de Atuação]
        D1[Drone 1]:::drone
        D2[Drone 2]:::drone
        D3[Drone N]:::drone
    end

    subgraph Camada_Monit [Visualização]
        MON[Monitor & Dashboard<br>Porta 8085]:::monitor
        UI[Painel Web / WebSockets]:::monitor
    end

    S1 -.->|Broadcast UDP| MGroup
    S2 -.->|Broadcast UDP| MGroup
    S3 -.->|Broadcast UDP| MGroup

    MGroup -.->|Escuta Multicast| B1
    MGroup -.->|Escuta Multicast| B2
    MGroup -.->|Escuta Multicast| B9
    MGroup -.->|Escuta Multicast| B_others

    B1 <-->|Malha TCP / Gossip / Heartbeats| B2
    B2 <-->|Malha TCP / Gossip / Heartbeats| B_others
    B_others <-->|Malha TCP / Gossip / Heartbeats| B9
    B1 <-->|Malha TCP / Gossip / Heartbeats| B9

    B1 -.->|Descoberta / Discovery| B9
    B2 -.->|Descoberta / Discovery| B9
    B_others -.->|Descoberta / Discovery| B9

    D1 ===|Conexão TCP Voo/Estado| B1
    D2 ===|Conexão TCP Voo/Estado| B2
    D3 ===|Conexão TCP Voo/Estado| B_others

    B9 --->|Métricas TCP| MON
    MON <-->|WebSockets| UI
```

O **HormuzNet** é uma plataforma de monitoramento marítimo e resposta tática em tempo real para a região estratégica do Estreito de Ormuz. Desenvolvido em **Go**, o projeto implementa uma arquitetura **totalmente distribuída e tolerante a falhas**, substituindo bases de controle centralizadas por uma malha cooperativa (*mesh*) de *brokers* P2P, sensores inteligentes e drones autônomos.

---

## 🚀 Diferenciais e Arquitetura

O sistema é dividido em três camadas lógicas que garantem consistência e alta disponibilidade:

### 1. Camada de Aquisição (UDP Multicast)
*   **Redundância Nativa:** Os sensores (Radares, Bóias, Sonar) não se conectam individualmente a servidores específicos. Eles publicam leituras em um endereço **Multicast UDP** (`224.1.2.3:9876`).
*   **Tolerância a Perdas:** Projetado para ambientes ruidosos. A perda de um pacote UDP não impacta o sistema, visto que sensores enviam leituras frequentes.

### 2. Camada de Sincronização (Descoberta P2P e Ring Failover)
*   **Descoberta Dinâmica:** Não há IPs hardcoded. O sistema utiliza um nó Líder (Centro/B9) para Descoberta. Quando os brokers ligam, eles perguntam ao líder quem está online e montam uma malha TCP dinâmica automaticamente.
*   **Filtro Regional e Ring Failover:** Cada Broker processa eventos de seu próprio setor geográfico. **Se um Broker morrer**, o algoritmo de **Herança em Anel (Ring Failover)** entra em ação. O próximo Broker sobrevivente na sequência assume os sensores da região morta, garantindo que não existam *pontos cegos* no mapa. 
*   **Relógio de Lamport:** A ordenação global de ocorrências é garantida através do algoritmo de Lamport, assegurando que todos mantenham filas de prioridade idênticas.

### 3. Camada de Atuação (Drones Autônomos)
*   **Exclusão Mútua Simplificada:** O Broker que possui conexão com o drone mais próximo efetua o despacho da ocorrência.
*   **Modelo de Falha Hostil:** Para simular as condições do Estreito, drones podem ser abatidos/perdidos (probabilidade configurada). O sistema recicla a ocorrência para a fila de prioridades e despacha um substituto.

---

## 🛠️ Como Rodar a Simulação (Menu Interativo)

Para facilitar a simulação de redes complexas em múltiplas máquinas, desenvolvemos um painel interativo. 

### Requisitos
*   [Docker](https://docs.docker.com/) e [Docker Compose](https://docs.docker.com/compose/).
*   (Opcional) Python 3 para o gerador de compose em background.

### Passos para Inicialização

1.  Abra o terminal e execute o menu:
    ```bash
    ./menu.sh
    ```
2.  **Passo a passo no Menu:**
    *   **Opção 1:** Inicia o Nó Líder (Broker 9) e exibe o IP físico do seu PC na rede.
    *   **Opção 2:** Cria os outros Brokers (seguidores). O script perguntará o IP do Líder.
    *   **Opções 3, 4 e 5:** Inicia o Monitor (Dashboard na porta 8085), Drones e Sensores, tudo dinamicamente apontado para a malha principal.

> **Modo Destruição (`eliminar.sh`):** Para testar resiliência e failover em tempo real, rode o `./eliminar.sh`. Ele listará todos os contêineres ativos do HormuzNet (brokers, drones, sensores) com números identificadores. Digite o número (ou múltiplos números separados por espaço) dos contêineres que deseja derrubar e observe a adaptação e o replanejamento da rede de forma imediata!
>
> **Modo Janelas de Terminal (`terminais.sh`):** Para acompanhar os logs individuais de cada broker, drone ou sensor em tempo real, execute `./terminais.sh` para abrir terminais dedicados para cada contêiner ativo.

---

## 📊 Painel de Monitoramento (WebSocket)
A interface tática atua como o consolidador visual do Estreito. Utilizando conexões **WebSocket**, o dashboard renderiza dinamicamente:
*   Posição, saúde e despacho de Drones.
*   Comunicação ativa P2P.
*   Alertas de Sensores e estado dos Brokers.

*(Acesse via: `http://localhost:8085` após subir a Opção 3 no menu)*

---

## 📄 Artigo e Documento Técnico
Uma documentação técnica detalhada contendo a fundamentação matemática, relógio de Lamport e testes de resiliência está disponível no formato LaTeX dentro de:
👉 `docs/solucao-hormuznet/principal.tex`
