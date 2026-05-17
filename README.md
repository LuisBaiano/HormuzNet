# HormuzNet: Monitoramento Marítimo Descentralizado e Autônomo

<p align="center">
  <img src="images/arquitetura_hormuz.png" alt="Arquitetura do Sistema HormuzNet" width="85%" />
</p>

O **HormuzNet** é uma plataforma de monitoramento marítimo e resposta tática em tempo real para a região estratégica do Estreito de Ormuz. Desenvolvido em **Go**, o projeto implementa uma arquitetura totalmente distribuída, sem pontos únicos de falha (*Single Points of Failure - SPOF*), substituindo bases de controle centralizadas por uma malha cooperativa (*mesh*) de *brokers* P2P, sensores inteligentes e drones autônomos.

---

## 🚀 Diferenciais e Arquitetura

O sistema é dividido em três camadas lógicas que garantem tolerância a falhas, consistência eventual e alta disponibilidade:

### 1. Camada de Aquisição (UDP Multicast)
*   **Redundância Nativa:** Os sensores (Radares, Bóias e Estações Meteorológicas) não se conectam individualmente a cada servidor. Em vez disso, publicam dados ambientais e alertas em um endereço de **UDP Multicast** (`224.0.0.1:8080`).
*   **Tolerância a Perdas:** Projetado sob a premissa de que a leitura mais recente é a mais valiosa, minimizando o *overhead* de retransmissões em conexões instáveis. Todos os *brokers* escutam o grupo multicast e capturam a telemetria em tempo real.

### 2. Camada de Sincronização (Malha P2P de Brokers)
*   **Rede Peer-to-Peer:** Quatro *brokers* dividem o mapa operacional em setores geográficos (Noroeste, Nordeste, Sudoeste, Sudeste). Eles comunicam-se entre si via conexões TCP diretas.
*   **Relógio Lógico de Lamport:** A ordenação global de ocorrências é garantida através do algoritmo de Lamport. Isso assegura que todos os servidores mantenham filas de prioridade de incidentes absolutamente idênticas e tomem decisões consistentes.
*   **Exclusão Mútua Distribuída:** Evita que múltiplos drones sejam despachados para o mesmo incidente. O broker detentor da conexão ativa com o drone mais apto efetua o despacho e propaga a exclusão para toda a malha.

### 3. Camada de Atuação (Drones Autônomos com Fallback)
*   **Conexão Resiliente:** Os drones gerenciam canais TCP persistentes com seus brokers primários. 
*   **Algoritmo de Fallback Automático:** Caso um broker caia, o drone detecta a perda de conexão e inicia uma varredura para se acoplar ao broker vizinho ativo mais próximo da malha, garantindo que a frota permaneça operando.
*   **Modelo Probabilístico de Falha:** Para simular as condições hostis do Estreito, existe uma probabilidade configurada de 10% de o drone ser abatido/perdido durante a missão. O sistema detecta a falha e reintroduz a ocorrência na fila de prioridades global automaticamente.

---

## 📂 Estrutura do Repositório

```bash
HormuzNet/
├── code/                    # Código-fonte do sistema em Go
│   ├── broker/              # Servidor, Relógio de Lamport e controle P2P
│   ├── drone/               # Lógica dos Drones e fallback TCP
│   ├── sensor/              # Emissores de UDP Multicast
│   └── monitor/             # Dashboard tático Web (WebSockets)
├── docs/                    # Documentos arquiteturais e de especificação
│   ├── solucao-hormuznet/   # Documento completo da solução em LaTeX
│   └── arquitetura_descentralizada.md
├── images/                  # Assets visuais do repositório
│   └── arquitetura_hormuz.png
├── docker-compose.yml       # Orquestração do cluster de simulação
├── start.sh                 # Script automatizado para inicialização do sistema
└── README.md                # Este documento de referência
```

---

## 🛠️ Como Executar a Simulação

O cluster inteiro pode ser executado e validado localmente de forma automatizada através do Docker.

### Requisitos
*   [Docker](https://docs.docker.com/) e [Docker Compose](https://docs.docker.com/compose/) instalados.

### Passos para Inicialização

1.  Clone o repositório e navegue até a raiz do projeto:
    ```bash
    cd "HormuzNet"
    ```

2.  Suba a infraestrutura completa (4 Brokers, 6 Drones Autônomos e 20 Sensores simulados):
    ```bash
    docker-compose up --build -d
    ```

3.  Acesse o painel tático de monitoramento no seu navegador:
    ```bash
    http://localhost:8082
    ```

4.  Para parar a simulação e remover os contêineres:
    ```bash
    docker-compose down
    ```

---

## 📊 Painel de Monitoramento (WebSocket)
A interface tática atua como o consolidador visual do Estreito de Ormuz. Utilizando conexões **WebSocket**, o dashboard renderiza dinamicamente:
*   A posição e status (Em Base, Em Missão, Abatido) dos Drones Autônomos.
*   A saúde das conexões de rede entre os Brokers da malha.
*   A leitura em tempo real e distribuição dos Sensores de aquisição.

---

## 📄 Artigo e Documento Técnico
Uma documentação técnica detalhada contendo a fundamentação matemática do Relógio de Lamport, a modelagem de concorrência concorrente Go, testes de resiliência a falhas de rede e tabelas de performance está disponível no formato LaTeX dentro de:
👉 `docs/solucao-hormuznet/principal.tex`
