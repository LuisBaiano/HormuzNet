# HormuzNet: Monitoramento Marítimo Descentralizado

Este repositório contém o código-fonte do sistema HormuzNet, um projeto voltado para o monitoramento e resposta rápida na região do Estreito de Ormuz. O desenvolvimento deste projeto focou na implementação de uma arquitetura de sistemas distribuídos, substituindo estruturas centralizadas por uma malha autônoma e resiliente a falhas.

## Estrutura do Projeto

A estrutura da aplicação se organiza da seguinte forma:

*   **`code/`**: Contém o código-fonte do sistema em Go, incluindo a lógica dos Brokers, Drones, Sensores e da interface de Monitoramento.
*   **`docs/`**: Contém os documentos detalhados sobre as decisões arquiteturais e modelagem do sistema, incluindo o diagrama de comunicação.

Para um aprofundamento técnico, consulte o arquivo [arquitetura_descentralizada.md](docs/arquitetura_descentralizada.md).

## Instalação e Execução

A camada de operação e execução do sistema foi projetada para rodar nativamente em contêineres Docker, simulando o ambiente distribuído na mesma máquina de forma transparente.

Para compilar e iniciar o cluster de monitoramento com a simulação padrão (4 Brokers, 6 Drones Autônomos e 20 Sensores), execute o seguinte comando na raiz do projeto:

```bash
docker-compose up --build -d
```

Após o início dos serviços, a interface gráfica tática (Dashboard) estará disponível via navegador no endereço:

`http://localhost:8082`

Para interromper a execução e desligar os nós da malha:

```bash
docker-compose down
```
