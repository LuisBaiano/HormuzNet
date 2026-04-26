package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"HormuzNet/internal/fila"
	"HormuzNet/internal/models"
)

// ── Configuração ──────────────────────────────────────────────────────────────

const (
	heartbeatInterval   = 5 * time.Second
	heartbeatTimeout    = 15 * time.Second // broker considerado morto após 3 falhas
	envelhecerInterval  = 10 * time.Second
	despachoInterval    = 500 * time.Millisecond
)

// ── Estado global do broker ───────────────────────────────────────────────────

type Broker struct {
	id      string
	setorID string
	portaUDP  string // recebe leituras de sensores
	portaTCP  string // recebe conexões de bases e brokers

	// Fila de ocorrências pendentes (thread-safe)
	fila *fila.FilaPrioridade

	// Bases conectadas: base_id → conexão TCP
	basesMu sync.RWMutex
	bases   map[string]net.Conn

	// Drones conhecidos: drone_id → InfoDrone
	dronesMu sync.RWMutex
	drones   map[string]models.InfoDrone

	// Brokers vizinhos: broker_id → conexão TCP de saída
	vizinhosMu sync.RWMutex
	vizinhos   map[string]net.Conn

	// Último heartbeat recebido de cada broker vizinho
	heartbeatMu sync.Mutex
	ultimoHB    map[string]time.Time

	// Ocorrências já atendidas (evita reprocessar cascata)
	atendidosMu sync.Mutex
	atendidos   map[string]bool

	logger *log.Logger
}

func novoBroker(id, setorID, portaUDP, portaTCP string) *Broker {
	return &Broker{
		id:        id,
		setorID:   setorID,
		portaUDP:  portaUDP,
		portaTCP:  portaTCP,
		fila:      fila.Nova(),
		bases:     make(map[string]net.Conn),
		drones:    make(map[string]models.InfoDrone),
		vizinhos:  make(map[string]net.Conn),
		ultimoHB:  make(map[string]time.Time),
		atendidos: make(map[string]bool),
		logger:    log.New(os.Stdout, fmt.Sprintf("[BROKER:%s] ", id), log.LstdFlags),
	}
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	id     := flag.String("id",      "",            "ID único do broker (ex: B1)")
	setor  := flag.String("setor",   "",            "ID do setor (ex: Setor_Norte)")
	udp    := flag.String("udp",     "0.0.0.0:9000","Porta UDP para sensores")
	tcp    := flag.String("tcp",     "0.0.0.0:9001","Porta TCP para bases e brokers")
	vizStr := flag.String("vizinhos","",            "Endereços TCP de brokers vizinhos, separados por vírgula")
	flag.Parse()

	if *id == "" || *setor == "" {
		fmt.Fprintln(os.Stderr, "Uso: broker -id B1 -setor Setor_Norte [-udp :9000] [-tcp :9001] [-vizinhos host:porta,host:porta]")
		os.Exit(1)
	}

	b := novoBroker(*id, *setor, *udp, *tcp)
	b.logger.Printf("Iniciando — setor=%s UDP=%s TCP=%s", *setor, *udp, *tcp)

	// Escuta conexões TCP de bases e outros brokers
	go b.escutarTCP()

	// Recebe leituras de sensores via UDP
	go b.escutarUDP()

	// Conecta aos brokers vizinhos configurados
	if *vizStr != "" {
		go b.conectarVizinhos(*vizStr)
	}

	// Goroutines de manutenção
	go b.loopHeartbeat()
	go b.loopDetectarFalhas()
	go b.loopEnvelhecerFila()
	go b.loopDespachar()

	// Bloqueia para sempre
	select {}
}

// ── UDP — leituras de sensores ────────────────────────────────────────────────

func (b *Broker) escutarUDP() {
	addr, err := net.ResolveUDPAddr("udp", b.portaUDP)
	if err != nil {
		b.logger.Fatalf("UDP addr: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		b.logger.Fatalf("UDP listen: %v", err)
	}
	defer conn.Close()
	b.logger.Printf("Escutando sensores UDP em %s", b.portaUDP)

	buf := make([]byte, 4096)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			b.logger.Printf("Erro UDP: %v", err)
			continue
		}
		go b.processarLeitura(buf[:n])
	}
}

func (b *Broker) processarLeitura(dados []byte) {
	var leitura models.LeituraSensor
	if err := json.Unmarshal(dados, &leitura); err != nil {
		b.logger.Printf("Leitura inválida: %v", err)
		return
	}

	// Só gera ocorrência para leituras com criticidade relevante
	if leitura.Criticidade < models.CriticidadeAlta {
		return
	}

	oc := models.Ocorrencia{
		ID:           fmt.Sprintf("%s-%s-%d", b.id, leitura.SensorID, time.Now().UnixNano()),
		SetorOrigem:  b.setorID,
		BrokerOrigem: b.id,
		Tipo:         leitura.Tipo,
		Descricao:    fmt.Sprintf("Sensor %s: %.2f %s", leitura.SensorID, leitura.Valor, leitura.Unidade),
		Criticidade:  leitura.Criticidade,
		Timestamp:    time.Now(),
	}

	b.fila.Enfileirar(oc)
	b.logger.Printf("Ocorrência enfileirada: %s [%s] — %s", oc.ID, oc.Criticidade, oc.Descricao)
}

// ── TCP — escuta bases e brokers ──────────────────────────────────────────────

func (b *Broker) escutarTCP() {
	ln, err := net.Listen("tcp", b.portaTCP)
	if err != nil {
		b.logger.Fatalf("TCP listen: %v", err)
	}
	defer ln.Close()
	b.logger.Printf("Escutando bases/brokers TCP em %s", b.portaTCP)

	for {
		conn, err := ln.Accept()
		if err != nil {
			b.logger.Printf("TCP accept: %v", err)
			continue
		}
		go b.identificarConexao(conn)
	}
}

// identificarConexao lê a primeira mensagem para saber se é base ou broker.
func (b *Broker) identificarConexao(conn net.Conn) {
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{})

	linha := scanner.Bytes()

	// Tenta como MensagemBase
	var mb models.MensagemBase
	if err := json.Unmarshal(linha, &mb); err == nil && mb.Tipo == models.BaseRegistro {
		b.registrarBase(mb.BaseID, mb.SetorID, mb.Drones, conn)
		go b.loopLeituraBase(mb.BaseID, conn, scanner)
		return
	}

	// Tenta como MensagemBroker
	var msg models.MensagemBroker
	if err := json.Unmarshal(linha, &msg); err == nil && msg.Tipo == models.MsgRegistro {
		b.registrarVizinho(msg.BrokerID, conn)
		// Envia réplica da fila pendente ao novo vizinho
		b.enviarReplicaFila(conn)
		go b.loopLeituraBroker(msg.BrokerID, conn, scanner)
		return
	}

	b.logger.Printf("Conexão desconhecida de %s — fechando", conn.RemoteAddr())
	conn.Close()
}

// ── Bases ─────────────────────────────────────────────────────────────────────

func (b *Broker) registrarBase(baseID, setorID string, drones []models.InfoDrone, conn net.Conn) {
	b.basesMu.Lock()
	b.bases[baseID] = conn
	b.basesMu.Unlock()

	b.dronesMu.Lock()
	for _, d := range drones {
		b.drones[d.DroneID] = d
	}
	b.dronesMu.Unlock()

	b.logger.Printf("Base registrada: %s (setor=%s, drones=%d)", baseID, setorID, len(drones))
}

func (b *Broker) loopLeituraBase(baseID string, conn net.Conn, scanner *bufio.Scanner) {
	defer func() {
		conn.Close()
		b.basesMu.Lock()
		delete(b.bases, baseID)
		b.basesMu.Unlock()
		b.logger.Printf("Base desconectada: %s", baseID)
	}()

	for scanner.Scan() {
		var msg models.MensagemBase
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		b.processarMensagemBase(msg)
	}
}

func (b *Broker) processarMensagemBase(msg models.MensagemBase) {
	switch msg.Tipo {
	case models.BaseDroneEstado:
		b.dronesMu.Lock()
		if d, ok := b.drones[msg.DroneID]; ok {
			d.Estado = msg.NovoEstado
			d.UltimaVez = time.Now()
			if msg.NovoEstado == models.DroneDisponivel {
				d.OcorrenciaID = ""
			}
			b.drones[msg.DroneID] = d
		}
		b.dronesMu.Unlock()

		b.logger.Printf("Drone %s → %s", msg.DroneID, msg.NovoEstado)

		// Se drone foi liberado, notifica todos os vizinhos
		if msg.NovoEstado == models.DroneDisponivel || msg.NovoEstado == models.DroneAbatido || msg.NovoEstado == models.DroneSemBateria {
			tipoMsg := models.MsgDroneLiberado
			if msg.NovoEstado == models.DroneAbatido || msg.NovoEstado == models.DroneSemBateria {
				tipoMsg = models.MsgDronePerdido
			}
			b.broadcastVizinhos(models.MensagemBroker{
				Tipo:      tipoMsg,
				BrokerID:  b.id,
				DroneID:   msg.DroneID,
				BaseID:    msg.BaseID,
				Motivo:    string(msg.NovoEstado),
				Timestamp: time.Now(),
			})
		}

	case models.BaseStatusDrones:
		b.dronesMu.Lock()
		for _, d := range msg.Drones {
			b.drones[d.DroneID] = d
		}
		b.dronesMu.Unlock()
	}
}

func (b *Broker) enviarComandoBase(baseID string, cmd models.ComandoBase) bool {
	b.basesMu.RLock()
	conn, ok := b.bases[baseID]
	b.basesMu.RUnlock()
	if !ok {
		return false
	}
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := json.NewEncoder(conn).Encode(cmd); err != nil {
		b.logger.Printf("Erro ao enviar comando para base %s: %v", baseID, err)
		return false
	}
	return true
}

// ── Drones — despacho local ───────────────────────────────────────────────────

// droneDisponivel retorna o drone disponível com maior bateria desta base.
// Retorna string vazia se não houver nenhum.
func (b *Broker) droneDisponivel() (models.InfoDrone, bool) {
	b.dronesMu.RLock()
	defer b.dronesMu.RUnlock()

	var melhor models.InfoDrone
	encontrou := false
	for _, d := range b.drones {
		if !d.Disponivel() {
			continue
		}
		if !encontrou || d.Bateria > melhor.Bateria {
			melhor = d
			encontrou = true
		}
	}
	return melhor, encontrou
}

func (b *Broker) marcarOcupado(droneID, ocorrenciaID string) {
	b.dronesMu.Lock()
	defer b.dronesMu.Unlock()
	if d, ok := b.drones[droneID]; ok {
		d.Estado = models.DroneDespachado
		d.OcorrenciaID = ocorrenciaID
		d.UltimaVez = time.Now()
		b.drones[droneID] = d
	}
}

// ── Loop de despacho ──────────────────────────────────────────────────────────

func (b *Broker) loopDespachar() {
	ticker := time.NewTicker(despachoInterval)
	defer ticker.Stop()
	for range ticker.C {
		b.tentarDespachar()
	}
}

func (b *Broker) tentarDespachar() {
	oc, ok := b.fila.Peek()
	if !ok {
		return
	}

	// Verifica se já foi atendida por outro broker
	b.atendidosMu.Lock()
	jaAtendida := b.atendidos[oc.ID]
	b.atendidosMu.Unlock()
	if jaAtendida {
		b.fila.Remover(oc.ID)
		return
	}

	// Tenta despachar drone local
	drone, temDrone := b.droneDisponivel()
	if temDrone {
		b.fila.Desenfileirar()
		b.marcarOcupado(drone.DroneID, oc.ID)

		b.enviarComandoBase(drone.BaseID, models.ComandoBase{
			Tipo:         models.CmdDespacharDrone,
			DroneID:      drone.DroneID,
			OcorrenciaID: oc.ID,
			SetorDestino: oc.SetorOrigem,
			Timestamp:    time.Now(),
		})

		b.logger.Printf("Despachado drone %s → ocorrência %s [%s]", drone.DroneID, oc.ID, oc.Criticidade)

		// Notifica todos os vizinhos que a ocorrência foi atendida
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:         models.MsgDroneDespachado,
			BrokerID:     b.id,
			DroneID:      drone.DroneID,
			BaseID:       drone.BaseID,
			OcorrenciaID: oc.ID,
			Timestamp:    time.Now(),
		})
		return
	}

	// Sem drone local — propaga em cascata para todos os vizinhos
	b.logger.Printf("Sem drone local para %s — propagando em cascata", oc.ID)
	b.broadcastVizinhos(models.MensagemBroker{
		Tipo:       models.MsgRequisicaoDrone,
		BrokerID:   b.id,
		Ocorrencia: &oc,
		Timestamp:  time.Now(),
	})
}

// ── Brokers vizinhos ──────────────────────────────────────────────────────────

func (b *Broker) conectarVizinhos(enderecos string) {
	// Divide por vírgula e conecta a cada um
	lista := splitCSV(enderecos)
	for _, addr := range lista {
		go b.conectarVizinho(addr)
	}
}

func (b *Broker) conectarVizinho(addr string) {
	backoff := 2 * time.Second
	for {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			b.logger.Printf("Falha ao conectar vizinho %s: %v — retry em %s", addr, err, backoff)
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}

		// Envia registro
		reg := models.MensagemBroker{
			Tipo:      models.MsgRegistro,
			BrokerID:  b.id,
			Timestamp: time.Now(),
		}
		if err := json.NewEncoder(conn).Encode(reg); err != nil {
			conn.Close()
			continue
		}

		b.logger.Printf("Conectado ao vizinho %s", addr)
		// O broker remoto nos dará seu ID na primeira resposta
		// Por ora usa o endereço como chave temporária
		b.vizinhosMu.Lock()
		b.vizinhos[addr] = conn
		b.vizinhosMu.Unlock()

		// Lê respostas
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			var msg models.MensagemBroker
			if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
				continue
			}
			// Substitui chave temporária pelo ID real na primeira mensagem
			if msg.BrokerID != "" && msg.BrokerID != addr {
				b.vizinhosMu.Lock()
				delete(b.vizinhos, addr)
				b.vizinhos[msg.BrokerID] = conn
				b.vizinhosMu.Unlock()
			}
			b.processarMensagemBroker(msg)
		}

		b.vizinhosMu.Lock()
		delete(b.vizinhos, addr)
		b.vizinhosMu.Unlock()
		conn.Close()
		b.logger.Printf("Vizinho %s desconectou — reconectando em %s", addr, backoff)
		time.Sleep(backoff)
		backoff = 2 * time.Second // reseta após reconexão bem-sucedida anterior
	}
}

func (b *Broker) registrarVizinho(brokerID string, conn net.Conn) {
	b.vizinhosMu.Lock()
	b.vizinhos[brokerID] = conn
	b.vizinhosMu.Unlock()

	b.heartbeatMu.Lock()
	b.ultimoHB[brokerID] = time.Now()
	b.heartbeatMu.Unlock()

	b.logger.Printf("Broker vizinho registrado: %s (%s)", brokerID, conn.RemoteAddr())
}

func (b *Broker) loopLeituraBroker(brokerID string, conn net.Conn, scanner *bufio.Scanner) {
	defer func() {
		conn.Close()
		b.vizinhosMu.Lock()
		delete(b.vizinhos, brokerID)
		b.vizinhosMu.Unlock()
		b.logger.Printf("Broker vizinho desconectado: %s", brokerID)
	}()

	for scanner.Scan() {
		var msg models.MensagemBroker
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		b.processarMensagemBroker(msg)
	}
}

// processarMensagemBroker trata todas as mensagens recebidas de vizinhos.
func (b *Broker) processarMensagemBroker(msg models.MensagemBroker) {
	// Atualiza heartbeat do remetente
	b.heartbeatMu.Lock()
	b.ultimoHB[msg.BrokerID] = time.Now()
	b.heartbeatMu.Unlock()

	switch msg.Tipo {

	case models.MsgHeartbeat:
		// Heartbeat já foi registrado acima — nada mais a fazer

	case models.MsgRegistro:
		// Pode chegar novamente após reconexão — ignora

	case models.MsgRequisicaoDrone:
		if msg.Ocorrencia == nil {
			return
		}
		oc := *msg.Ocorrencia

		// Já atendida por este broker?
		b.atendidosMu.Lock()
		jaAtendida := b.atendidos[oc.ID]
		b.atendidosMu.Unlock()
		if jaAtendida {
			return
		}

		// Tenta despachar drone local para a ocorrência do vizinho
		drone, temDrone := b.droneDisponivel()
		if !temDrone {
			return // Não temos drone — ignora (outro broker pode ter)
		}

		b.marcarOcupado(drone.DroneID, oc.ID)
		b.atendidosMu.Lock()
		b.atendidos[oc.ID] = true
		b.atendidosMu.Unlock()

		b.enviarComandoBase(drone.BaseID, models.ComandoBase{
			Tipo:         models.CmdDespacharDrone,
			DroneID:      drone.DroneID,
			OcorrenciaID: oc.ID,
			SetorDestino: oc.SetorOrigem,
			Timestamp:    time.Now(),
		})

		b.logger.Printf("Atendendo cascata: drone %s → ocorrência %s de %s [%s]",
			drone.DroneID, oc.ID, msg.BrokerID, oc.Criticidade)

		// Notifica todos (inclusive o solicitante) que foi despachado
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:         models.MsgDroneDespachado,
			BrokerID:     b.id,
			DroneID:      drone.DroneID,
			BaseID:       drone.BaseID,
			OcorrenciaID: oc.ID,
			Timestamp:    time.Now(),
		})

	case models.MsgDroneDespachado:
		// Marca ocorrência como atendida localmente para não redespachar
		b.atendidosMu.Lock()
		b.atendidos[msg.OcorrenciaID] = true
		b.atendidosMu.Unlock()
		b.fila.Remover(msg.OcorrenciaID)
		b.logger.Printf("Ocorrência %s atendida por %s (drone %s)", msg.OcorrenciaID, msg.BrokerID, msg.DroneID)

	case models.MsgDroneLiberado:
		b.logger.Printf("Drone %s liberado (base %s)", msg.DroneID, msg.BaseID)

	case models.MsgDronePerdido:
		b.logger.Printf("Drone %s perdido: %s (base %s)", msg.DroneID, msg.Motivo, msg.BaseID)

	case models.MsgReplicaFila:
		// Recebe fila pendente de outro broker ao se conectar
		for _, oc := range msg.FilaPendente {
			b.fila.Enfileirar(oc)
		}
		b.logger.Printf("Fila replicada de %s: %d ocorrências", msg.BrokerID, len(msg.FilaPendente))
	}
}

func (b *Broker) broadcastVizinhos(msg models.MensagemBroker) {
	b.vizinhosMu.RLock()
	defer b.vizinhosMu.RUnlock()

	for id, conn := range b.vizinhos {
		conn := conn
		id := id
		go func() {
			conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if err := json.NewEncoder(conn).Encode(msg); err != nil {
				b.logger.Printf("Erro broadcast para %s: %v", id, err)
			}
		}()
	}
}

func (b *Broker) enviarReplicaFila(conn net.Conn) {
	snap := b.fila.Snapshot()
	if len(snap) == 0 {
		return
	}
	msg := models.MensagemBroker{
		Tipo:         models.MsgReplicaFila,
		BrokerID:     b.id,
		FilaPendente: snap,
		Timestamp:    time.Now(),
	}
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	json.NewEncoder(conn).Encode(msg) //nolint
}

// ── Heartbeat e detecção de falhas ────────────────────────────────────────────

func (b *Broker) loopHeartbeat() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:      models.MsgHeartbeat,
			BrokerID:  b.id,
			Timestamp: time.Now(),
		})
	}
}

func (b *Broker) loopDetectarFalhas() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		agora := time.Now()
		b.heartbeatMu.Lock()
		for id, ultimo := range b.ultimoHB {
			if agora.Sub(ultimo) > heartbeatTimeout {
				b.logger.Printf("Broker %s presumido morto (último HB: %s atrás)", id, agora.Sub(ultimo).Round(time.Second))
				delete(b.ultimoHB, id)
			}
		}
		b.heartbeatMu.Unlock()
	}
}

func (b *Broker) loopEnvelhecerFila() {
	ticker := time.NewTicker(envelhecerInterval)
	defer ticker.Stop()
	for range ticker.C {
		b.fila.Envelhecer()
	}
}

// ── Utilitários ───────────────────────────────────────────────────────────────

func splitCSV(s string) []string {
	var resultado []string
	atual := ""
	for _, c := range s {
		if c == ',' {
			if atual != "" {
				resultado = append(resultado, atual)
				atual = ""
			}
		} else {
			atual += string(c)
		}
	}
	if atual != "" {
		resultado = append(resultado, atual)
	}
	return resultado
}
