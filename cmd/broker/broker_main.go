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

// ── Constantes ────────────────────────────────────────────────────────────────

const (
	heartbeatInterval  = 5 * time.Second
	heartbeatTimeout   = 15 * time.Second
	envelhecerInterval = 10 * time.Second
	despachoInterval   = 500 * time.Millisecond
	ociosidadeTimeout  = 60 * time.Second // drone ocioso se disponível > 60s sem despacho
)

// ── Estrutura do broker ───────────────────────────────────────────────────────

type Broker struct {
	id      string
	setorID string
	portaUDP string
	portaTCP string

	fila *fila.FilaPrioridade

	// Bases conectadas localmente: base_id → conn
	basesMu sync.RWMutex
	bases   map[string]net.Conn
	// Posição de cada base conhecida (local e remotas)
	basePosMu sync.RWMutex
	basePosicoes map[string]models.Coordenada

	// Lista GLOBAL de drones (todos os brokers sincronizam)
	// drone_id → InfoDrone
	dronesMu sync.RWMutex
	drones   map[string]models.InfoDrone

	// Brokers vizinhos: broker_id → conn
	vizinhosMu sync.RWMutex
	vizinhos   map[string]net.Conn

	heartbeatMu sync.Mutex
	ultimoHB    map[string]time.Time

	atendidosMu sync.Mutex
	atendidos   map[string]bool

	logger *log.Logger
}

func novoBroker(id, setorID, portaUDP, portaTCP string) *Broker {
	return &Broker{
		id:           id,
		setorID:      setorID,
		portaUDP:     portaUDP,
		portaTCP:     portaTCP,
		fila:         fila.Nova(),
		bases:        make(map[string]net.Conn),
		basePosicoes: make(map[string]models.Coordenada),
		drones:       make(map[string]models.InfoDrone),
		vizinhos:     make(map[string]net.Conn),
		ultimoHB:     make(map[string]time.Time),
		atendidos:    make(map[string]bool),
		logger:       log.New(os.Stdout, fmt.Sprintf("[BROKER:%s] ", id), log.LstdFlags),
	}
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	id      := flag.String("id",       "",             "ID único do broker (ex: B1)")
	setor   := flag.String("setor",    "",             "ID do setor (ex: Setor_Norte)")
	udp     := flag.String("udp",      "0.0.0.0:8080", "Porta UDP para sensores")
	tcp     := flag.String("tcp",      "0.0.0.0:6000", "Porta TCP para bases e brokers")
	vizStr  := flag.String("vizinhos", "",             "Endereços TCP de brokers vizinhos (vírgula)")
	flag.Parse()

	if *id == "" || *setor == "" {
		fmt.Fprintln(os.Stderr, "Uso: broker -id B1 -setor Setor_Norte [-udp :8080] [-tcp :6000] [-vizinhos IP:6000,IP:6000]")
		os.Exit(1)
	}

	b := novoBroker(*id, *setor, *udp, *tcp)
	b.logger.Printf("Iniciando — setor=%s UDP=%s TCP=%s", *setor, *udp, *tcp)

	go b.escutarTCP()
	go b.escutarUDP()
	if *vizStr != "" {
		go b.conectarVizinhos(*vizStr)
	}
	go b.loopHeartbeat()
	go b.loopDetectarFalhas()
	go b.loopEnvelhecerFila()
	go b.loopDespachar()
	go b.loopVerificarOciosidade()

	select {}
}

// ── UDP — sensores ────────────────────────────────────────────────────────────

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
			continue
		}
		go b.processarLeitura(buf[:n])
	}
}

func (b *Broker) processarLeitura(dados []byte) {
	var leitura models.LeituraSensor
	if err := json.Unmarshal(dados, &leitura); err != nil {
		return
	}
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
		// Posição da ocorrência = posição do sensor (futuramente virá no pacote)
		Posicao: models.Coordenada{X: 0, Y: 0},
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
			continue
		}
		go b.identificarConexao(conn)
	}
}

func (b *Broker) identificarConexao(conn net.Conn) {
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{})
	linha := scanner.Bytes()

	var mb models.MensagemBase
	if err := json.Unmarshal(linha, &mb); err == nil && mb.Tipo == models.BaseRegistro {
		b.registrarBase(mb.BaseID, mb.SetorID, mb.Posicao, mb.Drones, conn)
		go b.loopLeituraBase(mb.BaseID, conn, scanner)
		return
	}

	var msg models.MensagemBroker
	if err := json.Unmarshal(linha, &msg); err == nil && msg.Tipo == models.MsgRegistro {
		b.registrarVizinho(msg.BrokerID, conn)
		b.enviarReplicaFila(conn)
		b.enviarSincGlobal(conn) // envia estado global de drones ao novo vizinho
		go b.loopLeituraBroker(msg.BrokerID, conn, scanner)
		return
	}

	b.logger.Printf("Conexão desconhecida de %s — fechando", conn.RemoteAddr())
	conn.Close()
}

// ── Bases ─────────────────────────────────────────────────────────────────────

func (b *Broker) registrarBase(baseID, setorID string, pos models.Coordenada, drones []models.InfoDrone, conn net.Conn) {
	b.basesMu.Lock()
	b.bases[baseID] = conn
	b.basesMu.Unlock()

	b.basePosMu.Lock()
	b.basePosicoes[baseID] = pos
	b.basePosMu.Unlock()

	agora := time.Now()
	b.dronesMu.Lock()
	for _, d := range drones {
		d.BrokerID = b.id
		d.BaseID = baseID
		if d.Estado == models.DroneDisponivel {
			d.DisponiveisDesde = agora
		}
		b.drones[d.DroneID] = d
	}
	b.dronesMu.Unlock()

	b.logger.Printf("Base registrada: %s (setor=%s pos=(%.0f,%.0f) drones=%d)",
		baseID, setorID, pos.X, pos.Y, len(drones))

	// Sincroniza drones desta base com toda a malha
	b.sincronizarDrones(drones)
}

func (b *Broker) loopLeituraBase(baseID string, conn net.Conn, scanner *bufio.Scanner) {
	defer func() {
		conn.Close()
		b.basesMu.Lock()
		delete(b.bases, baseID)
		b.basesMu.Unlock()
		b.logger.Printf("Base %s desconectada — realocando seus drones", baseID)
		b.realocarDronesBase(baseID)
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
		agora := time.Now()
		b.dronesMu.Lock()
		d, ok := b.drones[msg.DroneID]
		if ok {
			d.Estado = msg.NovoEstado
			d.UltimaVez = agora
			if msg.NovoEstado == models.DroneDisponivel {
				d.OcorrenciaID = ""
				d.DisponiveisDesde = agora
			}
			if msg.NovoEstado == models.DroneDespachado || msg.NovoEstado == models.DroneEmMissao {
				d.DisponiveisDesde = time.Time{} // reseta ociosidade
			}
			b.drones[msg.DroneID] = d
		}
		b.dronesMu.Unlock()

		if !ok {
			return
		}

		b.logger.Printf("Drone %s → %s", msg.DroneID, msg.NovoEstado)

		// Notifica toda a malha da mudança de estado
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:      models.MsgSincDrone,
			BrokerID:  b.id,
			Drone:     &d,
			Timestamp: agora,
		})

		// Notifica missão concluída
		if msg.NovoEstado == models.DroneDisponivel && msg.OcorrenciaID != "" {
			b.broadcastVizinhos(models.MensagemBroker{
				Tipo:         models.MsgMissaoConcluida,
				BrokerID:     b.id,
				DroneID:      msg.DroneID,
				OcorrenciaID: msg.OcorrenciaID,
				Timestamp:    agora,
			})
		}

		if msg.NovoEstado == models.DroneAbatido || msg.NovoEstado == models.DroneSemBateria {
			b.broadcastVizinhos(models.MensagemBroker{
				Tipo:      models.MsgDronePerdido,
				BrokerID:  b.id,
				DroneID:   msg.DroneID,
				BaseID:    msg.BaseID,
				Motivo:    string(msg.NovoEstado),
				Timestamp: agora,
			})
		}

	case models.BaseStatusDrones:
		agora := time.Now()
		b.dronesMu.Lock()
		for _, d := range msg.Drones {
			d.BrokerID = b.id
			if ex, ok := b.drones[d.DroneID]; ok {
				d.DisponiveisDesde = ex.DisponiveisDesde
			}
			d.UltimaVez = agora
			b.drones[d.DroneID] = d
		}
		b.dronesMu.Unlock()
	}
}

// ── Realocação de drones após base destruída ──────────────────────────────────

func (b *Broker) realocarDronesBase(baseID string) {
	b.dronesMu.Lock()
	var orfaos []models.InfoDrone
	for id, d := range b.drones {
		if d.BaseID == baseID && d.Estado != models.DroneAbatido && d.Estado != models.DroneSemBateria {
			d.Estado = models.DroneRealocando
			b.drones[id] = d
			orfaos = append(orfaos, d)
		}
	}
	b.dronesMu.Unlock()

	if len(orfaos) == 0 {
		return
	}

	b.logger.Printf("Realocando %d drones órfãos da base %s", len(orfaos), baseID)

	// Tenta realocar em base local primeiro
	b.basesMu.RLock()
	var baseLocal string
	var connLocal net.Conn
	for bid, c := range b.bases {
		if bid != baseID {
			baseLocal = bid
			connLocal = c
			break
		}
	}
	b.basesMu.RUnlock()

	if baseLocal != "" {
		b.enviarDronesParaBase(connLocal, baseLocal, orfaos)
		return
	}

	// Sem base local disponível — pede para a malha absorver
	b.broadcastVizinhos(models.MensagemBroker{
		Tipo:         models.MsgRealocacaoDrones,
		BrokerID:     b.id,
		DronesOrfaos: orfaos,
		Timestamp:    time.Now(),
	})
}

func (b *Broker) enviarDronesParaBase(conn net.Conn, baseID string, drones []models.InfoDrone) {
	cmd := models.ComandoBase{
		Tipo:               models.CmdReceberDrones,
		DronesParaAbsorver: drones,
		Timestamp:          time.Now(),
	}
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := json.NewEncoder(conn).Encode(cmd); err != nil {
		b.logger.Printf("Erro ao enviar drones para base %s: %v", baseID, err)
		return
	}
	b.logger.Printf("%d drones realocados para base %s", len(drones), baseID)

	// Atualiza base dos drones no estado global
	b.dronesMu.Lock()
	for _, d := range drones {
		if ex, ok := b.drones[d.DroneID]; ok {
			ex.BaseID = baseID
			ex.Estado = models.DroneDisponivel
			ex.DisponiveisDesde = time.Now()
			b.drones[d.DroneID] = ex
		}
	}
	b.dronesMu.Unlock()
}

// ── Despacho por proximidade ──────────────────────────────────────────────────

// droneMaisProximo retorna o drone disponível mais próximo da ocorrência.
// Prioriza bateria como critério de desempate quando distâncias são iguais.
func (b *Broker) droneMaisProximo(oc models.Ocorrencia) (models.InfoDrone, bool) {
	b.dronesMu.RLock()
	defer b.dronesMu.RUnlock()

	var melhor models.InfoDrone
	encontrou := false
	var menorDist float64

	for _, d := range b.drones {
		if !d.Disponivel() {
			continue
		}
		dist := oc.Posicao.Distancia(d.Posicao)
		if !encontrou || dist < menorDist || (dist == menorDist && d.Bateria > melhor.Bateria) {
			melhor = d
			menorDist = dist
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
		d.DisponiveisDesde = time.Time{}
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

	b.atendidosMu.Lock()
	jaAtendida := b.atendidos[oc.ID]
	b.atendidosMu.Unlock()
	if jaAtendida {
		b.fila.Remover(oc.ID)
		return
	}

	drone, temDrone := b.droneMaisProximo(oc)
	if !temDrone {
		// Sem drone disponível em nenhuma base conhecida — propaga cascata
		b.logger.Printf("Sem drone disponível para %s [%s] — propagando cascata", oc.ID, oc.Criticidade)
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:       models.MsgRequisicaoDrone,
			BrokerID:   b.id,
			Ocorrencia: &oc,
			Timestamp:  time.Now(),
		})
		return
	}

	// Drone encontrado — pode estar em base local ou remota
	b.fila.Desenfileirar()
	b.marcarOcupado(drone.DroneID, oc.ID)

	// Sincroniza estado "ocupado" com a malha imediatamente
	d := drone
	d.Estado = models.DroneDespachado
	d.OcorrenciaID = oc.ID
	b.broadcastVizinhos(models.MensagemBroker{
		Tipo:      models.MsgSincDrone,
		BrokerID:  b.id,
		Drone:     &d,
		Timestamp: time.Now(),
	})

	cmd := models.ComandoBase{
		Tipo:         models.CmdDespacharDrone,
		DroneID:      drone.DroneID,
		OcorrenciaID: oc.ID,
		SetorDestino: oc.SetorOrigem,
		PosicaoAlvo:  oc.Posicao,
		Timestamp:    time.Now(),
	}

	// Envia comando para a base que gerencia o drone (local ou remota via broker)
	b.basesMu.RLock()
	conn, ehLocal := b.bases[drone.BaseID]
	b.basesMu.RUnlock()

	if ehLocal {
		conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		json.NewEncoder(conn).Encode(cmd) //nolint
		b.logger.Printf("Despachado drone %s (base local %s) → ocorrência %s [%s] dist=%.0f",
			drone.DroneID, drone.BaseID, oc.ID, oc.Criticidade,
			oc.Posicao.Distancia(drone.Posicao))
	} else {
		// Drone em base remota — solicita ao broker responsável que execute o despacho
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:         models.MsgDroneDespachado,
			BrokerID:     b.id,
			DroneID:      drone.DroneID,
			BaseID:       drone.BaseID,
			OcorrenciaID: oc.ID,
			Ocorrencia:   &oc,
			Timestamp:    time.Now(),
		})
		b.logger.Printf("Solicitado despacho remoto: drone %s (base %s broker %s) → %s",
			drone.DroneID, drone.BaseID, drone.BrokerID, oc.ID)
	}

	// Notifica toda a malha que a ocorrência foi atendida
	b.atendidosMu.Lock()
	b.atendidos[oc.ID] = true
	b.atendidosMu.Unlock()

	b.broadcastVizinhos(models.MensagemBroker{
		Tipo:         models.MsgDroneDespachado,
		BrokerID:     b.id,
		DroneID:      drone.DroneID,
		BaseID:       drone.BaseID,
		OcorrenciaID: oc.ID,
		Timestamp:    time.Now(),
	})
}

// ── Ociosidade ────────────────────────────────────────────────────────────────

func (b *Broker) loopVerificarOciosidade() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		agora := time.Now()
		b.dronesMu.RLock()
		for _, d := range b.drones {
			if d.Estado != models.DroneDisponivel {
				continue
			}
			if d.DisponiveisDesde.IsZero() {
				continue
			}
			ocioso := agora.Sub(d.DisponiveisDesde)
			if ocioso > ociosidadeTimeout {
				b.logger.Printf("[OCIOSIDADE] Drone %s disponível há %s sem despacho (base %s bateria=%d%%)",
					d.DroneID, ocioso.Round(time.Second), d.BaseID, d.Bateria)
			}
		}
		b.dronesMu.RUnlock()
	}
}

// ── Brokers vizinhos ──────────────────────────────────────────────────────────

func (b *Broker) conectarVizinhos(enderecos string) {
	for _, addr := range splitCSV(enderecos) {
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
		backoff = 2 * time.Second

		chaveTemp := addr
		b.vizinhosMu.Lock()
		b.vizinhos[chaveTemp] = conn
		b.vizinhosMu.Unlock()

		scanner := bufio.NewScanner(conn)
		idReal := chaveTemp
		for scanner.Scan() {
			var msg models.MensagemBroker
			if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
				continue
			}
			if msg.BrokerID != "" && msg.BrokerID != idReal {
				b.vizinhosMu.Lock()
				delete(b.vizinhos, idReal)
				b.vizinhos[msg.BrokerID] = conn
				b.vizinhosMu.Unlock()
				idReal = msg.BrokerID
			}
			b.processarMensagemBroker(msg)
		}

		b.vizinhosMu.Lock()
		delete(b.vizinhos, idReal)
		b.vizinhosMu.Unlock()
		conn.Close()
		b.logger.Printf("Vizinho %s desconectou — reconectando em %s", addr, backoff)
		time.Sleep(backoff)
		backoff = 2 * time.Second
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

func (b *Broker) processarMensagemBroker(msg models.MensagemBroker) {
	b.heartbeatMu.Lock()
	b.ultimoHB[msg.BrokerID] = time.Now()
	b.heartbeatMu.Unlock()

	switch msg.Tipo {

	case models.MsgHeartbeat:
		// heartbeat já registrado acima

	case models.MsgSincDrone:
		// Atualiza estado global do drone recebido de outro broker
		if msg.Drone == nil {
			return
		}
		b.dronesMu.Lock()
		existing, ok := b.drones[msg.Drone.DroneID]
		if !ok || msg.Drone.UltimaVez.After(existing.UltimaVez) {
			// Preserva DisponiveisDesde local se existir
			if ok && msg.Drone.Estado == models.DroneDisponivel && existing.DisponiveisDesde.IsZero() {
				msg.Drone.DisponiveisDesde = time.Now()
			}
			b.drones[msg.Drone.DroneID] = *msg.Drone
		}
		b.dronesMu.Unlock()

	case models.MsgMissaoConcluida:
		b.logger.Printf("Missão concluída: drone %s liberou ocorrência %s (broker %s)",
			msg.DroneID, msg.OcorrenciaID, msg.BrokerID)

	case models.MsgRequisicaoDrone:
		if msg.Ocorrencia == nil {
			return
		}
		oc := *msg.Ocorrencia
		b.atendidosMu.Lock()
		jaAtendida := b.atendidos[oc.ID]
		b.atendidosMu.Unlock()
		if jaAtendida {
			return
		}
		// Enfileira a ocorrência do vizinho — será despachada pelo loop local
		b.fila.Enfileirar(oc)

	case models.MsgDroneDespachado:
		// Ocorrência atendida — remove da fila local e marca como atendida
		b.atendidosMu.Lock()
		b.atendidos[msg.OcorrenciaID] = true
		b.atendidosMu.Unlock()
		b.fila.Remover(msg.OcorrenciaID)

		// Se o drone pertence a uma base local, executa o comando
		b.basesMu.RLock()
		conn, ehLocal := b.bases[msg.BaseID]
		b.basesMu.RUnlock()
		if ehLocal && msg.Ocorrencia != nil {
			cmd := models.ComandoBase{
				Tipo:         models.CmdDespacharDrone,
				DroneID:      msg.DroneID,
				OcorrenciaID: msg.OcorrenciaID,
				SetorDestino: msg.Ocorrencia.SetorOrigem,
				PosicaoAlvo:  msg.Ocorrencia.Posicao,
				Timestamp:    time.Now(),
			}
			conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			json.NewEncoder(conn).Encode(cmd) //nolint
			b.logger.Printf("Executando despacho remoto: drone %s → ocorrência %s", msg.DroneID, msg.OcorrenciaID)
		}

		b.logger.Printf("Ocorrência %s atendida por %s (drone %s)", msg.OcorrenciaID, msg.BrokerID, msg.DroneID)

	case models.MsgDronePerdido:
		b.logger.Printf("Drone %s perdido: %s (base %s broker %s)",
			msg.DroneID, msg.Motivo, msg.BaseID, msg.BrokerID)
		// Atualiza estado global
		b.dronesMu.Lock()
		if d, ok := b.drones[msg.DroneID]; ok {
			d.Estado = models.DroneAbatido
			d.UltimaVez = time.Now()
			b.drones[msg.DroneID] = d
		}
		b.dronesMu.Unlock()

	case models.MsgDroneLiberado:
		b.dronesMu.Lock()
		if d, ok := b.drones[msg.DroneID]; ok {
			d.Estado = models.DroneDisponivel
			d.OcorrenciaID = ""
			d.DisponiveisDesde = time.Now()
			d.UltimaVez = time.Now()
			b.drones[msg.DroneID] = d
		}
		b.dronesMu.Unlock()

	case models.MsgRealocacaoDrones:
		// Outro broker perdeu sua base — tenta absorver os drones órfãos
		if len(msg.DronesOrfaos) == 0 {
			return
		}
		b.basesMu.RLock()
		var baseID string
		var conn net.Conn
		for bid, c := range b.bases {
			baseID = bid
			conn = c
			break
		}
		b.basesMu.RUnlock()
		if baseID == "" {
			return // sem base disponível aqui
		}
		b.enviarDronesParaBase(conn, baseID, msg.DronesOrfaos)
		b.logger.Printf("Absorvidos %d drones órfãos de %s na base %s",
			len(msg.DronesOrfaos), msg.BrokerID, baseID)

	case models.MsgReplicaFila:
		for _, oc := range msg.FilaPendente {
			b.fila.Enfileirar(oc)
		}
		b.logger.Printf("Fila replicada de %s: %d ocorrências", msg.BrokerID, len(msg.FilaPendente))
	}
}

// ── Sincronização global de drones ────────────────────────────────────────────

func (b *Broker) sincronizarDrones(drones []models.InfoDrone) {
	for i := range drones {
		d := drones[i]
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:      models.MsgSincDrone,
			BrokerID:  b.id,
			Drone:     &d,
			Timestamp: time.Now(),
		})
	}
}

// enviarSincGlobal envia o estado de todos os drones conhecidos para um novo vizinho.
func (b *Broker) enviarSincGlobal(conn net.Conn) {
	b.dronesMu.RLock()
	drones := make([]models.InfoDrone, 0, len(b.drones))
	for _, d := range b.drones {
		drones = append(drones, d)
	}
	b.dronesMu.RUnlock()

	for i := range drones {
		d := drones[i]
		msg := models.MensagemBroker{
			Tipo:      models.MsgSincDrone,
			BrokerID:  b.id,
			Drone:     &d,
			Timestamp: time.Now(),
		}
		conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		json.NewEncoder(conn).Encode(msg) //nolint
	}
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
				b.logger.Printf("Broker %s presumido morto (sem HB há %s)",
					id, agora.Sub(ultimo).Round(time.Second))
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

// ── Broadcast e utilitários ───────────────────────────────────────────────────

func (b *Broker) broadcastVizinhos(msg models.MensagemBroker) {
	b.vizinhosMu.RLock()
	defer b.vizinhosMu.RUnlock()
	for id, conn := range b.vizinhos {
		id, conn := id, conn
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

func splitCSV(s string) []string {
	var res []string
	cur := ""
	for _, c := range s {
		if c == ',' {
			if cur != "" {
				res = append(res, cur)
				cur = ""
			}
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		res = append(res, cur)
	}
	return res
}
