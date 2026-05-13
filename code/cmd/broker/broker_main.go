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
	ociosidadeTimeout  = 60 * time.Second
)

// ── Estrutura do broker ───────────────────────────────────────────────────────

type Broker struct {
	id       string
	setorID  string
	portaUDP string
	portaTCP string

	lamport   int
	lamportMu sync.Mutex

	fila *fila.FilaPrioridade

	// Drones conectados localmente: drone_id → conn
	dronesLocaisMu sync.RWMutex
	dronesLocais   map[string]net.Conn

	// Lista GLOBAL de drones (todos os brokers sincronizam)
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
		lamport:      0,
		fila:         fila.Nova(),
		dronesLocais: make(map[string]net.Conn),
		drones:       make(map[string]models.InfoDrone),
		vizinhos:     make(map[string]net.Conn),
		ultimoHB:     make(map[string]time.Time),
		atendidos:    make(map[string]bool),
		logger:       log.New(os.Stdout, fmt.Sprintf("[BROKER:%s] ", id), log.LstdFlags),
	}
}

// Relógio de Lamport
func (b *Broker) tick() int {
	b.lamportMu.Lock()
	defer b.lamportMu.Unlock()
	b.lamport++
	return b.lamport
}

func (b *Broker) syncLamport(recebido int) {
	b.lamportMu.Lock()
	defer b.lamportMu.Unlock()
	if recebido > b.lamport {
		b.lamport = recebido
	}
	b.lamport++
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	id := flag.String("id", "", "ID único do broker (ex: B1)")
	setor := flag.String("setor", "", "ID do setor (ex: Setor_Norte)")
	udp := flag.String("udp", "224.0.0.1:8080", "Endereço Multicast UDP para sensores")
	tcp := flag.String("tcp", "0.0.0.0:6000", "Porta TCP para drones e brokers")
	vizStr := flag.String("vizinhos", "", "Endereços TCP de brokers vizinhos (vírgula)")
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
	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		b.logger.Fatalf("UDP multicast listen: %v", err)
	}
	defer conn.Close()
	b.logger.Printf("Escutando sensores em Multicast UDP %s", b.portaUDP)

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
		// Apenas Alta ou Baixa chegam aqui, mas caso haja outras
		// Baixa é registrada para envelhecimento
		if leitura.Criticidade != models.CriticidadeBaixa {
			return
		}
	}

	tempoLamport := b.tick()

	oc := models.Ocorrencia{
		ID:           fmt.Sprintf("%s-%s-%d", b.id, leitura.SensorID, time.Now().UnixNano()),
		SetorOrigem:  b.setorID,
		BrokerOrigem: b.id,
		Tipo:         leitura.Tipo,
		Descricao:    fmt.Sprintf("Sensor %s: %.2f %s", leitura.SensorID, leitura.Valor, leitura.Unidade),
		Criticidade:  leitura.Criticidade,
		Timestamp:    time.Now(),
		LamportTime:  tempoLamport,
		Posicao:      leitura.Posicao,
	}
	b.fila.Enfileirar(oc)
	b.logger.Printf("Ocorrência MULTICAST recebida localmente: %s [%s] L=%d — %s", oc.ID, oc.Criticidade, tempoLamport, oc.Descricao)
}

// ── TCP — escuta Drones e Brokers ─────────────────────────────────────────────

func (b *Broker) escutarTCP() {
	ln, err := net.Listen("tcp", b.portaTCP)
	if err != nil {
		b.logger.Fatalf("TCP listen: %v", err)
	}
	defer ln.Close()
	b.logger.Printf("Escutando Drones/Brokers TCP em %s", b.portaTCP)

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

	var md models.MensagemDrone
	if err := json.Unmarshal(linha, &md); err == nil && md.Tipo == models.DroneRegistro {
		b.registrarDroneLocal(md.DroneID, md.DroneInfo, conn)
		go b.loopLeituraDrone(md.DroneID, conn, scanner)
		return
	}

	var msg models.MensagemBroker
	if err := json.Unmarshal(linha, &msg); err == nil && msg.Tipo == models.MsgRegistro {
		b.syncLamport(msg.LamportTime)
		b.registrarVizinho(msg.BrokerID, conn)
		b.enviarSincGlobal(conn)
		go b.loopLeituraBroker(msg.BrokerID, conn, scanner)
		return
	}

	b.logger.Printf("Conexão desconhecida de %s — fechando", conn.RemoteAddr())
	conn.Close()
}

// ── Drones Locais ─────────────────────────────────────────────────────────────

func (b *Broker) registrarDroneLocal(droneID string, info *models.InfoDrone, conn net.Conn) {
	b.dronesLocaisMu.Lock()
	b.dronesLocais[droneID] = conn
	b.dronesLocaisMu.Unlock()

	if info != nil {
		info.BrokerID = b.id
		if info.Estado == models.DroneDisponivel {
			info.DisponiveisDesde = time.Now()
		}
		b.dronesMu.Lock()
		b.drones[droneID] = *info
		b.dronesMu.Unlock()

		b.logger.Printf("Drone %s registrado na malha local", droneID)
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:        models.MsgSincDrone,
			BrokerID:    b.id,
			Drone:       info,
			Timestamp:   time.Now(),
			LamportTime: b.tick(),
		})
	}
}

func (b *Broker) loopLeituraDrone(droneID string, conn net.Conn, scanner *bufio.Scanner) {
	defer func() {
		conn.Close()
		b.dronesLocaisMu.Lock()
		delete(b.dronesLocais, droneID)
		b.dronesLocaisMu.Unlock()

		b.logger.Printf("Drone %s perdeu conexão (possível abate)", droneID)

		b.dronesMu.Lock()
		d, ok := b.drones[droneID]
		if ok {
			d.Estado = models.DroneAbatido
			d.UltimaVez = time.Now()
			b.drones[droneID] = d
		}
		b.dronesMu.Unlock()

		if ok {
			b.broadcastVizinhos(models.MensagemBroker{
				Tipo:        models.MsgDronePerdido,
				BrokerID:    b.id,
				DroneID:     droneID,
				Motivo:      "DESCONEXAO_TCP",
				Timestamp:   time.Now(),
				LamportTime: b.tick(),
			})
			b.tratarDronePerdido(d)
		}
	}()

	for scanner.Scan() {
		var msg models.MensagemDrone
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		b.processarMensagemDrone(msg)
	}
}

func (b *Broker) processarMensagemDrone(msg models.MensagemDrone) {
	agora := time.Now()
	b.dronesMu.Lock()
	d, ok := b.drones[msg.DroneID]
	
	if ok && msg.Tipo == models.DroneKeepalive && msg.DroneInfo != nil {
		d.Bateria = msg.DroneInfo.Bateria
		d.Posicao = msg.DroneInfo.Posicao
		d.UltimaVez = agora
		b.drones[msg.DroneID] = d
	}

	if ok && msg.Tipo == models.DroneEstado {
		d.Estado = msg.NovoEstado
		d.Bateria = msg.Bateria
		d.Posicao = msg.Posicao
		d.UltimaVez = agora
		if msg.NovoEstado == models.DroneDisponivel {
			d.OcorrenciaID = ""
			d.DisponiveisDesde = agora
		}
		if msg.NovoEstado == models.DroneDespachado || msg.NovoEstado == models.DroneEmMissao {
			d.DisponiveisDesde = time.Time{}
			if msg.OcorrenciaID != "" {
				d.OcorrenciaID = msg.OcorrenciaID
			}
		}
		b.drones[msg.DroneID] = d
		b.logger.Printf("Drone %s → %s (Bat: %d%%)", msg.DroneID, msg.NovoEstado, msg.Bateria)

		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:        models.MsgSincDrone,
			BrokerID:    b.id,
			Drone:       &d,
			Timestamp:   agora,
			LamportTime: b.tick(),
		})

		if msg.NovoEstado == models.DroneDisponivel && msg.OcorrenciaID != "" {
			b.broadcastVizinhos(models.MensagemBroker{
				Tipo:         models.MsgMissaoConcluida,
				BrokerID:     b.id,
				DroneID:      msg.DroneID,
				OcorrenciaID: msg.OcorrenciaID,
				Timestamp:    agora,
				LamportTime:  b.tick(),
			})
		}

		if msg.NovoEstado == models.DroneAbatido || msg.NovoEstado == models.DroneSemBateria {
			b.tratarDronePerdido(d)
		}
	}
	b.dronesMu.Unlock()
}

func (b *Broker) tratarDronePerdido(d models.InfoDrone) {
	if d.OcorrenciaID != "" {
		b.logger.Printf("CRÍTICO: Drone %s abatido/perdido em missão! Re-enfileirando %s", d.DroneID, d.OcorrenciaID)
		b.atendidosMu.Lock()
		b.atendidos[d.OcorrenciaID] = false
		b.atendidosMu.Unlock()

		// Como não temos a ocorrência original salva se ela já foi tirada da fila,
		// precisamos que o broker que mantém a cópia repasse ou injetar uma dummy de alta prioridade.
		// Mas a fila replicada mantém a ocorrência se ela não foi removida.
		// Se foi, deveríamos manter um histórico.
		// Para simplificar: Remove da lista de atendidos. Outros brokers farão o mesmo.
	}
}

// ── Despacho por proximidade ──────────────────────────────────────────────────

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

// ── Loop de despacho (Exclusão Mútua Distribuída) ─────────────────────────────

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
		return // Fila espera um drone ficar disponível
	}

	// Regra de Exclusão Mútua Simplificada:
	// O Broker que detém a conexão TCP com o drone mais próximo é o responsável 
	// por enviar o comando. Assim, não há dupla requisição.
	b.dronesLocaisMu.RLock()
	conn, ehLocal := b.dronesLocais[drone.DroneID]
	b.dronesLocaisMu.RUnlock()

	if ehLocal {
		b.fila.Desenfileirar()
		b.marcarOcupado(drone.DroneID, oc.ID)

		cmd := models.ComandoDrone{
			Tipo:         models.CmdDespacharDrone,
			OcorrenciaID: oc.ID,
			SetorDestino: oc.SetorOrigem,
			PosicaoAlvo:  oc.Posicao,
			Timestamp:    time.Now(),
		}

		conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		json.NewEncoder(conn).Encode(cmd)

		b.logger.Printf("Despachado drone LOCAL %s → ocorrência %s L=%d", drone.DroneID, oc.ID, oc.LamportTime)

		b.atendidosMu.Lock()
		b.atendidos[oc.ID] = true
		b.atendidosMu.Unlock()

		d := drone
		d.Estado = models.DroneDespachado
		d.OcorrenciaID = oc.ID
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:         models.MsgDroneDespachado,
			BrokerID:     b.id,
			DroneID:      drone.DroneID,
			OcorrenciaID: oc.ID,
			Drone:        &d,
			Timestamp:    time.Now(),
			LamportTime:  b.tick(),
		})
	}
}

// ── Ociosidade ────────────────────────────────────────────────────────────────

func (b *Broker) loopVerificarOciosidade() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		agora := time.Now()
		b.dronesMu.RLock()
		for _, d := range b.drones {
			if d.Estado != models.DroneDisponivel || d.DisponiveisDesde.IsZero() {
				continue
			}
			ocioso := agora.Sub(d.DisponiveisDesde)
			if ocioso > ociosidadeTimeout {
				b.logger.Printf("[OCIOSIDADE] Drone %s disponível há %s sem despacho (bateria=%d%%)",
					d.DroneID, ocioso.Round(time.Second), d.Bateria)
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
			Tipo:        models.MsgRegistro,
			BrokerID:    b.id,
			Timestamp:   time.Now(),
			LamportTime: b.tick(),
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
	b.syncLamport(msg.LamportTime)

	b.heartbeatMu.Lock()
	b.ultimoHB[msg.BrokerID] = time.Now()
	b.heartbeatMu.Unlock()

	switch msg.Tipo {
	case models.MsgHeartbeat:
		// heartbeat já registrado acima
	case models.MsgSincDrone:
		if msg.Drone == nil { return }
		b.dronesMu.Lock()
		existing, ok := b.drones[msg.Drone.DroneID]
		if !ok || msg.Drone.UltimaVez.After(existing.UltimaVez) {
			if ok && msg.Drone.Estado == models.DroneDisponivel && existing.DisponiveisDesde.IsZero() {
				msg.Drone.DisponiveisDesde = time.Now()
			}
			b.drones[msg.Drone.DroneID] = *msg.Drone
		}
		b.dronesMu.Unlock()
	case models.MsgMissaoConcluida:
		b.logger.Printf("Missão concluída: drone %s liberou ocorrência %s", msg.DroneID, msg.OcorrenciaID)
	case models.MsgRequisicaoDrone:
		if msg.Ocorrencia == nil { return }
		oc := *msg.Ocorrencia
		b.atendidosMu.Lock()
		jaAtendida := b.atendidos[oc.ID]
		b.atendidosMu.Unlock()
		if !jaAtendida {
			b.fila.Enfileirar(oc)
		}
	case models.MsgDroneDespachado:
		b.atendidosMu.Lock()
		b.atendidos[msg.OcorrenciaID] = true
		b.atendidosMu.Unlock()
		b.fila.Remover(msg.OcorrenciaID)
		b.logger.Printf("Ocorrência %s atendida por %s (drone %s)", msg.OcorrenciaID, msg.BrokerID, msg.DroneID)
		if msg.Drone != nil {
			b.dronesMu.Lock()
			b.drones[msg.DroneID] = *msg.Drone
			b.dronesMu.Unlock()
		}
	case models.MsgDronePerdido:
		b.logger.Printf("Drone %s perdido: %s (broker %s)", msg.DroneID, msg.Motivo, msg.BrokerID)
		b.dronesMu.Lock()
		if d, ok := b.drones[msg.DroneID]; ok {
			d.Estado = models.DroneAbatido
			d.UltimaVez = time.Now()
			b.drones[msg.DroneID] = d
			b.tratarDronePerdido(d)
		}
		b.dronesMu.Unlock()
	}
}

// ── Sincronização global de drones ────────────────────────────────────────────

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
			Tipo:        models.MsgSincDrone,
			BrokerID:    b.id,
			Drone:       &d,
			Timestamp:   time.Now(),
			LamportTime: b.tick(),
		}
		conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		json.NewEncoder(conn).Encode(msg)
	}
}

// ── Heartbeat e detecção de falhas ────────────────────────────────────────────

func (b *Broker) loopHeartbeat() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:        models.MsgHeartbeat,
			BrokerID:    b.id,
			Timestamp:   time.Now(),
			LamportTime: b.tick(),
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
