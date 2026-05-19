package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
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

	ocorrenciasMu sync.RWMutex
	ocorrencias   map[string]models.Ocorrencia

	peersConhecidosMu sync.RWMutex
	peersConhecidos   map[string]bool // "IP:PORT" -> true

	setoresConhecidosMu sync.RWMutex
	setoresConhecidos   map[string]string // BrokerID -> SetorID

	brokersMortosMu sync.RWMutex
	brokersMortos   map[string]bool // BrokerID -> true (mortos)

	logger *log.Logger
}

func novoBroker(id, setorID, portaUDP, portaTCP string) *Broker {
	return &Broker{
		id:                id,
		setorID:           setorID,
		portaUDP:          portaUDP,
		portaTCP:          portaTCP,
		lamport:           0,
		fila:              fila.Nova(),
		dronesLocais:      make(map[string]net.Conn),
		drones:            make(map[string]models.InfoDrone),
		vizinhos:          make(map[string]net.Conn),
		ultimoHB:          make(map[string]time.Time),
		atendidos:         make(map[string]bool),
		ocorrencias:       make(map[string]models.Ocorrencia),
		peersConhecidos:   make(map[string]bool),
		setoresConhecidos: make(map[string]string),
		brokersMortos:     make(map[string]bool),
		logger:            log.New(os.Stdout, fmt.Sprintf("[BROKER:%s] ", id), log.LstdFlags),
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
	udp := flag.String("udp", "224.1.2.3:9876", "Endereço Multicast UDP para sensores")
	tcp := flag.String("tcp", "0.0.0.0:6000", "Porta TCP para drones e brokers")
	vizStr := flag.String("vizinhos", "", "Endereços TCP de brokers vizinhos (vírgula)")
	lider := flag.String("lider", "", "IP:PORT do Broker Lider para descoberta (se vazio, assume como lider)")
	flag.Parse()

	if *id == "" || *setor == "" {
		fmt.Fprintln(os.Stderr, "Uso: broker -id B1 -setor Setor_Norte [-udp :8080] [-tcp :6000] [-vizinhos IP:6000,IP:6000] [-lider IP:6000]")
		os.Exit(1)
	}

	b := novoBroker(*id, *setor, *udp, *tcp)
	b.logger.Printf("Iniciando — setor=%s UDP=%s TCP=%s", *setor, *udp, *tcp)

	go b.escutarTCP()
	go b.escutarUDP()
	if *lider != "" {
		b.logger.Printf("Modo Seguidor: Conectando ao Líder de Descoberta em %s", *lider)
		go b.conectarLider(*lider)
	} else if *vizStr != "" {
		go b.conectarVizinhos(*vizStr)
	} else {
		b.logger.Printf("Modo Líder: Aguardando brokers se conectarem para descoberta.")
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

func (b *Broker) responsavelPorSetor(setorDaLeitura string) bool {
	// Se é o meu setor, eu sempre sou o responsável principal
	if setorDaLeitura == b.setorID {
		return true
	}

	// Procura quem é o dono original deste setor
	donoOriginal := ""
	b.setoresConhecidosMu.RLock()
	for brokerID, setor := range b.setoresConhecidos {
		if setor == setorDaLeitura {
			donoOriginal = brokerID
			break
		}
	}
	b.setoresConhecidosMu.RUnlock()

	if donoOriginal == "" {
		return false
	}

	// O dono original está vivo?
	b.brokersMortosMu.RLock()
	donoMorto := b.brokersMortos[donoOriginal]
	b.brokersMortosMu.RUnlock()

	if !donoMorto {
		return false // Deixa o dono original cuidar!
	}

	// Lógica de Ring Failover
	b.setoresConhecidosMu.RLock()
	var todos []string
	for brokerID := range b.setoresConhecidos {
		todos = append(todos, brokerID)
	}
	b.setoresConhecidosMu.RUnlock()
	
	encontrouEu := false
	for _, br := range todos {
		if br == b.id { encontrouEu = true; break }
	}
	if !encontrouEu { todos = append(todos, b.id) }

	sort.Strings(todos)

	idxDono := -1
	for i, br := range todos {
		if br == donoOriginal {
			idxDono = i
			break
		}
	}

	if idxDono == -1 { return false }

	n := len(todos)
	for i := 1; i <= n; i++ {
		idx := (idxDono - i) % n
		if idx < 0 {
			idx += n
		}
		candidato := todos[idx]
		
		vivo := true
		if candidato != b.id {
			b.brokersMortosMu.RLock()
			vivo = !b.brokersMortos[candidato]
			b.brokersMortosMu.RUnlock()
		}

		if vivo {
			if candidato == b.id {
				b.logger.Printf("[FAILOVER ativado] Assumindo leitura do setor morto: %s", setorDaLeitura)
				return true
			}
			return false
		}
	}
	return false
}

func (b *Broker) processarLeitura(dados []byte) {
	var leitura models.LeituraSensor
	if err := json.Unmarshal(dados, &leitura); err != nil {
		return
	}

	b.logger.Printf("[UDP RECEBIDO] Leitura do Sensor %s, Setor %s, Criticidade %s", leitura.SensorID, leitura.SetorID, leitura.Criticidade.String())

	// Filtro Regional com tolerância a falhas (Anel)
	if !b.responsavelPorSetor(leitura.SetorID) {
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
		ID:           fmt.Sprintf("%s-%d", leitura.SensorID, leitura.Timestamp.UnixNano()),
		SetorOrigem:  leitura.SetorID,
		BrokerOrigem: b.id,
		Tipo:         leitura.Tipo,
		Descricao:    fmt.Sprintf("Sensor %s: %.2f %s", leitura.SensorID, leitura.Valor, leitura.Unidade),
		Criticidade:  leitura.Criticidade,
		Timestamp:    leitura.Timestamp,
		LamportTime:  tempoLamport,
		Posicao:      leitura.Posicao,
	}
	b.ocorrenciasMu.Lock()
	b.ocorrencias[oc.ID] = oc
	b.ocorrenciasMu.Unlock()

	b.fila.Enfileirar(oc)
	b.logger.Printf("Ocorrência MULTICAST recebida localmente: %s [%s] L=%d — %s", oc.ID, oc.Criticidade, tempoLamport, oc.Descricao)

	b.broadcastVizinhos(models.MensagemBroker{
		Tipo:        models.MsgRequisicaoDrone,
		BrokerID:    b.id,
		Ocorrencia:  &oc,
		Timestamp:   time.Now(),
		LamportTime: tempoLamport,
	})
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
		b.logger.Printf("[DRONE MSG RECEBIDA] Registro do Drone %s na malha local", md.DroneID)
		b.registrarDroneLocal(md.DroneID, md.DroneInfo, conn)
		go b.loopLeituraDrone(md.DroneID, conn, scanner)
		return
	}

	var msg models.MensagemBroker
	if err := json.Unmarshal(linha, &msg); err == nil && (msg.Tipo == models.MsgRegistro || msg.Tipo == models.MsgDiscovery) {
		b.logger.Printf("[TCP RECEBIDO] Conexão inicial de %s: tipo=%s", msg.BrokerID, msg.Tipo)
		b.syncLamport(msg.LamportTime)
		b.registrarVizinho(msg.BrokerID, conn)
		b.enviarSincGlobal(conn)
		
		if strings.HasPrefix(msg.BrokerID, "MONITOR-") {
			b.peersConhecidosMu.Lock()
			listaPeers := make([]string, 0, len(b.peersConhecidos))
			for p := range b.peersConhecidos {
				listaPeers = append(listaPeers, p)
			}
			b.peersConhecidosMu.Unlock()

			resposta := models.MensagemBroker{
				Tipo:        models.MsgPeerList,
				BrokerID:    b.id,
				Peers:       listaPeers,
				Timestamp:   time.Now(),
				LamportTime: b.tick(),
			}
			conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			json.NewEncoder(conn).Encode(resposta)
		}
		
		// Trata as mensagens iniciais e entra no loop
		b.processarMensagemBroker(msg, conn)
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
		b.logger.Printf("[DRONE MSG RECEBIDA] Drone %s envia tipo=%s, estado=%s", msg.DroneID, msg.Tipo, msg.NovoEstado)
		b.processarMensagemDrone(msg)
	}
}

func (b *Broker) processarMensagemDrone(msg models.MensagemDrone) {
	agora := time.Now()
	b.dronesMu.Lock()
	d, ok := b.drones[msg.DroneID]
	
	if ok && msg.Tipo == models.DroneKeepalive && msg.DroneInfo != nil {
		d.Posicao = msg.DroneInfo.Posicao
		d.UltimaVez = agora
		b.drones[msg.DroneID] = d
		
		// Sincroniza a nova posição com os outros brokers
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:        models.MsgSincDrone,
			BrokerID:    b.id,
			Drone:       &d,
			Timestamp:   agora,
			LamportTime: b.tick(),
		})
	}

	if ok && msg.Tipo == models.DroneEstado {
		d.Estado = msg.NovoEstado
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
		b.logger.Printf("Drone %s → %s", msg.DroneID, msg.NovoEstado)

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

		if msg.NovoEstado == models.DroneAbatido {
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

		b.ocorrenciasMu.RLock()
		oc, ok := b.ocorrencias[d.OcorrenciaID]
		b.ocorrenciasMu.RUnlock()

		if ok {
			b.fila.Enfileirar(oc)
		}
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
		if !encontrou || dist < menorDist {
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

	// Nova Lógica Pró-Ativa: Se eu tenho um drone local, eu tento atender o topo da fila.
	// Isso evita o impasse onde brokers ficam esperando uns aos outros.
	
	b.dronesLocaisMu.RLock()
	var droneAlvo string
	var conn net.Conn
	for id, c := range b.dronesLocais {
		b.dronesMu.RLock()
		d, ok := b.drones[id]
		b.dronesMu.RUnlock()
		if ok && d.Disponivel() {
			droneAlvo = id
			conn = c
			break
		}
	}
	b.dronesLocaisMu.RUnlock()

	if droneAlvo != "" {
		b.fila.Desenfileirar()
		b.marcarOcupado(droneAlvo, oc.ID)

		cmd := models.ComandoDrone{
			Tipo:         models.CmdDespacharDrone,
			OcorrenciaID: oc.ID,
			SetorDestino: oc.SetorOrigem,
			PosicaoAlvo:  oc.Posicao,
			Timestamp:    time.Now(),
		}

		conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if err := json.NewEncoder(conn).Encode(cmd); err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") {
				b.logger.Printf("Erro ao enviar comando para drone %s: %v. Fechando conexão.", droneAlvo, err)
			}
			conn.Close()

			// Re-enfileira a ocorrencia
			b.atendidosMu.Lock()
			b.atendidos[oc.ID] = false
			b.atendidosMu.Unlock()
			b.fila.Enfileirar(oc)
		} else {
			b.logger.Printf("[DRONE COMANDO ENVIADO] Para drone %s, comando=%s, ocorrencia=%s", droneAlvo, cmd.Tipo, oc.ID)
			b.logger.Printf("Despacho PRÓ-ATIVO: drone %s → ocorrência %s", droneAlvo, oc.ID)

			b.atendidosMu.Lock()
			b.atendidos[oc.ID] = true
			b.atendidosMu.Unlock()

			b.dronesMu.RLock()
			dInfo := b.drones[droneAlvo]
			b.dronesMu.RUnlock()
			
			b.broadcastVizinhos(models.MensagemBroker{
				Tipo:         models.MsgDroneDespachado,
				BrokerID:     b.id,
				DroneID:      droneAlvo,
				OcorrenciaID: oc.ID,
				Drone:        &dInfo,
				Timestamp:    time.Now(),
				LamportTime:  b.tick(),
			})
		}
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
				b.logger.Printf("[OCIOSIDADE] Drone %s disponível há %s sem despacho",
					d.DroneID, ocioso.Round(time.Second))
			}
		}
		b.dronesMu.RUnlock()
	}
}

// ── Brokers vizinhos ──────────────────────────────────────────────────────────

func (b *Broker) conectarLider(addr string) {
	// A porta TCP em que ESTE broker escuta
	_, portaLocal, _ := net.SplitHostPort(b.portaTCP)
	
	backoff := 2 * time.Second
	for {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			b.logger.Printf("Falha ao conectar no líder %s: %v — retry em %s", addr, err, backoff)
			time.Sleep(backoff)
			if backoff < 30*time.Second { backoff *= 2 }
			continue
		}

		b.logger.Printf("Conectado ao líder %s! Solicitando Discovery...", addr)
		reg := models.MensagemBroker{
			Tipo:        models.MsgDiscovery,
			BrokerID:    b.id,
			SetorID:     b.setorID,
			Motivo:      portaLocal, // Envia porta TCP local para o Líder
			Timestamp:   time.Now(),
			LamportTime: b.tick(),
		}
		if err := json.NewEncoder(conn).Encode(reg); err != nil {
			conn.Close()
			continue
		}
		b.logger.Printf("[TCP ENVIADO] Para lider %s, mensagem tipo=%s", addr, reg.Tipo)

		b.registrarVizinho(addr, conn)
		scanner := bufio.NewScanner(conn)
		go b.loopLeituraBroker("LIDER", conn, scanner)
		
		// Trava a rotina até que a conexão com o Líder caia
		<-make(chan struct{})
		break
	}
}

func (b *Broker) conectarVizinhos(enderecos string) {
	for _, addr := range splitCSV(enderecos) {
		go b.conectarVizinho(addr, false)
	}
}

func (b *Broker) conectarVizinho(addr string, isDiscovery bool) {
	backoff := 2 * time.Second
	for {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			if isDiscovery {
				// Para descoberta dinâmica, desiste após algumas tentativas
				if backoff > 10*time.Second {
					b.logger.Printf("Desistindo de conectar ao peer descoberto %s", addr)
					b.peersConhecidosMu.Lock()
					delete(b.peersConhecidos, addr)
					b.peersConhecidosMu.Unlock()
					return
				}
			}
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
			SetorID:     b.setorID,
			Timestamp:   time.Now(),
			LamportTime: b.tick(),
		}
		if err := json.NewEncoder(conn).Encode(reg); err != nil {
			conn.Close()
			continue
		}
		b.logger.Printf("[TCP ENVIADO] Para vizinho %s, mensagem tipo=%s", addr, reg.Tipo)

		b.logger.Printf("Conectado ao vizinho %s", addr)
		backoff = 2 * time.Second

		chaveTemp := addr
		b.vizinhosMu.Lock()
		b.vizinhos[chaveTemp] = conn
		b.vizinhosMu.Unlock()

		scanner := bufio.NewScanner(conn)
		idReal := chaveTemp
		for {
			conn.SetReadDeadline(time.Now().Add(heartbeatTimeout))
			if !scanner.Scan() {
				break
			}
			var msg models.MensagemBroker
			if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
				continue
			}
			if msg.Tipo != models.MsgHeartbeat {
				b.logger.Printf("[TCP RECEBIDO] De %s, mensagem tipo=%s", msg.BrokerID, msg.Tipo)
			}
			if msg.BrokerID != "" && msg.BrokerID != idReal {
				b.vizinhosMu.Lock()
				delete(b.vizinhos, idReal)
				b.vizinhos[msg.BrokerID] = conn
				b.vizinhosMu.Unlock()
				idReal = msg.BrokerID
			}
			b.processarMensagemBroker(msg, conn)
		}

		b.vizinhosMu.Lock()
		delete(b.vizinhos, idReal)
		b.vizinhosMu.Unlock()
		conn.Close()
		
		if isDiscovery {
			b.logger.Printf("Vizinho dinâmico %s desconectou.", addr)
			b.peersConhecidosMu.Lock()
			delete(b.peersConhecidos, addr)
			b.peersConhecidosMu.Unlock()
			return
		}
		
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
	for {
		conn.SetReadDeadline(time.Now().Add(heartbeatTimeout))
		if !scanner.Scan() {
			break
		}
		var msg models.MensagemBroker
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		if msg.Tipo != models.MsgHeartbeat {
			b.logger.Printf("[TCP RECEBIDO] De %s, mensagem tipo=%s", msg.BrokerID, msg.Tipo)
		}
		b.processarMensagemBroker(msg, conn)
	}
}

func (b *Broker) processarMensagemBroker(msg models.MensagemBroker, conn net.Conn) {
	b.syncLamport(msg.LamportTime)
	b.enviarParaMonitores(msg)

	b.heartbeatMu.Lock()
	b.ultimoHB[msg.BrokerID] = time.Now()
	b.heartbeatMu.Unlock()

	// Failover: Registrar setor e reviver broker se estiver morto
	if msg.SetorID != "" {
		b.setoresConhecidosMu.Lock()
		b.setoresConhecidos[msg.BrokerID] = msg.SetorID
		b.setoresConhecidosMu.Unlock()
	}

	b.brokersMortosMu.Lock()
	if b.brokersMortos[msg.BrokerID] {
		b.logger.Printf("Broker %s voltou à vida! Retornando o controle do setor %s para ele.", msg.BrokerID, msg.SetorID)
		b.brokersMortos[msg.BrokerID] = false
		b.broadcastVizinhos(models.MensagemBroker{
			Tipo:        models.MsgFailoverRecuperado,
			BrokerID:    msg.BrokerID,
			SetorID:     msg.SetorID,
			Timestamp:   time.Now(),
			LamportTime: b.tick(),
		})
	}
	b.brokersMortosMu.Unlock()

	switch msg.Tipo {
	case models.MsgHeartbeat:
		// heartbeat já registrado acima
	case models.MsgDiscovery:
		// Registro de novo broker via mecanismo de Líder
		portaRemota := msg.Motivo
		host, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
		peerAddr := host + ":" + portaRemota
		
		b.peersConhecidosMu.Lock()
		b.peersConhecidos[peerAddr] = true
		listaPeers := make([]string, 0, len(b.peersConhecidos))
		for p := range b.peersConhecidos {
			if p != peerAddr {
				listaPeers = append(listaPeers, p)
			}
		}
		b.peersConhecidosMu.Unlock()

		b.logger.Printf("Líder registrou novo peer %s (Broker %s). Enviando lista atualizada.", peerAddr, msg.BrokerID)
		
		resposta := models.MensagemBroker{
			Tipo:        models.MsgPeerList,
			BrokerID:    b.id,
			Peers:       listaPeers,
			Timestamp:   time.Now(),
			LamportTime: b.tick(),
		}
		conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if err := json.NewEncoder(conn).Encode(resposta); err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") {
				b.logger.Printf("Erro ao enviar PeerList para %s: %v. Fechando conexão.", peerAddr, err)
			}
			conn.Close()
		} else {
			b.logger.Printf("[TCP ENVIADO] Para novo peer %s, mensagem tipo=%s", peerAddr, resposta.Tipo)
		}

		// Avisa a todos os outros peers sobre esse novo
		broadcast := models.MensagemBroker{
			Tipo:        models.MsgPeerList,
			BrokerID:    b.id,
			Peers:       []string{peerAddr},
			Timestamp:   time.Now(),
			LamportTime: b.tick(),
		}
		b.broadcastVizinhos(broadcast)
		
	case models.MsgPeerList:
		// Atualiza lista local de peers conhecidos
		for _, peer := range msg.Peers {
			b.peersConhecidosMu.Lock()
			jaConhece := b.peersConhecidos[peer]
			if !jaConhece {
				b.peersConhecidos[peer] = true
			}
			b.peersConhecidosMu.Unlock()
			
			if !jaConhece {
				b.logger.Printf("Descoberto novo peer via Líder: %s. Conectando...", peer)
				go b.conectarVizinho(peer, true) // modoDiscovery = true
			}
		}
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
		b.ocorrenciasMu.Lock()
		delete(b.ocorrencias, msg.OcorrenciaID)
		b.ocorrenciasMu.Unlock()
	case models.MsgRequisicaoDrone:
		if msg.Ocorrencia == nil || msg.Ocorrencia.Criticidade == models.CriticidadeNula { return }
		oc := *msg.Ocorrencia
		b.atendidosMu.Lock()
		jaAtendida := b.atendidos[oc.ID]
		b.atendidosMu.Unlock()
		if !jaAtendida {
			b.ocorrenciasMu.Lock()
			b.ocorrencias[oc.ID] = oc
			b.ocorrenciasMu.Unlock()
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
		if err := json.NewEncoder(conn).Encode(msg); err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") {
				b.logger.Printf("Erro ao sincronizar drone %s: %v. Fechando conexão.", d.DroneID, err)
			}
			conn.Close()
			return
		} else {
			b.logger.Printf("[TCP ENVIADO] Para peer/monitor, mensagem tipo=%s (sinc drone %s)", msg.Tipo, d.DroneID)
		}
	}
}

// ── Heartbeat e detecção de falhas ────────────────────────────────────────────

func (b *Broker) loopHeartbeat() {
	ticker := time.NewTicker(heartbeatInterval / 2)
	defer ticker.Stop()
	for range ticker.C {
		hb := models.MensagemBroker{
			Tipo:        models.MsgHeartbeat,
			BrokerID:    b.id,
			SetorID:     b.setorID,
			Timestamp:   time.Now(),
			LamportTime: b.tick(),
		}
		b.broadcastVizinhos(hb)
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
				b.brokersMortosMu.Lock()
				if !b.brokersMortos[id] {
					b.logger.Printf("Broker %s presumido morto (sem HB há %s). Ativando rotinas de Failover!", id, agora.Sub(ultimo).Round(time.Second))
					b.brokersMortos[id] = true
					go b.verificarEAtivarFailover(id)
				}
				b.brokersMortosMu.Unlock()
			}
		}
		b.heartbeatMu.Unlock()
	}
}

func (b *Broker) verificarEAtivarFailover(deadBrokerID string) {
	b.setoresConhecidosMu.RLock()
	setorMorto, ok := b.setoresConhecidos[deadBrokerID]
	b.setoresConhecidosMu.RUnlock()
	if !ok {
		return
	}

	b.setoresConhecidosMu.RLock()
	var todos []string
	for brokerID := range b.setoresConhecidos {
		todos = append(todos, brokerID)
	}
	b.setoresConhecidosMu.RUnlock()

	encontrouEu := false
	for _, br := range todos {
		if br == b.id {
			encontrouEu = true
			break
		}
	}
	if !encontrouEu {
		todos = append(todos, b.id)
	}

	sort.Strings(todos)

	idxDono := -1
	for i, br := range todos {
		if br == deadBrokerID {
			idxDono = i
			break
		}
	}
	if idxDono == -1 {
		return
	}

	n := len(todos)
	for i := 1; i <= n; i++ {
		idx := (idxDono - i) % n
		if idx < 0 {
			idx += n
		}
		candidato := todos[idx]

		vivo := true
		if candidato != b.id {
			b.brokersMortosMu.RLock()
			vivo = !b.brokersMortos[candidato]
			b.brokersMortosMu.RUnlock()
		}

		if vivo {
			if candidato == b.id {
				b.logger.Printf("[FAILOVER ATIVADO] Eu (%s) assumo o setor '%s' do broker morto '%s'!", b.id, setorMorto, deadBrokerID)
				b.broadcastVizinhos(models.MensagemBroker{
					Tipo:        models.MsgFailover,
					BrokerID:    b.id,
					SetorID:     setorMorto,
					Motivo:      deadBrokerID,
					Timestamp:   time.Now(),
					LamportTime: b.tick(),
				})
			}
			break
		}
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
				if !strings.Contains(err.Error(), "use of closed network connection") {
					b.logger.Printf("Erro broadcast para %s: %v. Fechando conexão.", id, err)
				}
				conn.Close()
			} else {
				if msg.Tipo != models.MsgHeartbeat {
					b.logger.Printf("[TCP ENVIADO] Para broker/peer %s, mensagem tipo=%s", id, msg.Tipo)
				}
			}
		}()
	}
}

func (b *Broker) enviarParaMonitores(msg models.MensagemBroker) {
	b.vizinhosMu.RLock()
	defer b.vizinhosMu.RUnlock()
	for id, conn := range b.vizinhos {
		if strings.HasPrefix(id, "MONITOR-") {
			id, conn := id, conn
			go func() {
				conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if err := json.NewEncoder(conn).Encode(msg); err != nil {
					if !strings.Contains(err.Error(), "use of closed network connection") {
						b.logger.Printf("Erro ao encaminhar para monitor %s: %v. Fechando conexão.", id, err)
					}
					conn.Close()
				}
			}()
		}
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
