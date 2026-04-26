package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"HormuzNet/internal/models"
)

// ── Configuração ──────────────────────────────────────────────────────────────

const (
	missaoDurMin    = 8 * time.Second  // duração mínima de uma missão simulada
	missaoDurMax    = 20 * time.Second // duração máxima
	retornoDur      = 5 * time.Second  // tempo de retorno à base
	bateriaConsumo  = 5               // % de bateria consumida por missão
	bateriaRecarga  = 2               // % de bateria recarregada por segundo em espera
	keepaliveIntv   = 10 * time.Second
)

// ── Estado da base ────────────────────────────────────────────────────────────

type Base struct {
	id      string
	setorID string

	// Endereços dos brokers, em ordem de prioridade (fallback)
	brokerAddrs []string

	// Drones gerenciados por esta base
	dronesMu sync.RWMutex
	drones   map[string]*DroneLocal

	// Conexão TCP ativa com o broker
	connMu  sync.Mutex
	conn    net.Conn
	encoder *json.Encoder

	logger *log.Logger
}

// DroneLocal representa o estado de um drone no processo da base.
type DroneLocal struct {
	mu           sync.Mutex
	info         models.InfoDrone
	ocupado      bool
}

func novaBase(id, setorID string, brokerAddrs []string, numDrones int) *Base {
	b := &Base{
		id:          id,
		setorID:     setorID,
		brokerAddrs: brokerAddrs,
		drones:      make(map[string]*DroneLocal),
		logger:      log.New(os.Stdout, fmt.Sprintf("[BASE:%s] ", id), log.LstdFlags),
	}

	// Cria drones com bateria inicial aleatória entre 70-100%
	for i := 1; i <= numDrones; i++ {
		droneID := fmt.Sprintf("drone_%s_%02d", id, i)
		d := &DroneLocal{
			info: models.InfoDrone{
				DroneID:   droneID,
				BaseID:    id,
				Estado:    models.DroneDisponivel,
				Bateria:   70 + rand.Intn(31),
				UltimaVez: time.Now(),
			},
		}
		b.drones[droneID] = d
		b.logger.Printf("Drone registrado: %s (bateria=%d%%)", droneID, d.info.Bateria)
	}
	return b
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	id       := flag.String("id",      "",              "ID da base (ex: Base_Norte)")
	setor    := flag.String("setor",   "",              "ID do setor (ex: Setor_Norte)")
	brokers  := flag.String("brokers", "localhost:6000","Endereços TCP dos brokers, em ordem de prioridade (vírgula)")
	ndrones  := flag.Int("drones",     3,               "Número de drones nesta base")
	flag.Parse()

	if *id == "" || *setor == "" {
		fmt.Fprintln(os.Stderr, "Uso: base -id Base_Norte -setor Setor_Norte -brokers IP1:6000,IP2:6000 [-drones 3]")
		os.Exit(1)
	}

	addrs := splitCSV(*brokers)
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "Informe pelo menos um endereço de broker em -brokers")
		os.Exit(1)
	}

	base := novaBase(*id, *setor, addrs, *ndrones)

	// Goroutine de recarga de bateria em background
	go base.loopRecarga()

	// Loop principal: conecta ao broker (com fallback) e processa comandos
	base.loopConexao()
}

// ── Conexão com broker (fallback automático) ──────────────────────────────────

func (b *Base) loopConexao() {
	backoff := 2 * time.Second
	idx := 0 // índice do broker atual na lista de fallback

	for {
		addr := b.brokerAddrs[idx%len(b.brokerAddrs)]
		b.logger.Printf("Conectando ao broker %s...", addr)

		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			b.logger.Printf("Falha ao conectar em %s: %v", addr, err)
			idx++ // tenta próximo broker da lista
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}

		b.logger.Printf("Conectada ao broker %s", addr)
		backoff = 2 * time.Second // reseta backoff após sucesso

		b.connMu.Lock()
		b.conn = conn
		b.encoder = json.NewEncoder(conn)
		b.connMu.Unlock()

		// Envia registro com lista completa de drones
		if err := b.enviarRegistro(); err != nil {
			b.logger.Printf("Erro ao registrar: %v", err)
			conn.Close()
			idx++
			continue
		}

		// Keepalive em background
		stopKA := make(chan struct{})
		go b.loopKeepalive(conn, stopKA)

		// Lê e processa comandos do broker
		b.processarComandos(conn)

		close(stopKA)
		conn.Close()

		b.connMu.Lock()
		b.conn = nil
		b.encoder = nil
		b.connMu.Unlock()

		b.logger.Printf("Conexão com broker encerrada — tentando próximo...")
		idx++ // tenta próximo broker na reconexão
		time.Sleep(backoff)
	}
}

func (b *Base) enviarRegistro() error {
	b.dronesMu.RLock()
	drones := make([]models.InfoDrone, 0, len(b.drones))
	for _, d := range b.drones {
		d.mu.Lock()
		drones = append(drones, d.info)
		d.mu.Unlock()
	}
	b.dronesMu.RUnlock()

	msg := models.MensagemBase{
		Tipo:      models.BaseRegistro,
		BaseID:    b.id,
		SetorID:   b.setorID,
		Drones:    drones,
		Timestamp: time.Now(),
	}
	b.connMu.Lock()
	defer b.connMu.Unlock()
	if b.encoder == nil {
		return fmt.Errorf("sem conexão")
	}
	return b.encoder.Encode(msg)
}

func (b *Base) loopKeepalive(conn net.Conn, stop <-chan struct{}) {
	ticker := time.NewTicker(keepaliveIntv)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Envia status atualizado dos drones como keepalive
			b.dronesMu.RLock()
			drones := make([]models.InfoDrone, 0, len(b.drones))
			for _, d := range b.drones {
				d.mu.Lock()
				drones = append(drones, d.info)
				d.mu.Unlock()
			}
			b.dronesMu.RUnlock()

			msg := models.MensagemBase{
				Tipo:      models.BaseStatusDrones,
				BaseID:    b.id,
				SetorID:   b.setorID,
				Drones:    drones,
				Timestamp: time.Now(),
			}
			conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if err := json.NewEncoder(conn).Encode(msg); err != nil {
				return
			}
		case <-stop:
			return
		}
	}
}

// ── Processamento de comandos do broker ───────────────────────────────────────

func (b *Base) processarComandos(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var cmd models.ComandoBase
		if err := json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
			b.logger.Printf("Comando inválido: %v", err)
			continue
		}
		go b.executarComando(cmd)
	}
}

func (b *Base) executarComando(cmd models.ComandoBase) {
	switch cmd.Tipo {

	case models.CmdDespacharDrone:
		b.despacharDrone(cmd.DroneID, cmd.OcorrenciaID, cmd.SetorDestino)

	case models.CmdRetornarDrone:
		b.retornarDrone(cmd.DroneID)
	}
}

// ── Lógica dos drones ─────────────────────────────────────────────────────────

func (b *Base) despacharDrone(droneID, ocorrenciaID, setorDestino string) {
	b.dronesMu.RLock()
	d, ok := b.drones[droneID]
	b.dronesMu.RUnlock()

	if !ok {
		b.logger.Printf("Drone %s não encontrado nesta base", droneID)
		return
	}

	d.mu.Lock()
	if !d.info.Disponivel() {
		d.mu.Unlock()
		b.logger.Printf("Drone %s indisponível (estado=%s bateria=%d%%)", droneID, d.info.Estado, d.info.Bateria)
		return
	}
	d.info.Estado = models.DroneDespachado
	d.info.OcorrenciaID = ocorrenciaID
	d.info.UltimaVez = time.Now()
	d.ocupado = true
	d.mu.Unlock()

	b.logger.Printf("Drone %s despachado → %s (ocorrência: %s)", droneID, setorDestino, ocorrenciaID)
	b.notificarEstado(droneID, ocorrenciaID, models.DroneDespachado)

	// Simula a missão em goroutine
	go b.simularMissao(d, ocorrenciaID)
}

func (b *Base) simularMissao(d *DroneLocal, ocorrenciaID string) {
	// Fase 1: deslocamento até o local (aleatório entre 2-5s)
	time.Sleep(2*time.Second + time.Duration(rand.Intn(3))*time.Second)

	d.mu.Lock()
	// Chance de 5% de o drone ser abatido durante a missão
	if rand.Float32() < 0.05 {
		d.info.Estado = models.DroneAbatido
		d.info.UltimaVez = time.Now()
		d.ocupado = false
		d.mu.Unlock()
		b.logger.Printf("Drone %s ABATIDO durante missão %s", d.info.DroneID, ocorrenciaID)
		b.notificarEstado(d.info.DroneID, ocorrenciaID, models.DroneAbatido)
		return
	}
	d.info.Estado = models.DroneEmMissao
	d.info.UltimaVez = time.Now()
	d.mu.Unlock()
	b.notificarEstado(d.info.DroneID, ocorrenciaID, models.DroneEmMissao)

	// Fase 2: execução da missão
	duracao := missaoDurMin + time.Duration(rand.Int63n(int64(missaoDurMax-missaoDurMin)))
	b.logger.Printf("Drone %s em missão (%s)...", d.info.DroneID, duracao.Round(time.Second))
	time.Sleep(duracao)

	// Fase 3: retorno
	d.mu.Lock()
	d.info.Estado = models.DroneRetornando
	d.info.Bateria -= bateriaConsumo
	if d.info.Bateria < 0 {
		d.info.Bateria = 0
	}
	d.info.UltimaVez = time.Now()
	d.mu.Unlock()
	b.notificarEstado(d.info.DroneID, ocorrenciaID, models.DroneRetornando)

	b.logger.Printf("Drone %s retornando (bateria=%d%%)...", d.info.DroneID, d.info.Bateria)
	time.Sleep(retornoDur)

	// Fase 4: disponível novamente (ou sem bateria)
	d.mu.Lock()
	if d.info.Bateria <= 10 {
		d.info.Estado = models.DroneSemBateria
		d.ocupado = false
		d.mu.Unlock()
		b.logger.Printf("Drone %s SEM BATERIA após missão", d.info.DroneID)
		b.notificarEstado(d.info.DroneID, "", models.DroneSemBateria)
		return
	}
	d.info.Estado = models.DroneDisponivel
	d.info.OcorrenciaID = ""
	d.ocupado = false
	d.info.UltimaVez = time.Now()
	d.mu.Unlock()

	b.logger.Printf("Drone %s disponível (bateria=%d%%)", d.info.DroneID, d.info.Bateria)
	b.notificarEstado(d.info.DroneID, "", models.DroneDisponivel)
}

func (b *Base) retornarDrone(droneID string) {
	b.dronesMu.RLock()
	d, ok := b.drones[droneID]
	b.dronesMu.RUnlock()
	if !ok {
		return
	}
	d.mu.Lock()
	if d.info.Estado == models.DroneEmMissao || d.info.Estado == models.DroneDespachado {
		d.info.Estado = models.DroneRetornando
		d.info.UltimaVez = time.Now()
		ocID := d.info.OcorrenciaID
		d.mu.Unlock()
		b.notificarEstado(droneID, ocID, models.DroneRetornando)
		b.logger.Printf("Drone %s retornando por ordem do broker", droneID)
	} else {
		d.mu.Unlock()
	}
}

// ── Notificação de estado ao broker ──────────────────────────────────────────

func (b *Base) notificarEstado(droneID, ocorrenciaID string, estado models.EstadoDrone) {
	msg := models.MensagemBase{
		Tipo:         models.BaseDroneEstado,
		BaseID:       b.id,
		SetorID:      b.setorID,
		DroneID:      droneID,
		NovoEstado:   estado,
		OcorrenciaID: ocorrenciaID,
		Timestamp:    time.Now(),
	}
	b.connMu.Lock()
	defer b.connMu.Unlock()
	if b.encoder == nil {
		return
	}
	b.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := b.encoder.Encode(msg); err != nil {
		b.logger.Printf("Erro ao notificar estado de %s: %v", droneID, err)
	}
}

// ── Recarga de bateria ────────────────────────────────────────────────────────

func (b *Base) loopRecarga() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		b.dronesMu.RLock()
		for _, d := range b.drones {
			d.mu.Lock()
			// Só recarrega drones parados na base
			if d.info.Estado == models.DroneDisponivel || d.info.Estado == models.DroneSemBateria {
				if d.info.Bateria < 100 {
					d.info.Bateria += bateriaRecarga
					if d.info.Bateria > 100 {
						d.info.Bateria = 100
					}
				}
				// Reativa drone sem bateria quando recarregar o suficiente
				if d.info.Estado == models.DroneSemBateria && d.info.Bateria > 30 {
					d.info.Estado = models.DroneDisponivel
					d.info.UltimaVez = time.Now()
					b.logger.Printf("Drone %s recarregado e disponível (bateria=%d%%)", d.info.DroneID, d.info.Bateria)
					// Notifica broker fora do lock
					go b.notificarEstado(d.info.DroneID, "", models.DroneDisponivel)
				}
			}
			d.mu.Unlock()
		}
		b.dronesMu.RUnlock()
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