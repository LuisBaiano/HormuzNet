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

// ── Constantes ────────────────────────────────────────────────────────────────

const (
	missaoDurMin   = 8 * time.Second
	missaoDurMax   = 20 * time.Second
	retornoDur     = 5 * time.Second
	bateriaConsumo = 5
	bateriaRecarga = 2
	keepaliveIntv  = 10 * time.Second
	droneKAIntv    = 3 * time.Second // intervalo do sinal "estou vivo" do drone
)

// ── Base ──────────────────────────────────────────────────────────────────────

type Base struct {
	id          string
	setorID     string
	posicao     models.Coordenada
	brokerAddrs []string

	dronesMu sync.RWMutex
	drones   map[string]*DroneLocal

	connMu  sync.Mutex
	conn    net.Conn
	encoder *json.Encoder

	logger *log.Logger
}

type DroneLocal struct {
	mu      sync.Mutex
	info    models.InfoDrone
	ocupado bool
}

func novaBase(id, setorID string, pos models.Coordenada, addrs []string, numDrones int) *Base {
	b := &Base{
		id:          id,
		setorID:     setorID,
		posicao:     pos,
		brokerAddrs: addrs,
		drones:      make(map[string]*DroneLocal),
		logger:      log.New(os.Stdout, fmt.Sprintf("[BASE:%s] ", id), log.LstdFlags),
	}
	for i := 1; i <= numDrones; i++ {
		droneID := fmt.Sprintf("drone_%s_%02d", id, i)
		// Posição inicial: próxima à base com pequena variação
		dx := (rand.Float64() - 0.5) * 10
		dy := (rand.Float64() - 0.5) * 10
		d := &DroneLocal{
			info: models.InfoDrone{
				DroneID:          droneID,
				BaseID:           id,
				Estado:           models.DroneDisponivel,
				Bateria:          70 + rand.Intn(31),
				Posicao:          models.Coordenada{X: pos.X + dx, Y: pos.Y + dy},
				UltimaVez:        time.Now(),
				DisponiveisDesde: time.Now(),
			},
		}
		b.drones[droneID] = d
		b.logger.Printf("Drone %s criado (bateria=%d%% pos=(%.1f,%.1f))",
			droneID, d.info.Bateria, d.info.Posicao.X, d.info.Posicao.Y)
	}
	return b
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	id      := flag.String("id",      "",              "ID da base (ex: Base_Norte)")
	setor   := flag.String("setor",   "",              "ID do setor")
	brokers := flag.String("brokers", "localhost:6000","Endereços TCP dos brokers (vírgula, ordem de prioridade)")
	ndrones := flag.Int("drones",     3,               "Número de drones iniciais")
	posX    := flag.Float64("x",      0,               "Posição X da base")
	posY    := flag.Float64("y",      0,               "Posição Y da base")
	flag.Parse()

	if *id == "" || *setor == "" {
		fmt.Fprintln(os.Stderr, "Uso: base -id Base_Norte -setor Setor_Norte -brokers IP:6000,IP:6000 [-drones 3] [-x 100] [-y 200]")
		os.Exit(1)
	}

	addrs := splitCSV(*brokers)
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "Informe pelo menos um endereço de broker")
		os.Exit(1)
	}

	pos := models.Coordenada{X: *posX, Y: *posY}
	base := novaBase(*id, *setor, pos, addrs, *ndrones)

	go base.loopRecarga()
	go base.loopDroneKeepalive()
	base.loopConexao()
}

// ── Conexão com broker (fallback automático) ──────────────────────────────────

func (b *Base) loopConexao() {
	backoff := 2 * time.Second
	idx := 0

	for {
		addr := b.brokerAddrs[idx%len(b.brokerAddrs)]
		b.logger.Printf("Conectando ao broker %s...", addr)

		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			b.logger.Printf("Falha em %s: %v — próximo em %s", addr, err, backoff)
			idx++
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}

		b.logger.Printf("Conectada ao broker %s", addr)
		backoff = 2 * time.Second

		b.connMu.Lock()
		b.conn = conn
		b.encoder = json.NewEncoder(conn)
		b.connMu.Unlock()

		if err := b.enviarRegistro(); err != nil {
			b.logger.Printf("Erro ao registrar: %v", err)
			conn.Close()
			idx++
			continue
		}

		stopKA := make(chan struct{})
		go b.loopKeepalive(conn, stopKA)
		b.processarComandos(conn)
		close(stopKA)

		conn.Close()
		b.connMu.Lock()
		b.conn = nil
		b.encoder = nil
		b.connMu.Unlock()

		b.logger.Printf("Conexão encerrada — tentando próximo broker...")
		idx++
		time.Sleep(backoff)
	}
}

func (b *Base) enviarRegistro() error {
	b.dronesMu.RLock()
	drones := b.listaDrones()
	b.dronesMu.RUnlock()

	msg := models.MensagemBase{
		Tipo:      models.BaseRegistro,
		BaseID:    b.id,
		SetorID:   b.setorID,
		Posicao:   b.posicao,
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
			b.dronesMu.RLock()
			drones := b.listaDrones()
			b.dronesMu.RUnlock()
			msg := models.MensagemBase{
				Tipo:      models.BaseStatusDrones,
				BaseID:    b.id,
				SetorID:   b.setorID,
				Posicao:   b.posicao,
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

// ── Comandos do broker ────────────────────────────────────────────────────────

func (b *Base) processarComandos(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var cmd models.ComandoBase
		if err := json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
			continue
		}
		go b.executarComando(cmd)
	}
}

func (b *Base) executarComando(cmd models.ComandoBase) {
	switch cmd.Tipo {
	case models.CmdDespacharDrone:
		b.despacharDrone(cmd.DroneID, cmd.OcorrenciaID, cmd.SetorDestino, cmd.PosicaoAlvo)
	case models.CmdRetornarDrone:
		b.retornarDrone(cmd.DroneID)
	case models.CmdReceberDrones:
		b.absorverDrones(cmd.DronesParaAbsorver)
	}
}

// ── Absorção de drones realocados ─────────────────────────────────────────────

func (b *Base) absorverDrones(drones []models.InfoDrone) {
	b.dronesMu.Lock()
	for _, d := range drones {
		d.BaseID = b.id
		d.Estado = models.DroneDisponivel
		d.UltimaVez = time.Now()
		d.DisponiveisDesde = time.Now()
		d.Posicao = models.Coordenada{
			X: b.posicao.X + (rand.Float64()-0.5)*10,
			Y: b.posicao.Y + (rand.Float64()-0.5)*10,
		}
		b.drones[d.DroneID] = &DroneLocal{info: d}
		b.logger.Printf("Drone %s absorvido por realocação (bateria=%d%%)", d.DroneID, d.Bateria)
	}
	b.dronesMu.Unlock()

	// Notifica o broker do novo estado de cada drone absorvido
	for _, d := range drones {
		b.notificarEstado(d.DroneID, "", models.DroneDisponivel)
	}
}

// ── Lógica dos drones ─────────────────────────────────────────────────────────

func (b *Base) despacharDrone(droneID, ocorrenciaID, setorDestino string, alvo models.Coordenada) {
	b.dronesMu.RLock()
	d, ok := b.drones[droneID]
	b.dronesMu.RUnlock()

	if !ok {
		b.logger.Printf("Drone %s não encontrado", droneID)
		return
	}

	d.mu.Lock()
	if !d.info.Disponivel() {
		d.mu.Unlock()
		b.logger.Printf("Drone %s indisponível (estado=%s bateria=%d%%)",
			droneID, d.info.Estado, d.info.Bateria)
		return
	}
	d.info.Estado = models.DroneDespachado
	d.info.OcorrenciaID = ocorrenciaID
	d.info.UltimaVez = time.Now()
	d.info.DisponiveisDesde = time.Time{}
	d.ocupado = true
	d.mu.Unlock()

	b.logger.Printf("Drone %s despachado → %s (ocorrência: %s alvo=(%.0f,%.0f))",
		droneID, setorDestino, ocorrenciaID, alvo.X, alvo.Y)
	b.notificarEstado(droneID, ocorrenciaID, models.DroneDespachado)

	go b.simularMissao(d, ocorrenciaID, alvo)
}

func (b *Base) simularMissao(d *DroneLocal, ocorrenciaID string, alvo models.Coordenada) {
	// Deslocamento até o local
	time.Sleep(2*time.Second + time.Duration(rand.Intn(3))*time.Second)

	d.mu.Lock()
	// 5% de chance de ser abatido
	if rand.Float32() < 0.05 {
		d.info.Estado = models.DroneAbatido
		d.info.UltimaVez = time.Now()
		d.ocupado = false
		d.mu.Unlock()
		b.logger.Printf("Drone %s ABATIDO durante missão %s", d.info.DroneID, ocorrenciaID)
		b.notificarEstado(d.info.DroneID, ocorrenciaID, models.DroneAbatido)
		return
	}
	// Move posição para o alvo
	d.info.Posicao = alvo
	d.info.Estado = models.DroneEmMissao
	d.info.UltimaVez = time.Now()
	d.mu.Unlock()
	b.notificarEstado(d.info.DroneID, ocorrenciaID, models.DroneEmMissao)

	// Executa a missão
	duracao := missaoDurMin + time.Duration(rand.Int63n(int64(missaoDurMax-missaoDurMin)))
	b.logger.Printf("Drone %s em missão (%s)...", d.info.DroneID, duracao.Round(time.Second))
	time.Sleep(duracao)

	// Retorno à base
	d.mu.Lock()
	d.info.Estado = models.DroneRetornando
	d.info.Bateria -= bateriaConsumo
	if d.info.Bateria < 0 {
		d.info.Bateria = 0
	}
	d.info.UltimaVez = time.Now()
	ocID := d.info.OcorrenciaID
	d.mu.Unlock()
	b.notificarEstado(d.info.DroneID, ocID, models.DroneRetornando)

	b.logger.Printf("Drone %s retornando (bateria=%d%%)...", d.info.DroneID, d.info.Bateria)
	time.Sleep(retornoDur)

	// Move posição de volta à base
	d.mu.Lock()
	d.info.Posicao = models.Coordenada{
		X: b.posicao.X + (rand.Float64()-0.5)*10,
		Y: b.posicao.Y + (rand.Float64()-0.5)*10,
	}
	if d.info.Bateria <= 10 {
		d.info.Estado = models.DroneSemBateria
		d.ocupado = false
		d.mu.Unlock()
		b.logger.Printf("Drone %s SEM BATERIA", d.info.DroneID)
		b.notificarEstado(d.info.DroneID, "", models.DroneSemBateria)
		return
	}
	d.info.Estado = models.DroneDisponivel
	d.info.OcorrenciaID = ""
	d.info.DisponiveisDesde = time.Now()
	d.ocupado = false
	d.info.UltimaVez = time.Now()
	d.mu.Unlock()

	b.logger.Printf("Drone %s disponível (bateria=%d%%)", d.info.DroneID, d.info.Bateria)
	b.notificarEstado(d.info.DroneID, ocID, models.DroneDisponivel)
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
		ocID := d.info.OcorrenciaID
		d.info.Estado = models.DroneRetornando
		d.info.UltimaVez = time.Now()
		d.mu.Unlock()
		b.notificarEstado(droneID, ocID, models.DroneRetornando)
		b.logger.Printf("Drone %s retornando por ordem do broker", droneID)
	} else {
		d.mu.Unlock()
	}
}

// ── Sinal de vida do drone (keepalive individual) ─────────────────────────────

func (b *Base) loopDroneKeepalive() {
	ticker := time.NewTicker(droneKAIntv)
	defer ticker.Stop()
	for range ticker.C {
		b.dronesMu.RLock()
		for _, d := range b.drones {
			d.mu.Lock()
			d.info.UltimaVez = time.Now()
			d.mu.Unlock()
		}
		b.dronesMu.RUnlock()
		// O status geral já é enviado pelo loopKeepalive da base
		// Este loop apenas mantém UltimaVez atualizado internamente
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
			if d.info.Estado == models.DroneDisponivel || d.info.Estado == models.DroneSemBateria {
				if d.info.Bateria < 100 {
					d.info.Bateria += bateriaRecarga
					if d.info.Bateria > 100 {
						d.info.Bateria = 100
					}
				}
				if d.info.Estado == models.DroneSemBateria && d.info.Bateria > 30 {
					d.info.Estado = models.DroneDisponivel
					d.info.DisponiveisDesde = time.Now()
					d.info.UltimaVez = time.Now()
					b.logger.Printf("Drone %s recarregado (bateria=%d%%)", d.info.DroneID, d.info.Bateria)
					go b.notificarEstado(d.info.DroneID, "", models.DroneDisponivel)
				}
			}
			d.mu.Unlock()
		}
		b.dronesMu.RUnlock()
	}
}

// ── Notificação de estado ─────────────────────────────────────────────────────

func (b *Base) notificarEstado(droneID, ocorrenciaID string, estado models.EstadoDrone) {
	b.dronesMu.RLock()
	var pos models.Coordenada
	if d, ok := b.drones[droneID]; ok {
		d.mu.Lock()
		pos = d.info.Posicao
		d.mu.Unlock()
	}
	b.dronesMu.RUnlock()

	msg := models.MensagemBase{
		Tipo:         models.BaseDroneEstado,
		BaseID:       b.id,
		SetorID:      b.setorID,
		DroneID:      droneID,
		NovoEstado:   estado,
		OcorrenciaID: ocorrenciaID,
		Posicao2:     pos,
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

// ── Utilitários ───────────────────────────────────────────────────────────────

func (b *Base) listaDrones() []models.InfoDrone {
	lista := make([]models.InfoDrone, 0, len(b.drones))
	for _, d := range b.drones {
		d.mu.Lock()
		lista = append(lista, d.info)
		d.mu.Unlock()
	}
	return lista
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
