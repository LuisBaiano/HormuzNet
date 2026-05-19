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
	"strings"
	"sync"
	"time"

	"HormuzNet/internal/models"
)

const (
	missaoDurMin   = 4 * time.Second
	missaoDurMax   = 8 * time.Second
	retornoDur     = 3 * time.Second
	keepaliveIntv  = 3 * time.Second
)

type Drone struct {
	id          string
	brokerAddrs []string

	mu      sync.Mutex
	info    models.InfoDrone
	ocupado bool

	connMu  sync.Mutex
	conn    net.Conn
	encoder *json.Encoder

	logger *log.Logger
}

func novoDrone(id string, addrs []string, posX, posY float64) *Drone {
	rand.Seed(time.Now().UnixNano())
	return &Drone{
		id:          id,
		brokerAddrs: addrs,
		info: models.InfoDrone{
			DroneID:          id,
			Estado:           models.DroneDisponivel,
			Posicao:          models.Coordenada{X: posX, Y: posY},
			UltimaVez:        time.Now(),
			DisponiveisDesde: time.Now(),
		},
		logger: log.New(os.Stdout, fmt.Sprintf("[DRONE:%s] ", id), log.LstdFlags),
	}
}

func main() {
	id := flag.String("id", "", "ID do drone (ex: drone_01)")
	brokers := flag.String("brokers", "localhost:6000", "Endereços TCP dos brokers (vírgula, ordem de prioridade para fallback)")
	posX := flag.Float64("x", 0, "Posição X inicial")
	posY := flag.Float64("y", 0, "Posição Y inicial")
	flag.Parse()

	if *id == "" {
		fmt.Fprintln(os.Stderr, "Uso: drone -id drone_01 -brokers IP:6000,IP:6001 -x 100 -y 200")
		os.Exit(1)
	}

	addrs := strings.Split(*brokers, ",")
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "Informe pelo menos um endereço de broker")
		os.Exit(1)
	}

	d := novoDrone(*id, addrs, *posX, *posY)

	d.loopConexao() // Bloqueante
}

// ── Conexão com Broker (Fallback Automático) ──────────────────────────────────

func (d *Drone) loopConexao() {
	backoff := 2 * time.Second
	idx := 0

	for {
		addr := d.brokerAddrs[idx%len(d.brokerAddrs)]
		d.logger.Printf("Conectando ao broker %s...", addr)

		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			d.logger.Printf("Falha ao conectar em %s: %v — tentando próximo em %s", addr, err, backoff)
			idx++
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}

		d.logger.Printf("Conectado ao broker %s", addr)
		backoff = 2 * time.Second

		d.connMu.Lock()
		d.conn = conn
		d.encoder = json.NewEncoder(conn)
		d.connMu.Unlock()

		d.mu.Lock()
		d.info.BrokerID = addr
		d.mu.Unlock()

		if err := d.enviarRegistro(); err != nil {
			d.logger.Printf("Erro ao registrar: %v", err)
			conn.Close()
			idx++
			continue
		}

		stopKA := make(chan struct{})
		go d.loopKeepalive(stopKA)
		d.processarComandos(conn)
		close(stopKA)

		conn.Close()
		d.connMu.Lock()
		d.conn = nil
		d.encoder = nil
		d.connMu.Unlock()

		d.logger.Printf("Conexão com %s perdida. Iniciando fallback para o próximo broker...", addr)
		idx++
		time.Sleep(1 * time.Second)
	}
}

func (d *Drone) enviarRegistro() error {
	d.mu.Lock()
	infoCopia := d.info
	d.mu.Unlock()

	msg := models.MensagemDrone{
		Tipo:      models.DroneRegistro,
		DroneID:   d.id,
		Timestamp: time.Now(),
		DroneInfo: &infoCopia,
	}

	d.connMu.Lock()
	defer d.connMu.Unlock()
	if d.encoder == nil {
		return fmt.Errorf("sem conexão")
	}
	d.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	return d.encoder.Encode(msg)
}

func (d *Drone) loopKeepalive(stop <-chan struct{}) {
	ticker := time.NewTicker(keepaliveIntv)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.mu.Lock()
			infoCopia := d.info
			d.mu.Unlock()

			msg := models.MensagemDrone{
				Tipo:      models.DroneKeepalive,
				DroneID:   d.id,
				Timestamp: time.Now(),
				DroneInfo: &infoCopia,
			}
			d.connMu.Lock()
			if d.encoder != nil {
				d.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if err := d.encoder.Encode(msg); err != nil {
					d.logger.Printf("Erro ao enviar keepalive: %v. Fechando conexão.", err)
					d.conn.Close()
				} else {
					d.logger.Printf("[KEEPALIVE ENVIADO] Drone envia keepalive, posicao=(%.0f, %.0f)", infoCopia.Posicao.X, infoCopia.Posicao.Y)
				}
			}
			d.connMu.Unlock()
		case <-stop:
			return
		}
	}
}

// ── Recebimento de Comandos ───────────────────────────────────────────────────

func (d *Drone) processarComandos(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var cmd models.ComandoDrone
		if err := json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
			continue
		}
		go d.executarComando(cmd)
	}
}

func (d *Drone) executarComando(cmd models.ComandoDrone) {
	d.logger.Printf("[COMANDO RECEBIDO] Broker ordena ação tipo=%s, ocorrencia=%s, alvo=(%.0f, %.0f)", cmd.Tipo, cmd.OcorrenciaID, cmd.PosicaoAlvo.X, cmd.PosicaoAlvo.Y)
	switch cmd.Tipo {
	case models.CmdDespacharDrone:
		d.iniciarMissao(cmd.OcorrenciaID, cmd.PosicaoAlvo)
	case models.CmdRetornarDrone:
		d.abortarMissao()
	}
}

// ── Lógica de Missão ──────────────────────────────────────────────────────────

func (d *Drone) iniciarMissao(ocorrenciaID string, alvo models.Coordenada) {
	d.mu.Lock()
	if !d.info.Disponivel() {
		d.mu.Unlock()
		d.logger.Printf("Comando ignorado: indisponível (estado=%s)", d.info.Estado)
		return
	}
	d.info.Estado = models.DroneDespachado
	d.info.OcorrenciaID = ocorrenciaID
	d.info.UltimaVez = time.Now()
	d.info.DisponiveisDesde = time.Time{}
	d.ocupado = true
	posInicial := d.info.Posicao
	d.mu.Unlock()

	d.logger.Printf("Despachado para %s (Alvo: %.0f, %.0f)", ocorrenciaID, alvo.X, alvo.Y)
	d.notificarEstado(models.DroneDespachado, ocorrenciaID, posInicial)

	// Tempo de deslocamento
	time.Sleep(2*time.Second + time.Duration(rand.Intn(3))*time.Second)

	d.mu.Lock()
	if d.info.Estado != models.DroneDespachado {
		d.mu.Unlock()
		return // Missão abortada no meio do caminho
	}

	// 10% de chance de ser abatido/perdido (simulação realista)
	if rand.Float32() < 0.10 {
		d.info.Estado = models.DroneAbatido
		d.ocupado = false
		d.mu.Unlock()
		d.logger.Printf("CRÍTICO: Fui ABATIDO ou PERDIDO a caminho da missão %s", ocorrenciaID)
		d.notificarEstado(models.DroneAbatido, ocorrenciaID, alvo)
		// Processo morre para simular perda de recurso físico
		os.Exit(1)
	}

	d.info.Posicao = alvo // Chegou ao alvo
	d.info.Estado = models.DroneEmMissao
	d.mu.Unlock()
	d.notificarEstado(models.DroneEmMissao, ocorrenciaID, alvo)

	duracao := missaoDurMin + time.Duration(rand.Int63n(int64(missaoDurMax-missaoDurMin)))
	d.logger.Printf("Em missão (%s)...", duracao.Round(time.Second))
	time.Sleep(duracao)

	// Fim da missão, inicia retorno ao "ponto de patrulha" anterior
	d.mu.Lock()
	if d.info.Estado != models.DroneEmMissao {
		d.mu.Unlock()
		return
	}
	d.info.Estado = models.DroneRetornando
	d.mu.Unlock()
	d.notificarEstado(models.DroneRetornando, ocorrenciaID, alvo)

	d.logger.Printf("Missão concluída. Retornando...")
	time.Sleep(retornoDur)

	d.mu.Lock()
	d.info.Posicao = posInicial // Voltou ao posto

	d.info.Estado = models.DroneDisponivel
	d.info.OcorrenciaID = ""
	d.info.DisponiveisDesde = time.Now()
	d.ocupado = false
	d.mu.Unlock()

	d.logger.Printf("Pronto e Disponível")
	d.notificarEstado(models.DroneDisponivel, ocorrenciaID, posInicial)
}

func (d *Drone) abortarMissao() {
	d.mu.Lock()
	if d.info.Estado == models.DroneEmMissao || d.info.Estado == models.DroneDespachado {
		ocID := d.info.OcorrenciaID
		d.info.Estado = models.DroneRetornando
		pos := d.info.Posicao
		d.mu.Unlock()
		d.logger.Printf("Abortando missão %s por ordem do Broker.", ocID)
		d.notificarEstado(models.DroneRetornando, ocID, pos)
	} else {
		d.mu.Unlock()
	}
}

// ── Notificação de Estado ─────────────────────────────────────────────────────

func (d *Drone) notificarEstado(estado models.EstadoDrone, ocID string, pos models.Coordenada) {
	d.mu.Lock()
	d.mu.Unlock()

	msg := models.MensagemDrone{
		Tipo:         models.DroneEstado,
		DroneID:      d.id,
		NovoEstado:   estado,
		OcorrenciaID: ocID,
		Posicao:      pos,
		Timestamp:    time.Now(),
	}

	d.connMu.Lock()
	defer d.connMu.Unlock()
	if d.encoder != nil {
		d.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if err := d.encoder.Encode(msg); err == nil {
			d.logger.Printf("[ESTADO ENVIADO] Drone notifica estado=%s, ocorrencia=%s, posicao=(%.0f, %.0f)", estado, ocID, pos.X, pos.Y)
		}
	}
}
