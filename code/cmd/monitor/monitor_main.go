/*
Este arquivo implementa o Monitor Central do HormuzNet.
Ele serve como a central de controle e consolidação de visualização tática do Estreito.

Responsabilidades principais:
  - Conectar-se ao Broker Líder via TCP e descobrir automaticamente os demais
    Brokers da malha através do protocolo MsgPeerList (auto-discovery)
  - Coletar e de-duplicar eventos de todos os Brokers (status de Drones,
    ocorrências, Failovers e missões concluídas)
  - Detectar Brokers inativos por timeout de heartbeat
  - Expor um servidor HTTP com WebSocket RFC 6455 (implementado do zero, sem libs
    externas) na porta 8085 para atualizar o dashboard em tempo real
  - Servir o dashboard HTML/CSS/JS embutido com atualização automática a cada 1s
*/
package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"HormuzNet/internal/models"
)

// ── WebSocket RFC 6455 ────────────────────────────────────────────────────────

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func wsAccept(k string) string {
	h := sha1.New()
	h.Write([]byte(k + wsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func wsUpgrade(w http.ResponseWriter, r *http.Request) (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("hijack indisponível")
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return nil, nil, err
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + wsAccept(key) + "\r\n\r\n"
	rw.WriteString(resp)
	rw.Flush()
	return conn, rw, nil
}

func wsFrame(payload []byte) []byte {
	l := len(payload)
	var h []byte
	h = append(h, 0x81)
	switch {
	case l <= 125:
		h = append(h, byte(l))
	case l <= 65535:
		h = append(h, 126, byte(l>>8), byte(l))
	default:
		h = append(h, 127, 0, 0, 0, 0, byte(l>>24), byte(l>>16), byte(l>>8), byte(l))
	}
	return append(h, payload...)
}

func wsLer(r io.Reader) ([]byte, error) {
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	masked := hdr[1]&0x80 != 0
	plen := int(hdr[1] & 0x7F)
	switch plen {
	case 126:
		ext := make([]byte, 2)
		io.ReadFull(r, ext)
		plen = int(ext[0])<<8 | int(ext[1])
	case 127:
		ext := make([]byte, 8)
		io.ReadFull(r, ext)
		plen = int(ext[4])<<24 | int(ext[5])<<16 | int(ext[6])<<8 | int(ext[7])
	}
	var mk [4]byte
	if masked {
		io.ReadFull(r, mk[:])
	}
	payload := make([]byte, plen)
	io.ReadFull(r, payload)
	if masked {
		for i := range payload {
			payload[i] ^= mk[i%4]
		}
	}
	return payload, nil
}

// ── Hub de clientes WebSocket ─────────────────────────────────────────────────

type wsClient struct {
	conn net.Conn
	mu   sync.Mutex
}

type Hub struct {
	mu      sync.RWMutex
	clients map[*wsClient]struct{}
}

var hub = &Hub{clients: make(map[*wsClient]struct{})}

func (h *Hub) add(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) remove(c *wsClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	c.conn.Close()
}

func (h *Hub) broadcast(data []byte) {
	frame := wsFrame(data)
	h.mu.RLock()
	for c := range h.clients {
		c := c
		go func() {
			c.mu.Lock()
			c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			c.conn.Write(frame)
			c.mu.Unlock()
		}()
	}
	h.mu.RUnlock()
}

// ── Estado global do monitor ──────────────────────────────────────────────────

type BrokerStatus struct {
	ID       string    `json:"id"`
	Addr     string    `json:"addr"`
	Vivo     bool      `json:"vivo"`
	UltimoHB time.Time `json:"ultimo_hb"`
}

type EventoLog struct {
	Timestamp time.Time `json:"timestamp"`
	Tipo      string    `json:"tipo"`
	Mensagem  string    `json:"mensagem"`
	Nivel     string    `json:"nivel"` // info | warn | danger
}

type OcorrenciaDetalhada struct {
	ID          string    `json:"id"`
	Tipo        string    `json:"tipo"`
	Criticidade string    `json:"criticidade"`
	Status      string    `json:"status"`
	Timestamp   time.Time `json:"timestamp"`
}

type EstadoGlobal struct {
	Drones      map[string]models.InfoDrone       `json:"drones"`
	Brokers     []BrokerStatus                    `json:"brokers"`
	Eventos     []EventoLog                       `json:"eventos"`
	Ocorrencias map[string]OcorrenciaDetalhada    `json:"ocorrencias"`
	Failovers   map[string]string                 `json:"failovers"`
}

var (
	estadoMu    sync.RWMutex
	drones      = make(map[string]models.InfoDrone)
	brokers     = make(map[string]*BrokerStatus)
	eventos     []EventoLog
	ocorrencias = make(map[string]OcorrenciaDetalhada)
	failovers   = make(map[string]string)
)

func obterBrokerID(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	switch port {
	case "6000":
		return "B1"
	case "6001":
		return "B2"
	case "6002":
		return "B3"
	case "6003":
		return "B4"
	case "6004":
		return "B5"
	case "6005":
		return "B6"
	case "6006":
		return "B7"
	case "6007":
		return "B8"
	case "6008":
		return "B9"
	default:
		return "B_" + port
	}
}

func obterSetorPorBrokerID(brokerID string) string {
	switch brokerID {
	case "B1":
		return "Setor_Noroeste"
	case "B2":
		return "Setor_Norte"
	case "B3":
		return "Setor_Nordeste"
	case "B4":
		return "Setor_Leste"
	case "B5":
		return "Setor_Sudeste"
	case "B6":
		return "Setor_Sul"
	case "B7":
		return "Setor_Sudoeste"
	case "B8":
		return "Setor_Oeste"
	case "B9":
		return "Setor_Centro"
	default:
		return ""
	}
}

func addEvento(tipo, msg, nivel string) {
	estadoMu.Lock()
	eventos = append(eventos, EventoLog{
		Timestamp: time.Now(),
		Tipo:      tipo,
		Mensagem:  msg,
		Nivel:     nivel,
	})
	if len(eventos) > 100 {
		eventos = eventos[len(eventos)-100:]
	}
	estadoMu.Unlock()
}

func snapshot() []byte {
	estadoMu.RLock()
	blist := make([]BrokerStatus, 0, len(brokers))
	for _, b := range brokers {
		blist = append(blist, *b)
	}
	ev := make([]EventoLog, len(eventos))
	copy(ev, eventos)
	d := make(map[string]models.InfoDrone, len(drones))
	for k, v := range drones {
		d[k] = v
	}
	o := make(map[string]OcorrenciaDetalhada, len(ocorrencias))
	for k, v := range ocorrencias {
		o[k] = v
	}
	fo := make(map[string]string, len(failovers))
	for k, v := range failovers {
		fo[k] = v
	}
	estadoMu.RUnlock()

	estado := EstadoGlobal{Drones: d, Brokers: blist, Eventos: ev, Ocorrencias: o, Failovers: fo}
	data, _ := json.Marshal(estado)
	return data
}

// ── Conexão com broker como observer ─────────────────────────────────────────

func conectarBroker(addr string) {
	backoff := 2 * time.Second
	for {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			estadoMu.Lock()
			if b, ok := brokers[addr]; ok {
				b.Vivo = false
			}
			estadoMu.Unlock()
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}

		backoff = 2 * time.Second
		log.Printf("[MONITOR] Conectado ao broker %s", addr)

		// Registra como peer broker (ID especial MONITOR-...)
		reg := models.MensagemBroker{
			Tipo:      models.MsgRegistro,
			BrokerID:  "MONITOR-" + addr,
			Timestamp: time.Now(),
		}
		json.NewEncoder(conn).Encode(reg)

		estadoMu.Lock()
		if b, ok := brokers[addr]; !ok {
			brokers[addr] = &BrokerStatus{ID: obterBrokerID(addr), Addr: addr, Vivo: true, UltimoHB: time.Now()}
		} else {
			if b.ID == "" {
				b.ID = obterBrokerID(addr)
			}
			b.Vivo = true
			b.UltimoHB = time.Now()
		}
		estadoMu.Unlock()

		addEvento("CONEXAO", fmt.Sprintf("Conectado ao broker %s", addr), "info")

		scanner := bufio.NewScanner(conn)
		for {
			conn.SetReadDeadline(time.Now().Add(15 * time.Second))
			if !scanner.Scan() {
				break
			}
			var msg models.MensagemBroker
			if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
				continue
			}
			processarMensagem(msg, addr)
		}

		conn.Close()
		estadoMu.Lock()
		if b, ok := brokers[addr]; ok {
			b.Vivo = false
		}
		estadoMu.Unlock()
		addEvento("FALHA", fmt.Sprintf("Broker %s desconectou", addr), "danger")
		log.Printf("[MONITOR] Broker %s desconectou — reconectando em %s", addr, backoff)
		time.Sleep(backoff)
		backoff *= 2
	}
}

func processarMensagem(msg models.MensagemBroker, addr string) {
	estadoMu.Lock()
	if b, ok := brokers[addr]; ok {
		b.UltimoHB = time.Now()
	}

	// Registra/atualiza o broker que originou a mensagem
	if msg.BrokerID != "" && !strings.HasPrefix(msg.BrokerID, "MONITOR-") {
		bID := msg.BrokerID
		var bStatus *BrokerStatus
		for _, b := range brokers {
			if b.ID == bID || b.Addr == bID {
				bStatus = b
				break
			}
		}
		if bStatus != nil {
			bStatus.Vivo = true
			bStatus.UltimoHB = time.Now()

			// Se o broker está vivo, garante que o failover do seu setor original seja removido
			setor := obterSetorPorBrokerID(bID)
			if setor != "" {
				if _, isFailoverActive := failovers[setor]; isFailoverActive {
					delete(failovers, setor)
					eventos = append(eventos, EventoLog{
						Timestamp: time.Now(),
						Tipo:      "RECUPERACAO",
						Mensagem:  fmt.Sprintf("Broker %s voltou e recuperou o setor %s", bID, setor),
						Nivel:     "info",
					})
					if len(eventos) > 100 {
						eventos = eventos[len(eventos)-100:]
					}
				}
			}
		}
	}
	estadoMu.Unlock()

	switch msg.Tipo {
	case models.MsgPeerList:
		for _, peer := range msg.Peers {
			estadoMu.Lock()
			_, ok := brokers[peer]
			if !ok {
				brokers[peer] = &BrokerStatus{
					ID:   obterBrokerID(peer),
					Addr: peer,
				}
				estadoMu.Unlock()
				log.Printf("[MONITOR] Novo broker descoberto via Líder: %s. Conectando...", peer)
				go conectarBroker(peer)
			} else {
				estadoMu.Unlock()
			}
		}

	case models.MsgSincDrone:
		if msg.Drone == nil {
			return
		}
		estadoMu.Lock()
		drones[msg.Drone.DroneID] = *msg.Drone
		estadoMu.Unlock()

	case models.MsgDroneDespachado:
		estadoMu.Lock()
		o, ok := ocorrencias[msg.OcorrenciaID]
		alreadyDespatched := ok && o.Status == "ANDAMENTO"
		if !alreadyDespatched {
			if !ok {
				o = OcorrenciaDetalhada{
					ID:     msg.OcorrenciaID,
					Status: "ANDAMENTO",
				}
			} else {
				o.Status = "ANDAMENTO"
			}
			ocorrencias[msg.OcorrenciaID] = o
		}
		estadoMu.Unlock()
		if !alreadyDespatched {
			addEvento("DESPACHO",
				fmt.Sprintf("Drone %s despachado para ocorrência %s (broker %s)",
					msg.DroneID, msg.OcorrenciaID, msg.BrokerID), "warn")
		}

	case models.MsgDronePerdido:
		estadoMu.Lock()
		d, ok := drones[msg.DroneID]
		alreadyAbatido := ok && d.Estado == models.DroneAbatido
		if !alreadyAbatido {
			if !ok {
				d = models.InfoDrone{
					DroneID: msg.DroneID,
					Estado:  models.DroneAbatido,
				}
			} else {
				d.Estado = models.DroneAbatido
			}
			drones[msg.DroneID] = d
		}
		estadoMu.Unlock()
		if !alreadyAbatido {
			addEvento("PERDA",
				fmt.Sprintf("Drone %s PERDIDO — %s", msg.DroneID, msg.Motivo), "danger")
		}

	case models.MsgDroneLiberado:
		estadoMu.Lock()
		d, ok := drones[msg.DroneID]
		alreadyAvailable := ok && d.Estado == models.DroneDisponivel
		if !alreadyAvailable {
			if !ok {
				d = models.InfoDrone{
					DroneID: msg.DroneID,
					Estado:  models.DroneDisponivel,
				}
			} else {
				d.Estado = models.DroneDisponivel
			}
			drones[msg.DroneID] = d
		}
		estadoMu.Unlock()
		if !alreadyAvailable {
			addEvento("LIBERADO", fmt.Sprintf("Drone %s disponível", msg.DroneID), "info")
		}

	case models.MsgMissaoConcluida:
		estadoMu.Lock()
		o, ok := ocorrencias[msg.OcorrenciaID]
		alreadyCompleted := ok && o.Status == "CONCLUIDA"
		if !alreadyCompleted {
			if !ok {
				o = OcorrenciaDetalhada{
					ID:     msg.OcorrenciaID,
					Status: "CONCLUIDA",
				}
			} else {
				o.Status = "CONCLUIDA"
			}
			ocorrencias[msg.OcorrenciaID] = o
		}
		estadoMu.Unlock()
		if !alreadyCompleted {
			addEvento("MISSAO",
				fmt.Sprintf("Missão concluída: drone %s liberou %s", msg.DroneID, msg.OcorrenciaID), "info")
		}

	case models.MsgRequisicaoDrone:
		if msg.Ocorrencia != nil {
			estadoMu.Lock()
			_, exists := ocorrencias[msg.Ocorrencia.ID]
			if !exists {
				ocorrencias[msg.Ocorrencia.ID] = OcorrenciaDetalhada{
					ID:          msg.Ocorrencia.ID,
					Tipo:        msg.Ocorrencia.Tipo,
					Criticidade: msg.Ocorrencia.Criticidade.String(),
					Status:      "ESPERA",
					Timestamp:   msg.Ocorrencia.Timestamp,
				}
			}
			estadoMu.Unlock()
			if !exists {
				addEvento("REQUISICAO",
					fmt.Sprintf("Ocorrência %s [%s] em %s", msg.Ocorrencia.ID,
						msg.Ocorrencia.Criticidade, msg.Ocorrencia.SetorOrigem), "warn")
			}
		}

	case models.MsgFailover:
		estadoMu.Lock()
		prevBroker, alreadyFailover := failovers[msg.SetorID]
		isNewFailover := !alreadyFailover || prevBroker != msg.BrokerID
		if isNewFailover {
			failovers[msg.SetorID] = msg.BrokerID
		}
		estadoMu.Unlock()
		if isNewFailover {
			addEvento("FAILOVER", fmt.Sprintf("Broker %s assumiu setor %s (failover)", msg.BrokerID, msg.SetorID), "danger")
		}

	case models.MsgFailoverRecuperado:
		estadoMu.Lock()
		_, isFailoverActive := failovers[msg.SetorID]
		if isFailoverActive {
			delete(failovers, msg.SetorID)
		}
		estadoMu.Unlock()
		if isFailoverActive {
			addEvento("RECUPERACAO", fmt.Sprintf("Broker %s recuperou setor %s", msg.BrokerID, msg.SetorID), "info")
		}
	}
}

// ── HTTP + WebSocket ──────────────────────────────────────────────────────────

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, _, err := wsUpgrade(w, r)
	if err != nil {
		return
	}
	c := &wsClient{conn: conn}
	hub.add(c)
	defer hub.remove(c)

	// Envia estado inicial imediatamente
	hub.broadcast(snapshot())

	// Lê frames (ignora — monitor é só leitura)
	for {
		if _, err := wsLer(conn); err != nil {
			return
		}
	}
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	brokersFlag := flag.String("brokers", "localhost:6000", "Endereços TCP dos brokers (vírgula)")
	porta := flag.String("porta", "8085", "Porta HTTP do dashboard")
	flag.Parse()

	addrs := strings.Split(*brokersFlag, ",")
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		estadoMu.Lock()
		brokers[addr] = &BrokerStatus{ID: obterBrokerID(addr), Addr: addr, Vivo: false}
		estadoMu.Unlock()
		go conectarBroker(addr)
	}

	// Push de estado a cada 1s para todos os clientes WS
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			hub.broadcast(snapshot())
		}
	}()

	// Verifica brokers mortos por timeout de heartbeat
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			estadoMu.Lock()
			for _, b := range brokers {
				if b.Vivo && time.Since(b.UltimoHB) > 15*time.Second {
					b.Vivo = false
				}
			}
			estadoMu.Unlock()
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, dashboardHTML)
	})
	http.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir("./assets"))))
	http.HandleFunc("/ws", handleWS)
	http.HandleFunc("/api/estado", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(snapshot())
	})

	log.Printf("[MONITOR] Dashboard: http://localhost:%s", *porta)
	log.Printf("[MONITOR] Observando brokers: %s", *brokersFlag)
	if err := http.ListenAndServe(":"+*porta, nil); err != nil {
		log.Fatal(err)
	}
}

// ── Dashboard HTML ────────────────────────────────────────────────────────────

const dashboardHTML = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HormuzNet — Centro de Controle</title>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Share+Tech+Mono&display=swap" rel="stylesheet">
<style>
:root {
  --bg:       #05080d;
  --bg2:      #0b121a;
  --bg3:      #101a26;
  --border:   #1e3a4f;
  --green:    #00ff88;
  --green2:   #00cc6e;
  --green3:   #004422;
  --amber:    #ffcc00;
  --red:      #ff4444;
  --blue:     #00d4ff;
  --dim:      #345a70;
  --text:     #e0f2f7;
  --textdim:  #7fb89d;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

body {
  font-family: 'Share Tech Mono', monospace;
  background: var(--bg);
  color: var(--text);
  height: 100vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

/* ── HEADER ── */
header {
  background: var(--bg2);
  border-bottom: 1px solid var(--border);
  padding: 10px 20px;
  display: flex;
  align-items: center;
  gap: 20px;
  flex-shrink: 0;
  position: relative;
}
header::after {
  content: '';
  position: absolute;
  bottom: 0; left: 0; right: 0;
  height: 1px;
  background: linear-gradient(90deg, transparent, var(--green), transparent);
  opacity: 0.4;
}
.logo {
  font-family: 'Orbitron', sans-serif;
  font-size: 1.1rem;
  font-weight: 900;
  letter-spacing: .15em;
  color: var(--green);
  text-shadow: 0 0 20px rgba(0,232,122,.4);
}
.logo span { color: var(--textdim); font-weight: 400; }
.header-stats {
  display: flex;
  gap: 24px;
  margin-left: auto;
}
.hstat {
  display: flex;
  flex-direction: column;
  align-items: center;
  font-size: .65rem;
  color: var(--textdim);
  letter-spacing: .08em;
}
.hstat b {
  font-size: 1.3rem;
  color: var(--text);
  font-family: 'Orbitron', sans-serif;
  font-weight: 700;
}
.hstat b.green { color: var(--green); }
.hstat b.amber { color: var(--amber); }
.hstat b.red   { color: var(--red); }
.hstat b.blue  { color: var(--blue); }
.ws-dot {
  width: 8px; height: 8px;
  border-radius: 50%;
  background: var(--red);
  box-shadow: 0 0 6px var(--red);
  margin-left: auto;
  flex-shrink: 0;
}
.ws-dot.on { background: var(--green); box-shadow: 0 0 8px var(--green); }
.clock {
  font-family: 'Orbitron', sans-serif;
  font-size: .9rem;
  color: var(--green);
  letter-spacing: .1em;
  flex-shrink: 0;
}

/* ── LAYOUT PRINCIPAL ── */
.main {
  display: grid;
  grid-template-columns: 320px 1fr 320px;
  grid-template-rows: 1fr 280px;
  gap: 1px;
  background: var(--border);
  flex: 1;
  overflow: hidden;
}
.panel-bottom {
  grid-column: 1 / span 3;
}
.panel {
  background: var(--bg);
  overflow: hidden;
  display: flex;
  flex-direction: column;
}
.panel-title {
  font-family: 'Orbitron', sans-serif;
  font-size: .85rem;
  font-weight: 700;
  letter-spacing: .2em;
  color: var(--green2);
  padding: 10px 14px 8px;
  border-bottom: 1px solid var(--border);
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-shrink: 0;
}
.panel-title .cnt {
  background: var(--green3);
  color: var(--green);
  padding: 1px 7px;
  border-radius: 10px;
  font-size: .6rem;
}
.panel-body { flex: 1; overflow-y: auto; padding: 10px; }
.panel-body::-webkit-scrollbar { width: 3px; }
.panel-body::-webkit-scrollbar-thumb { background: var(--border); }

/* ── MAPA ── */
.map-wrap { 
  flex: 1; position: relative; overflow: hidden; 
  background: url('/assets/map.png') center center / cover no-repeat;
}
canvas#mapa {
  width: 100%; height: 100%;
  display: block;
}
.map-legend {
  position: absolute;
  bottom: 10px; right: 10px;
  background: rgba(7,11,18,.85);
  border: 1px solid var(--border);
  padding: 8px 12px;
  font-size: .8rem;
  line-height: 1.8;
}
.leg { display: flex; align-items: center; gap: 6px; }
.leg-dot { width: 8px; height: 8px; border-radius: 50%; }

/* ── DRONES ── */
.drone-card {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-left: 3px solid var(--dim);
  border-radius: 4px;
  padding: 8px 10px;
  margin-bottom: 6px;
  font-size: .85rem;
  transition: border-color .2s;
}
.drone-card.DISPONIVEL  { border-left-color: var(--green); }
.drone-card.DESPACHADO  { border-left-color: var(--amber); }
.drone-card.EM_MISSAO   { border-left-color: var(--blue);  }
.drone-card.RETORNANDO  { border-left-color: var(--textdim); }
.drone-card.ABATIDO { border-left-color: var(--red); opacity:.6; }
.drone-card.REALOCANDO  { border-left-color: var(--amber); }

.dc-top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px; }
.dc-id  { color: var(--text); font-weight: bold; letter-spacing: .04em; }
.dc-est {
  font-size: .6rem;
  padding: 1px 7px;
  border-radius: 3px;
  background: var(--bg3);
  letter-spacing: .06em;
}
.dc-est.DISPONIVEL  { color: var(--green); }
.dc-est.DESPACHADO  { color: var(--amber); }
.dc-est.EM_MISSAO   { color: var(--blue);  }
.dc-est.RETORNANDO  { color: var(--textdim); }
.dc-est.ABATIDO { color: var(--red); }
.dc-est.REALOCANDO  { color: var(--amber); }

.dc-info { color: var(--textdim); font-size: .66rem; display: flex; gap: 12px; }

/* ── BROKERS ── */
.broker-card {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 4px;
  padding: 8px 10px;
  margin-bottom: 6px;
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: .7rem;
}
.br-led {
  width: 9px; height: 9px;
  border-radius: 50%;
  flex-shrink: 0;
  background: var(--red);
  box-shadow: 0 0 5px var(--red);
}
.br-led.on { background: var(--green); box-shadow: 0 0 8px var(--green); }
.br-info { flex: 1; }
.br-id { color: var(--text); font-weight: bold; }
.br-addr { color: var(--textdim); font-size: .62rem; }
.br-hb { color: var(--textdim); font-size: .6rem; margin-left: auto; }

/* ── LOG ── */
.log-wrap { flex: 1; overflow-y: auto; padding: 8px; }
.log-wrap::-webkit-scrollbar { width: 3px; }
.log-wrap::-webkit-scrollbar-thumb { background: var(--border); }
.log-item {
  display: flex;
  gap: 8px;
  padding: 4px 0;
  border-bottom: 1px solid rgba(26,48,64,.5);
  font-size: .64rem;
  line-height: 1.4;
}
.log-hora { color: var(--textdim); flex-shrink: 0; }
.log-tipo {
  flex-shrink: 0;
  width: 70px;
  font-weight: bold;
  font-size: .6rem;
}
.log-tipo.info   { color: var(--green2); }
.log-tipo.warn   { color: var(--amber); }
.log-tipo.danger { color: var(--red); }
.log-msg { color: var(--text); }

/* ── TABELA DE FILA ── */
.table-wrap { flex: 1; overflow-y: auto; padding: 0; }
.fila-pedidos { width: 100%; border-collapse: collapse; font-family: 'Share Tech Mono', monospace; }
.fila-pedidos th { 
  position: sticky; top: 0; background: var(--bg3); 
  text-align: left; padding: 10px; font-size: .7rem; 
  color: var(--green); border-bottom: 2px solid var(--border);
}
.fila-pedidos td { padding: 8px 10px; font-size: .75rem; border-bottom: 1px solid rgba(26,48,64,.3); }
.st-espera { color: var(--amber); }
.st-andamento { color: var(--blue); font-weight: bold; }
.st-concluida { color: var(--green); opacity: .8; }

/* ── SCAN LINE EFFECT ── */
body::before {
  content: '';
  position: fixed;
  inset: 0;
  background: repeating-linear-gradient(
    0deg,
    transparent,
    transparent 2px,
    rgba(0,0,0,.06) 2px,
    rgba(0,0,0,.06) 4px
  );
  pointer-events: none;
  z-index: 9999;
}
</style>
</head>
<body>

<header>
  <div class="logo">HORMUZNET <span>// CENTRO DE CONTROLE TÁTICO</span></div>
  <div class="header-stats">
    <div class="hstat"><b class="green" id="h-disp">0</b>DISPONÍVEIS</div>
    <div class="hstat"><b class="amber" id="h-miss">0</b>EM MISSÃO</div>
    <div class="hstat"><b class="blue"  id="h-ret">0</b>RETORNANDO</div>
    <div class="hstat"><b class="red"   id="h-perd">0</b>PERDIDOS</div>
    <div class="hstat"><b id="h-total">0</b>TOTAL DRONES</div>
    <div style="width: 20px; border-left: 1px solid var(--border); margin: 0 10px;"></div>
    <div class="hstat"><b class="amber" id="h-oc-esp">0</b>EM ESPERA</div>
    <div class="hstat"><b class="blue" id="h-oc-and">0</b>EM ANDAMENTO</div>
    <div class="hstat"><b class="green" id="h-oc-con">0</b>CONCLUÍDAS</div>
  </div>
  <div class="clock" id="clock">--:--:--</div>
  <div class="ws-dot" id="wsdot"></div>
</header>

<div class="main">

  <!-- Coluna esquerda: drones -->
  <div class="panel">
    <div class="panel-title">
      UNIDADES AÉREAS
      <span class="cnt" id="cnt-drones">0</span>
    </div>
    <div class="panel-body" id="lista-drones">
      <div style="color:var(--textdim);font-size:.7rem;text-align:center;margin-top:40px">
        Aguardando conexão...
      </div>
    </div>
  </div>

  <!-- Centro: mapa tático -->
  <div class="panel">
    <div class="panel-title">MAPA TÁTICO — VISÃO CARTESIANA</div>
    <div class="map-wrap">
      <canvas id="mapa"></canvas>
      <div class="map-legend">
        <div class="leg"><div class="leg-dot" style="background:var(--green)"></div>DISPONÍVEL</div>
        <div class="leg"><div class="leg-dot" style="background:var(--amber)"></div>DESPACHADO</div>
        <div class="leg"><div class="leg-dot" style="background:var(--blue)"></div>EM MISSÃO</div>
        <div class="leg"><div class="leg-dot" style="background:var(--textdim)"></div>RETORNANDO</div>
        <div class="leg"><div class="leg-dot" style="background:var(--red)"></div>PERDIDO</div>
      </div>
    </div>
  </div>

  <!-- Coluna direita: brokers + log -->
  <div class="panel">
    <div class="panel-title">
      BROKERS
      <span class="cnt" id="cnt-brokers">0</span>
    </div>
    <div style="padding:8px;flex-shrink:0;max-height:240px;overflow-y:auto" id="lista-brokers"></div>
    <div class="panel-title" style="margin-top:4px">LOG DE EVENTOS</div>
    <div class="log-wrap" id="log-eventos"></div>
  </div>

  <!-- Rodapé: Fila de Pedidos -->
  <div class="panel panel-bottom">
    <div class="panel-title">
      FILA DE PEDIDOS EM TEMPO REAL
      <span class="cnt" id="cnt-ocorrencias">0</span>
    </div>
    <div class="table-wrap">
      <table class="fila-pedidos">
        <thead>
          <tr>
            <th>HORA</th>
            <th>ID DO PEDIDO</th>
            <th>TIPO</th>
            <th>CRITICIDADE</th>
            <th>STATUS ATUAL</th>
          </tr>
        </thead>
        <tbody id="corpo-fila">
          <!-- Dinâmico -->
        </tbody>
      </table>
    </div>
  </div>

</div>

<script>
// ── Estado ────────────────────────────────────────────────────────────────────
let estado = {drones: {}, brokers: [], eventos: [], failovers: {}};
let ws = null;

function formatTime(isoStr) {
  if (!isoStr || isoStr.startsWith('0001-01-01')) return '--';
  try {
    const d = new Date(isoStr);
    if (isNaN(d.getTime())) return '--';
    return d.toLocaleTimeString('pt-BR');
  } catch (e) {
    return '--';
  }
}
const COR = {
  DISPONIVEL:'#00e87a', DESPACHADO:'#ffb800', EM_MISSAO:'#00c2ff',
  RETORNANDO:'#4a7060', ABATIDO:'#ff3b3b', REALOCANDO:'#ffb800'
};

// ── Relógio ───────────────────────────────────────────────────────────────────
setInterval(() => {
  document.getElementById('clock').textContent =
    new Date().toLocaleTimeString('pt-BR');
}, 1000);

// ── WebSocket ─────────────────────────────────────────────────────────────────
function conectar() {
  ws = new WebSocket('ws://' + location.host + '/ws');
  ws.onopen  = () => document.getElementById('wsdot').classList.add('on');
  ws.onclose = () => { document.getElementById('wsdot').classList.remove('on'); setTimeout(conectar, 2500); };
  ws.onmessage = e => {
    try { estado = JSON.parse(e.data); renderTudo(); } catch(_){}
  };
}
conectar();

// ── Render ────────────────────────────────────────────────────────────────────
function renderTudo() {
  renderDrones();
  renderBrokers();
  renderLog();
  renderMapa();
  renderOcorrencias();
  atualizarHeader();
}

function atualizarHeader() {
  const d = Object.values(estado.drones || {});
  const disp  = d.filter(x => x.estado === 'DISPONIVEL').length;
  const miss  = d.filter(x => x.estado === 'EM_MISSAO' || x.estado === 'DESPACHADO').length;
  const ret   = d.filter(x => x.estado === 'RETORNANDO').length;
  const perd  = d.filter(x => x.estado === 'ABATIDO').length;
  document.getElementById('h-disp').textContent  = disp;
  document.getElementById('h-miss').textContent  = miss;
  document.getElementById('h-ret').textContent   = ret;
  document.getElementById('h-perd').textContent  = perd;
  document.getElementById('h-total').textContent = d.length;

  const o = Object.values(estado.ocorrencias || {});
  const esp = o.filter(x => x.status === 'ESPERA').length;
  const and = o.filter(x => x.status === 'ANDAMENTO').length;
  const con = o.filter(x => x.status === 'CONCLUIDA').length;
  document.getElementById('h-oc-esp').textContent = esp;
  document.getElementById('h-oc-and').textContent = and;
  document.getElementById('h-oc-con').textContent = con;
}

function renderOcorrencias() {
  const cont = document.getElementById('corpo-fila');
  const olist = Object.values(estado.ocorrencias || {})
    .sort((a,b) => b.id.localeCompare(a.id)) // Mais recentes primeiro
    .slice(0, 50);
  document.getElementById('cnt-ocorrencias').textContent = Object.keys(estado.ocorrencias).length;

  cont.innerHTML = olist.map(o => {
    const stClass = 'st-' + o.status.toLowerCase();
    const hora = formatTime(o.timestamp);
    return '<tr>'
      + '<td>' + hora + '</td>'
      + '<td style="font-size:.65rem;color:var(--dim)">' + o.id + '</td>'
      + '<td>' + o.tipo.toUpperCase() + '</td>'
      + '<td style="color:' + (o.criticidade==='ALTA'?'var(--red)':'var(--textdim)') + '">' + o.criticidade + '</td>'
      + '<td class="' + stClass + '">' + o.status + '</td>'
      + '</tr>';
  }).join('');
}

function renderDrones() {
  const cont = document.getElementById('lista-drones');
  const dlist = Object.values(estado.drones || {}).sort((a, b) => {
    const idA = a.drone_id || '';
    const idB = b.drone_id || '';
    return idA.localeCompare(idB, undefined, { numeric: true, sensitivity: 'base' });
  });
  document.getElementById('cnt-drones').textContent = dlist.length;
  if (!dlist.length) return;

  cont.innerHTML = dlist.map(d => {
    const oc = d.ocorrencia_id ? '<span style="color:var(--blue)">▶ ' + d.ocorrencia_id.slice(-12) + '</span>' : '';
    const pos = d.posicao ? '(' + Math.round(d.posicao.x) + ',' + Math.round(d.posicao.y) + ')' : '--';
    return '<div class="drone-card ' + d.estado + '">'
      + '<div class="dc-top">'
      +   '<span class="dc-id">' + d.drone_id + '</span>'
      +   '<span class="dc-est ' + d.estado + '">' + d.estado + '</span>'
      + '</div>'
      + '<div class="dc-info">'
      +   '<span>📍 ' + pos + '</span>'
      + '</div>'
      + (oc ? '<div style="margin-top:3px;font-size:.62rem">' + oc + '</div>' : '')
      + '</div>';
  }).join('');
}

function renderBrokers() {
  const cont = document.getElementById('lista-brokers');
  const todosOsBrokers = [
    { id: 'B1', setor: 'Setor_Noroeste' },
    { id: 'B2', setor: 'Setor_Norte' },
    { id: 'B3', setor: 'Setor_Nordeste' },
    { id: 'B4', setor: 'Setor_Leste' },
    { id: 'B5', setor: 'Setor_Sudeste' },
    { id: 'B6', setor: 'Setor_Sul' },
    { id: 'B7', setor: 'Setor_Sudoeste' },
    { id: 'B8', setor: 'Setor_Oeste' },
    { id: 'B9', setor: 'Setor_Centro' }
  ];

  cont.innerHTML = todosOsBrokers.map(eb => {
    const b = (estado.brokers || []).find(x => x.id === eb.id);
    const vivo = b ? b.vivo : false;
    const addr = b ? b.addr : 'Offline';
    const hb = b ? formatTime(b.ultimo_hb) : '--';
    
    return '<div class="broker-card" style="' + (!vivo ? 'opacity: 0.6;' : '') + '">'
      + '<div class="br-led' + (vivo ? ' on' : '') + '"></div>'
      + '<div class="br-info">'
      +   '<div class="br-id">' + eb.id + ' <span style="font-size:0.6rem;color:var(--textdim)">(' + eb.setor.split('_')[1] + ')</span></div>'
      +   '<div class="br-addr">' + addr + '</div>'
      + '</div>'
      + '<div class="br-hb">' + hb + '</div>'
      + '</div>';
  }).join('');

  const ativos = (estado.brokers || []).filter(x => x.vivo).length;
  document.getElementById('cnt-brokers').textContent = ativos + '/' + todosOsBrokers.length;
}

function renderLog() {
  const cont = document.getElementById('log-eventos');
  const evs = (estado.eventos || []).slice().reverse().slice(0, 40);
  cont.innerHTML = evs.map(e => {
    const hora = formatTime(e.timestamp);
    return '<div class="log-item">'
      + '<span class="log-hora">' + hora + '</span>'
      + '<span class="log-tipo ' + e.nivel + '">' + e.tipo + '</span>'
      + '<span class="log-msg">' + e.mensagem + '</span>'
      + '</div>';
  }).join('');
}

// ── Mapa Tático ───────────────────────────────────────────────────────────────
function renderMapa() {
  const canvas = document.getElementById('mapa');
  const wrap = canvas.parentElement;
  canvas.width  = wrap.clientWidth;
  canvas.height = wrap.clientHeight;
  const ctx = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;

  // Limpa o fundo para mostrar a imagem
  ctx.clearRect(0, 0, W, H);

  // Grade
  ctx.strokeStyle = 'rgba(26,48,64,.5)';
  ctx.lineWidth = 1;
  const step = 60;
  for (let x = 0; x < W; x += step) { ctx.beginPath(); ctx.moveTo(x,0); ctx.lineTo(x,H); ctx.stroke(); }
  for (let y = 0; y < H; y += step) { ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(W,y); ctx.stroke(); }

  // Eixos
  ctx.strokeStyle = 'rgba(0,232,122,.15)';
  ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(W/2,0); ctx.lineTo(W/2,H); ctx.stroke();
  ctx.beginPath(); ctx.moveTo(0,H/2); ctx.lineTo(W,H/2); ctx.stroke();

  const setorParaBroker = {
    'Setor_Noroeste': 'B1',
    'Setor_Norte': 'B2',
    'Setor_Nordeste': 'B3',
    'Setor_Leste': 'B4',
    'Setor_Sudeste': 'B5',
    'Setor_Sul': 'B6',
    'Setor_Sudoeste': 'B7',
    'Setor_Oeste': 'B8',
    'Setor_Centro': 'B9'
  };

  // Zonas dos Brokers (Grade 3x3 vazada no centro)
  const drawSec = (c, x, y, w, h, setorName) => {
    let fill = c;
    let stroke = c.replace('0.15', '0.5');
    const failoverBroker = estado.failovers ? estado.failovers[setorName] : null;
    if (failoverBroker) {
      fill = 'rgba(255, 68, 68, 0.2)'; // Highlighted red/orange for failover
      stroke = 'rgba(255, 68, 68, 0.7)';
    } else {
      const bId = setorParaBroker[setorName];
      const broker = (estado.brokers || []).find(b => b.id === bId);
      const vivo = broker ? broker.vivo : false;
      if (!vivo) {
        fill = 'rgba(30, 30, 30, 0.55)'; // Grayed out/darker for offline
        stroke = 'rgba(255, 68, 68, 0.3)'; // Dim red stroke for offline
      }
    }
    ctx.fillStyle = fill; ctx.fillRect(x, y, w, h);
    ctx.strokeStyle = stroke; ctx.strokeRect(x, y, w, h);
  };
  const cw = W/3, ch = H/3;
  drawSec('rgba(0, 232, 122, 0.15)', 0, 0, cw, ch, 'Setor_Noroeste'); // NW
  drawSec('rgba(0, 194, 255, 0.15)', cw, 0, cw, ch, 'Setor_Norte'); // N
  drawSec('rgba(255, 184, 0, 0.15)', cw*2, 0, cw, ch, 'Setor_Nordeste'); // NE
  drawSec('rgba(255, 59, 59, 0.15)', cw*2, ch, cw, ch, 'Setor_Leste'); // E
  drawSec('rgba(0, 232, 122, 0.15)', cw*2, ch*2, cw, ch, 'Setor_Sudeste'); // SE
  drawSec('rgba(0, 194, 255, 0.15)', cw, ch*2, cw, ch, 'Setor_Sul'); // S
  drawSec('rgba(255, 184, 0, 0.15)', 0, ch*2, cw, ch, 'Setor_Sudoeste'); // SW
  drawSec('rgba(255, 59, 59, 0.15)', 0, ch, cw, ch, 'Setor_Oeste'); // W
  drawSec('rgba(100, 100, 100, 0.15)', cw, ch, cw, ch, 'Setor_Centro'); // CENTRO

  const getLabel = (defaultText, setorName) => {
    const failoverBroker = estado.failovers ? estado.failovers[setorName] : null;
    if (failoverBroker) {
      return { text: defaultText + ' (FAILOVER: ' + failoverBroker + ')', color: 'rgba(255, 68, 68, 0.95)', font: 'bold 11px Orbitron' };
    }
    const bId = setorParaBroker[setorName];
    const broker = (estado.brokers || []).find(b => b.id === bId);
    const vivo = broker ? broker.vivo : false;
    if (!vivo) {
      return { text: defaultText + ' (INATIVO)', color: 'rgba(255, 68, 68, 0.65)', font: 'bold 10px Orbitron' };
    }
    return { text: defaultText, color: 'rgba(255, 255, 255, 0.85)', font: 'bold 11px Orbitron' };
  };

  const drawLabel = (defaultText, setorName, tx, ty) => {
    const lbl = getLabel(defaultText, setorName);
    ctx.fillStyle = lbl.color;
    ctx.font = lbl.font;
    ctx.fillText(lbl.text, tx, ty);
  };

  ctx.textAlign = 'center';
  drawLabel('B1: NOROESTE', 'Setor_Noroeste', cw/2, ch/2);
  drawLabel('B2: NORTE', 'Setor_Norte', cw*1.5, ch/2);
  drawLabel('B3: NORDESTE', 'Setor_Nordeste', cw*2.5, ch/2);
  drawLabel('B4: LESTE', 'Setor_Leste', cw*2.5, ch*1.5);
  drawLabel('B5: SUDESTE', 'Setor_Sudeste', cw*2.5, ch*2.5);
  drawLabel('B6: SUL', 'Setor_Sul', cw*1.5, ch*2.5);
  drawLabel('B7: SUDOESTE', 'Setor_Sudoeste', cw/2, ch*2.5);
  drawLabel('B8: OESTE', 'Setor_Oeste', cw/2, ch*1.5);
  drawLabel('B9: CENTRO', 'Setor_Centro', cw*1.5, ch*1.5);

  const dlist = Object.values(estado.drones || {});
  if (!dlist.length) {
    ctx.fillStyle = 'rgba(74,112,96,.5)';
    ctx.font = '14px Share Tech Mono';
    ctx.textAlign = 'center';
    ctx.fillText('SEM DRONES REGISTRADOS', W/2, H/2);
    return;
  }

  // Escala fixa para grade 3x3 (0 a 1000 unidades)
  const xMin = 0, xMax = 1000;
  const yMin = 0, yMax = 1000;
  const scaleX = (W - 40) / (xMax - xMin);
  const scaleY = (H - 40) / (yMax - yMin);
  const scale  = Math.min(scaleX, scaleY);
  const offX = (W - (xMax - xMin) * scale) / 2;
  const offY = (H - (yMax - yMin) * scale) / 2;

  const toScreen = (x, y) => ({ sx: x * scale + offX, sy: H - (y * scale + offY) });



  // Drones
  dlist.forEach(d => {
    if (!d.posicao) return;
    const {sx, sy} = toScreen(d.posicao.x, d.posicao.y);
    const cor = COR[d.estado] || '#4a7060';

    // Aura para drones em missão
    if (d.estado === 'EM_MISSAO' || d.estado === 'DESPACHADO') {
      const grad = ctx.createRadialGradient(sx, sy, 2, sx, sy, 18);
      grad.addColorStop(0, cor + '44');
      grad.addColorStop(1, 'transparent');
      ctx.beginPath(); ctx.arc(sx, sy, 18, 0, Math.PI*2);
      ctx.fillStyle = grad; ctx.fill();
    }

    // Ponto do drone
    ctx.beginPath();
    ctx.arc(sx, sy, 5, 0, Math.PI * 2);
    ctx.fillStyle = cor;
    ctx.shadowColor = cor;
    ctx.shadowBlur = 8;
    ctx.fill();
    ctx.shadowBlur = 0;

    // Label
    ctx.fillStyle = cor;
    ctx.font = 'bold 10px Share Tech Mono';
    ctx.textAlign = 'center';
    ctx.fillText(d.drone_id.split('_').pop(), sx, sy - 10);
  });
}

window.addEventListener('resize', renderMapa);
</script>
</body>
</html>`
