package models

import "time"

// ── Níveis de criticidade ─────────────────────────────────────────────────────

type Criticidade int

const (
	CriticidadeBaixa Criticidade = 1
	CriticidadeAlta  Criticidade = 2
)

func (c Criticidade) String() string {
	switch c {
	case CriticidadeBaixa:
		return "BAIXA"
	case CriticidadeAlta:
		return "ALTA"
	default:
		return "DESCONHECIDA"
	}
}

// ── Coordenada cartesiana ─────────────────────────────────────────────────────
// Usada por drones, bases e ocorrências para cálculo de proximidade.

type Coordenada struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// Distancia calcula distância euclidiana entre dois pontos.
func (c Coordenada) Distancia(outro Coordenada) float64 {
	dx := c.X - outro.X
	dy := c.Y - outro.Y
	return dx*dx + dy*dy // retorna quadrado (evita sqrt; suficiente para comparação)
}

// ── Estados do drone ──────────────────────────────────────────────────────────

type EstadoDrone string

const (
	DroneDisponivel EstadoDrone = "DISPONIVEL"
	DroneDespachado EstadoDrone = "DESPACHADO"
	DroneEmMissao   EstadoDrone = "EM_MISSAO"
	DroneRetornando EstadoDrone = "RETORNANDO"
	DroneAbatido    EstadoDrone = "ABATIDO"
	DroneSemBateria EstadoDrone = "SEM_BATERIA"
)

// ── Sensor → Broker (UDP) ─────────────────────────────────────────────────────

type LeituraSensor struct {
	SensorID    string      `json:"sensor_id"`
	SetorID     string      `json:"setor_id"`
	Tipo        string      `json:"tipo"`
	Posicao     Coordenada  `json:"posicao"` // coordenada do sensor
	Valor       float64     `json:"valor"`
	Unidade     string      `json:"unidade"`
	Criticidade Criticidade `json:"criticidade"`
	Timestamp   time.Time   `json:"timestamp"`
}

// ── Ocorrência ────────────────────────────────────────────────────────────────

type Ocorrencia struct {
	ID           string      `json:"id"`
	SetorOrigem  string      `json:"setor_origem"`
	BrokerOrigem string      `json:"broker_origem"`
	Tipo         string      `json:"tipo"`
	Descricao    string      `json:"descricao"`
	Criticidade  Criticidade `json:"criticidade"`
	Posicao      Coordenada  `json:"posicao"`      // localização do evento
	Timestamp    time.Time   `json:"timestamp"`
	LamportTime  int         `json:"lamport_time"` // Relógio de Lamport
	Atendida     bool        `json:"atendida"`
	DroneID      string      `json:"drone_id,omitempty"`
}

// ── InfoDrone ─────────────────────────────────────────────────────────────────

type InfoDrone struct {
	DroneID      string      `json:"drone_id"`
	BrokerID     string      `json:"broker_id"` // broker responsável atual
	Estado       EstadoDrone `json:"estado"`
	OcorrenciaID string      `json:"ocorrencia_id,omitempty"`
	Bateria      int         `json:"bateria"` // 0-100 %
	Posicao      Coordenada  `json:"posicao"` // posição atual
	UltimaVez    time.Time   `json:"ultima_vez"`
	// Controle de ociosidade: instante em que ficou disponível pela última vez
	DisponiveisDesde time.Time `json:"disponivel_desde,omitempty"`
}

func (d *InfoDrone) Disponivel() bool {
	return d.Estado == DroneDisponivel && d.Bateria > 10
}

// ── Mensagens broker↔broker (TCP) ────────────────────────────────────────────

type TipoMensagemBroker string

const (
	MsgRequisicaoDrone TipoMensagemBroker = "REQUISICAO_DRONE"
	MsgDroneDespachado TipoMensagemBroker = "DRONE_DESPACHADO"
	MsgSemDrone        TipoMensagemBroker = "SEM_DRONE"
	MsgDroneLiberado   TipoMensagemBroker = "DRONE_LIBERADO"
	MsgDronePerdido    TipoMensagemBroker = "DRONE_PERDIDO"
	MsgHeartbeat       TipoMensagemBroker = "HEARTBEAT"
	MsgRegistro        TipoMensagemBroker = "REGISTRO"
	MsgReplicaFila     TipoMensagemBroker = "REPLICA_FILA"

	// Sincronização global de drones entre brokers
	MsgSincDrone TipoMensagemBroker = "SINC_DRONE"

	// Notificação de missão concluída
	MsgMissaoConcluida TipoMensagemBroker = "MISSAO_CONCLUIDA"
)

type MensagemBroker struct {
	Tipo        TipoMensagemBroker `json:"tipo"`
	BrokerID    string             `json:"broker_id"`
	Timestamp   time.Time          `json:"timestamp"`
	LamportTime int                `json:"lamport_time"` // Relógio de Lamport

	Ocorrencia   *Ocorrencia `json:"ocorrencia,omitempty"`
	DroneID      string      `json:"drone_id,omitempty"`
	OcorrenciaID string      `json:"ocorrencia_id,omitempty"`
	Motivo       string      `json:"motivo,omitempty"`

	// SINC_DRONE: estado atualizado de um drone
	Drone *InfoDrone `json:"drone,omitempty"`

	// REPLICA_FILA: snapshot de ocorrências pendentes
	FilaPendente []Ocorrencia `json:"fila_pendente,omitempty"`
}

// ── Mensagens Drone↔Broker (TCP) ──────────────────────────────────────────────

type TipoMensagemDrone string

const (
	DroneRegistro TipoMensagemDrone = "REGISTRO_DRONE"
	DroneKeepalive TipoMensagemDrone = "KEEPALIVE_DRONE"
	DroneEstado   TipoMensagemDrone = "DRONE_ESTADO"
)

type MensagemDrone struct {
	Tipo         TipoMensagemDrone `json:"tipo"`
	DroneID      string            `json:"drone_id"`
	Timestamp    time.Time         `json:"timestamp"`
	DroneInfo    *InfoDrone        `json:"drone_info,omitempty"`
	NovoEstado   EstadoDrone       `json:"novo_estado,omitempty"`
	OcorrenciaID string            `json:"ocorrencia_id,omitempty"`
	Posicao      Coordenada        `json:"posicao,omitempty"`
	Bateria      int               `json:"bateria,omitempty"`
}

type TipoComandoDrone string

const (
	CmdDespacharDrone TipoComandoDrone = "DESPACHAR_DRONE"
	CmdRetornarDrone  TipoComandoDrone = "RETORNAR_DRONE"
)

type ComandoDrone struct {
	Tipo         TipoComandoDrone `json:"tipo"`
	OcorrenciaID string           `json:"ocorrencia_id,omitempty"`
	SetorDestino string           `json:"setor_destino,omitempty"`
	PosicaoAlvo  Coordenada       `json:"posicao_alvo,omitempty"`
	Timestamp    time.Time        `json:"timestamp"`
}
