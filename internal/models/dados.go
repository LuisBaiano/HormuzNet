package models

import "time"

// ── Níveis de criticidade de uma ocorrência ────────────────────────────────

type Criticidade int

const (
	CriticidadeBaixa  Criticidade = 1
	CriticidadeMedia  Criticidade = 2
	CriticidadeAlta   Criticidade = 3
	CriticidadeCritica Criticidade = 4
	CriticidadeMaxima Criticidade = 5
)

func (c Criticidade) String() string {
	switch c {
	case CriticidadeBaixa:
		return "BAIXA"
	case CriticidadeMedia:
		return "MEDIA"
	case CriticidadeAlta:
		return "ALTA"
	case CriticidadeCritica:
		return "CRITICA"
	case CriticidadeMaxima:
		return "MAXIMA"
	default:
		return "DESCONHECIDA"
	}
}

// ── Estados do drone ───────────────────────────────────────────────────────

type EstadoDrone string

const (
	DroneDisponivel  EstadoDrone = "DISPONIVEL"
	DroneDespachado  EstadoDrone = "DESPACHADO"
	DroneEmMissao    EstadoDrone = "EM_MISSAO"
	DroneRetornando  EstadoDrone = "RETORNANDO"
	DroneAbatido     EstadoDrone = "ABATIDO"
	DroneSemBateria  EstadoDrone = "SEM_BATERIA"
)

// ── Sensor → Broker (UDP) ──────────────────────────────────────────────────

// LeituraSensor representa a leitura bruta enviada por um sensor via UDP.
// O campo OcorrenciaID é preenchido pelo broker ao detectar evento crítico.
type LeituraSensor struct {
	SensorID    string      `json:"sensor_id"`
	SetorID     string      `json:"setor_id"`
	Tipo        string      `json:"tipo"`       // radar | sonar | boia | visual
	Valor       float64     `json:"valor"`
	Unidade     string      `json:"unidade"`
	Criticidade Criticidade `json:"criticidade"`
	Timestamp   time.Time   `json:"timestamp"`
}

// ── Ocorrência gerada pelo broker ──────────────────────────────────────────

// Ocorrencia é criada pelo broker ao receber leitura crítica e propagada
// em cascata para outros brokers quando não há drone disponível localmente.
type Ocorrencia struct {
	ID          string      `json:"id"`
	SetorOrigem string      `json:"setor_origem"`
	BrokerOrigem string     `json:"broker_origem"`
	Tipo        string      `json:"tipo"`
	Descricao   string      `json:"descricao"`
	Criticidade Criticidade `json:"criticidade"`
	Timestamp   time.Time   `json:"timestamp"`
	Atendida    bool        `json:"atendida"`
	DroneID     string      `json:"drone_id,omitempty"`
}

// ── Mensagens broker↔broker (TCP, JSON delimitado por linha) ──────────────

type TipoMensagemBroker string

const (
	// Broker solicita drone a outro broker
	MsgRequisicaoDrone TipoMensagemBroker = "REQUISICAO_DRONE"
	// Broker confirma despacho de drone para a ocorrência
	MsgDroneDespachado TipoMensagemBroker = "DRONE_DESPACHADO"
	// Broker informa que não tem drone disponível
	MsgSemDrone TipoMensagemBroker = "SEM_DRONE"
	// Broker notifica todos que drone foi liberado (missão concluída)
	MsgDroneLiberado TipoMensagemBroker = "DRONE_LIBERADO"
	// Broker notifica todos que drone foi perdido (abatido/sem bateria)
	MsgDronePerdido TipoMensagemBroker = "DRONE_PERDIDO"
	// Heartbeat periódico para detecção de falha
	MsgHeartbeat TipoMensagemBroker = "HEARTBEAT"
	// Broker recém-iniciado se apresenta à rede
	MsgRegistro TipoMensagemBroker = "REGISTRO"
	// Replicação de fila: broker repassa ocorrências pendentes ao se conectar
	MsgReplicaFila TipoMensagemBroker = "REPLICA_FILA"
)

// MensagemBroker é o envelope único para toda comunicação TCP entre brokers.
type MensagemBroker struct {
	Tipo      TipoMensagemBroker `json:"tipo"`
	BrokerID  string             `json:"broker_id"`
	Timestamp time.Time          `json:"timestamp"`

	// Preenchido conforme o Tipo da mensagem
	Ocorrencia  *Ocorrencia  `json:"ocorrencia,omitempty"`
	DroneID     string       `json:"drone_id,omitempty"`
	BaseID      string       `json:"base_id,omitempty"`
	OcorrenciaID string      `json:"ocorrencia_id,omitempty"`
	Motivo      string       `json:"motivo,omitempty"`

	// Usado em REPLICA_FILA
	FilaPendente []Ocorrencia `json:"fila_pendente,omitempty"`
}

// ── Base → Broker (TCP) ────────────────────────────────────────────────────

type TipoMensagemBase string

const (
	// Base registra ao se conectar ao broker
	BaseRegistro TipoMensagemBase = "REGISTRO_BASE"
	// Base reporta status atual de todos os seus drones
	BaseStatusDrones TipoMensagemBase = "STATUS_DRONES"
	// Drone reporta mudança de estado (via base)
	BaseDroneEstado TipoMensagemBase = "DRONE_ESTADO"
)

type MensagemBase struct {
	Tipo      TipoMensagemBase `json:"tipo"`
	BaseID    string           `json:"base_id"`
	SetorID   string           `json:"setor_id"`
	Timestamp time.Time        `json:"timestamp"`

	// Lista de drones desta base e seus estados atuais
	Drones []InfoDrone `json:"drones,omitempty"`

	// Atualização de um único drone
	DroneID      string      `json:"drone_id,omitempty"`
	NovoEstado   EstadoDrone `json:"novo_estado,omitempty"`
	OcorrenciaID string      `json:"ocorrencia_id,omitempty"`
}

// ── Broker → Base (TCP) — comandos ────────────────────────────────────────

type TipoComandoBase string

const (
	// Broker ordena despacho de drone específico
	CmdDespacharDrone TipoComandoBase = "DESPACHAR_DRONE"
	// Broker ordena retorno de drone
	CmdRetornarDrone TipoComandoBase = "RETORNAR_DRONE"
)

type ComandoBase struct {
	Tipo         TipoComandoBase `json:"tipo"`
	DroneID      string          `json:"drone_id"`
	OcorrenciaID string          `json:"ocorrencia_id"`
	SetorDestino string          `json:"setor_destino"`
	Timestamp    time.Time       `json:"timestamp"`
}

// ── InfoDrone — estado de um drone individual ──────────────────────────────

type InfoDrone struct {
	DroneID      string      `json:"drone_id"`
	BaseID       string      `json:"base_id"`
	Estado       EstadoDrone `json:"estado"`
	OcorrenciaID string      `json:"ocorrencia_id,omitempty"` // se em missão
	Bateria      int         `json:"bateria"`                 // 0-100 %
	UltimaVez    time.Time   `json:"ultima_vez"`
}

func (d *InfoDrone) Disponivel() bool {
	return d.Estado == DroneDisponivel && d.Bateria > 10
}
