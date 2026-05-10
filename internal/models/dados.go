package models

import "time"

// ── Níveis de criticidade ─────────────────────────────────────────────────────

type Criticidade int

const (
	CriticidadeBaixa   Criticidade = 1
	CriticidadeMedia   Criticidade = 2
	CriticidadeAlta    Criticidade = 3
	CriticidadeCritica Criticidade = 4
	CriticidadeMaxima  Criticidade = 5
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
	DroneRealocando EstadoDrone = "REALOCANDO" // base destruída, aguardando nova base
)

// ── Sensor → Broker (UDP) ─────────────────────────────────────────────────────

type LeituraSensor struct {
	SensorID    string      `json:"sensor_id"`
	SetorID     string      `json:"setor_id"`
	Tipo        string      `json:"tipo"`
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
	Atendida     bool        `json:"atendida"`
	DroneID      string      `json:"drone_id,omitempty"`
}

// ── InfoDrone ─────────────────────────────────────────────────────────────────

type InfoDrone struct {
	DroneID      string      `json:"drone_id"`
	BaseID       string      `json:"base_id"`       // base atual (pode mudar por realocação)
	BrokerID     string      `json:"broker_id"`     // broker responsável pelo drone
	Estado       EstadoDrone `json:"estado"`
	OcorrenciaID string      `json:"ocorrencia_id,omitempty"`
	Bateria      int         `json:"bateria"`       // 0-100 %
	Posicao      Coordenada  `json:"posicao"`       // posição atual
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
	// Enviado sempre que um drone muda de estado em qualquer broker
	MsgSincDrone TipoMensagemBroker = "SINC_DRONE"

	// Realocação: base destruída → drones migram para outra base
	MsgRealocacaoDrones TipoMensagemBroker = "REALOCACAO_DRONES"

	// Notificação de missão concluída (drone terminou serviço)
	MsgMissaoConcluida TipoMensagemBroker = "MISSAO_CONCLUIDA"
)

type MensagemBroker struct {
	Tipo      TipoMensagemBroker `json:"tipo"`
	BrokerID  string             `json:"broker_id"`
	Timestamp time.Time          `json:"timestamp"`

	Ocorrencia   *Ocorrencia  `json:"ocorrencia,omitempty"`
	DroneID      string       `json:"drone_id,omitempty"`
	BaseID       string       `json:"base_id,omitempty"`
	OcorrenciaID string       `json:"ocorrencia_id,omitempty"`
	Motivo       string       `json:"motivo,omitempty"`

	// SINC_DRONE: estado atualizado de um drone
	Drone *InfoDrone `json:"drone,omitempty"`

	// REALOCACAO_DRONES: lista de drones órfãos após base destruída
	DronesOrfaos []InfoDrone `json:"drones_orfaos,omitempty"`

	// REPLICA_FILA: snapshot de ocorrências pendentes
	FilaPendente []Ocorrencia `json:"fila_pendente,omitempty"`
}

// ── Mensagens base→broker (TCP) ───────────────────────────────────────────────

type TipoMensagemBase string

const (
	BaseRegistro     TipoMensagemBase = "REGISTRO_BASE"
	BaseStatusDrones TipoMensagemBase = "STATUS_DRONES"
	BaseDroneEstado  TipoMensagemBase = "DRONE_ESTADO"
	// Base aceita drones realocados de outra base destruída
	BaseAceitarDrones TipoMensagemBase = "ACEITAR_DRONES"
)

type MensagemBase struct {
	Tipo      TipoMensagemBase `json:"tipo"`
	BaseID    string           `json:"base_id"`
	SetorID   string           `json:"setor_id"`
	Posicao   Coordenada       `json:"posicao"`
	Timestamp time.Time        `json:"timestamp"`

	Drones       []InfoDrone `json:"drones,omitempty"`
	DroneID      string      `json:"drone_id,omitempty"`
	NovoEstado   EstadoDrone `json:"novo_estado,omitempty"`
	OcorrenciaID string      `json:"ocorrencia_id,omitempty"`
	Posicao2     Coordenada  `json:"posicao_drone,omitempty"` // posição atual do drone
}

// ── Comandos broker→base (TCP) ────────────────────────────────────────────────

type TipoComandoBase string

const (
	CmdDespacharDrone  TipoComandoBase = "DESPACHAR_DRONE"
	CmdRetornarDrone   TipoComandoBase = "RETORNAR_DRONE"
	// Broker ordena base a absorver drones realocados
	CmdReceberDrones   TipoComandoBase = "RECEBER_DRONES"
)

type ComandoBase struct {
	Tipo         TipoComandoBase `json:"tipo"`
	DroneID      string          `json:"drone_id,omitempty"`
	OcorrenciaID string          `json:"ocorrencia_id,omitempty"`
	SetorDestino string          `json:"setor_destino,omitempty"`
	PosicaoAlvo  Coordenada      `json:"posicao_alvo,omitempty"`
	Timestamp    time.Time       `json:"timestamp"`
	// CmdReceberDrones: lista de drones para absorver
	DronesParaAbsorver []InfoDrone `json:"drones_para_absorver,omitempty"`
}
