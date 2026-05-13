// Package fila implementa uma fila de prioridade thread-safe para ocorrências.
//
// Critério de ordenação:
//  1. Maior criticidade primeiro.
//  2. Empate: timestamp mais antigo primeiro (FIFO).
//
// Ao longo do tempo, ocorrências de menor criticidade sobem na fila pelo
// fator de envelhecimento: a cada 30s sem atendimento, a criticidade efetiva
// sobe 1 nível (máximo 5), garantindo que nenhuma requisição fique presa
// indefinidamente atrás de ocorrências mais críticas.
package fila

import (
	"container/heap"
	"sync"
	"time"

	"HormuzNet/internal/models"
)

const intervaloEnvelhecimento = 30 * time.Second

// item é o elemento interno do heap.
type item struct {
	ocorrencia  models.Ocorrencia
	prioridade  int       // calculada em tempo real
	criadadEm   time.Time // para desempate FIFO
	indice      int       // posição no slice interno (exigido por heap.Interface)
}

// heapInterno implementa heap.Interface para *item.
type heapInterno []*item

func (h heapInterno) Len() int { return len(h) }

// Less: maior prioridade sai primeiro; empate por menor tempo de Lamport; empate secundário por timestamp.
func (h heapInterno) Less(i, j int) bool {
	if h[i].prioridade != h[j].prioridade {
		return h[i].prioridade > h[j].prioridade // Maior prioridade primeiro
	}
	if h[i].ocorrencia.LamportTime != h[j].ocorrencia.LamportTime {
		return h[i].ocorrencia.LamportTime < h[j].ocorrencia.LamportTime // Menor tempo de Lamport primeiro
	}
	return h[i].criadadEm.Before(h[j].criadadEm)
}

func (h heapInterno) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].indice = i
	h[j].indice = j
}

func (h *heapInterno) Push(x interface{}) {
	n := len(*h)
	it := x.(*item)
	it.indice = n
	*h = append(*h, it)
}

func (h *heapInterno) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	it.indice = -1
	*h = old[:n-1]
	return it
}

// FilaPrioridade é a fila pública thread-safe.
type FilaPrioridade struct {
	mu   sync.Mutex
	heap heapInterno
}

// Nova cria uma FilaPrioridade pronta para uso.
func Nova() *FilaPrioridade {
	fp := &FilaPrioridade{}
	heap.Init(&fp.heap)
	return fp
}

// Enfileirar adiciona uma ocorrência à fila.
// Se já existir ocorrência com mesmo ID, a chamada é ignorada.
func (fp *FilaPrioridade) Enfileirar(oc models.Ocorrencia) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Evita duplicata
	for _, it := range fp.heap {
		if it.ocorrencia.ID == oc.ID {
			return
		}
	}

	heap.Push(&fp.heap, &item{
		ocorrencia: oc,
		prioridade: int(oc.Criticidade),
		criadadEm:  oc.Timestamp,
	})
}

// Desenfileirar retira e retorna a ocorrência de maior prioridade.
// Retorna false se a fila estiver vazia.
func (fp *FilaPrioridade) Desenfileirar() (models.Ocorrencia, bool) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	if fp.heap.Len() == 0 {
		return models.Ocorrencia{}, false
	}
	it := heap.Pop(&fp.heap).(*item)
	return it.ocorrencia, true
}

// Peek retorna a próxima ocorrência sem removê-la.
// Retorna false se a fila estiver vazia.
func (fp *FilaPrioridade) Peek() (models.Ocorrencia, bool) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	if fp.heap.Len() == 0 {
		return models.Ocorrencia{}, false
	}
	return fp.heap[0].ocorrencia, true
}

// Remover remove uma ocorrência pelo ID (ex: foi atendida por outro broker).
func (fp *FilaPrioridade) Remover(id string) bool {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	for i, it := range fp.heap {
		if it.ocorrencia.ID == id {
			heap.Remove(&fp.heap, i)
			return true
		}
	}
	return false
}

// Tamanho retorna o número de ocorrências pendentes.
func (fp *FilaPrioridade) Tamanho() int {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.heap.Len()
}

// Snapshot retorna cópia ordenada da fila (não remove elementos).
// Usado para replicação de fila entre brokers.
func (fp *FilaPrioridade) Snapshot() []models.Ocorrencia {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Deep copy: cria novos *item para não corromper índices do heap original
	copia := make(heapInterno, len(fp.heap))
	for i, it := range fp.heap {
		clone := *it // copia o valor do item (não o ponteiro)
		copia[i] = &clone
	}
	heap.Init(&copia)

	resultado := make([]models.Ocorrencia, 0, len(copia))
	for copia.Len() > 0 {
		it := heap.Pop(&copia).(*item)
		resultado = append(resultado, it.ocorrencia)
	}
	return resultado
}

// Envelhecer percorre a fila e incrementa a prioridade efetiva de ocorrências
// esperando há mais de intervaloEnvelhecimento.
// Deve ser chamado periodicamente (ex: a cada 10s) por uma goroutine do broker.
func (fp *FilaPrioridade) Envelhecer() {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	agora := time.Now()
	alterou := false
	for _, it := range fp.heap {
		espera := agora.Sub(it.criadadEm)
		nivelExtra := int(espera / intervaloEnvelhecimento)
		novaPrioridade := int(it.ocorrencia.Criticidade) + nivelExtra
		if novaPrioridade > int(models.CriticidadeAlta) {
			novaPrioridade = int(models.CriticidadeAlta)
		}
		if novaPrioridade != it.prioridade {
			it.prioridade = novaPrioridade
			alterou = true
		}
	}
	if alterou {
		heap.Init(&fp.heap) // re-heapifica após mudanças de prioridade
	}
}
