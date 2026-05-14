package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net"
	"time"

	"HormuzNet/internal/models"
)

func main() {
	id := flag.String("id", "sensor_01", "ID do sensor")
	tipo := flag.String("tipo", "radar", "Tipo do sensor (radar, sonar, boia, visual, meteo)")
	setor := flag.String("setor", "Setor_Norte", "ID do setor")
	broker := flag.String("broker", "224.0.0.1:8080", "Endereço Multicast UDP")
	intervalo := flag.Int("intervalo", 1000, "Intervalo entre leituras (ms)")
	posX := flag.Float64("x", 0, "Posição X inicial")
	posY := flag.Float64("y", 0, "Posição Y inicial")
	flag.Parse()

	// Seed para aleatoriedade
	rand.Seed(time.Now().UnixNano())

	// Resolve endereço UDP do broker
	addr, err := net.ResolveUDPAddr("udp", *broker)
	if err != nil {
		log.Fatalf("Erro ao resolver endereço: %v", err)
	}

	// Abre conexão UDP
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatalf("Erro ao conectar UDP: %v", err)
	}
	defer conn.Close()

	log.Printf("Sensor %s [%s] iniciado. Enviando para %s a cada %dms", *id, *tipo, *broker, *intervalo)

	ticker := time.NewTicker(time.Duration(*intervalo) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		leitura := gerarLeitura(*id, *tipo, *setor, *posX, *posY)
		dados, err := json.Marshal(leitura)
		if err != nil {
			continue
		}
		
		_, err = conn.Write(dados)
		if err != nil {
			log.Printf("Erro ao enviar dados: %v", err)
		}
	}
}

func gerarLeitura(id, tipo, setor string, x, y float64) models.LeituraSensor {
	var valor float64
	var unidade string
	crit := models.CriticidadeNula

	// Só gera uma possível ocorrência em 20% das leituras para manter drones ocupados
	if rand.Float64() > 0.20 {
		return models.LeituraSensor{
			SensorID:    id,
			SetorID:     setor,
			Tipo:        tipo,
			Posicao:     models.Coordenada{X: x, Y: y},
			Valor:       0,
			Unidade:     "-",
			Criticidade: crit,
			Timestamp:   time.Now(),
		}
	}

	// Geração de dados baseada no tipo para maior realismo
	switch tipo {
	case "radar":
		valor = rand.Float64() * 100
		unidade = "objetos"
		if valor > 75 {
			crit = models.CriticidadeAlta
		} else {
			crit = models.CriticidadeBaixa
		}
	case "sonar":
		valor = rand.Float64() * 150
		unidade = "dB"
		if valor > 100 {
			crit = models.CriticidadeAlta
		} else {
			crit = models.CriticidadeBaixa
		}
	case "boia":
		valor = rand.Float64() * 12
		unidade = "m"
		if valor > 7 {
			crit = models.CriticidadeAlta
		} else {
			crit = models.CriticidadeBaixa
		}
	case "visual":
		valor = rand.Float64()
		unidade = "confiança"
		if valor > 0.85 {
			crit = models.CriticidadeAlta
		} else {
			crit = models.CriticidadeBaixa
		}
	case "meteo":
		valor = rand.Float64()*60 - 10 // -10 a 50
		unidade = "°C"
		if valor > 45 || valor < -5 {
			crit = models.CriticidadeAlta
		} else {
			crit = models.CriticidadeBaixa
		}
	default:
		valor = rand.Float64() * 100
		unidade = "un"
		crit = models.CriticidadeBaixa
	}

	// Injeção de eventos críticos aleatórios (2% de chance) para testar despacho de drones
	if rand.Float64() < 0.02 {
		crit = models.CriticidadeAlta
	}

	return models.LeituraSensor{
		SensorID:    id,
		SetorID:     setor,
		Tipo:        tipo,
		Posicao:     models.Coordenada{X: x, Y: y},
		Valor:       valor,
		Unidade:     unidade,
		Criticidade: crit,
		Timestamp:   time.Now(),
	}
}
