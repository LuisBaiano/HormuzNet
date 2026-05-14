package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"

	"HormuzNet/internal/models"
)

func main() {
	setor := flag.String("setor", "", "ID do setor para filtrar (ex: Setor_Norte)")
	brokerAddr := flag.String("broker", "224.0.0.1:8080", "Endereço Multicast UDP dos sensores")
	flag.Parse()

	if *setor == "" {
		fmt.Println("Uso: observer -setor <NomeDoSetor>")
		return
	}

	addr, err := net.ResolveUDPAddr("udp", *brokerAddr)
	if err != nil {
		log.Fatalf("Erro ao resolver endereço UDP: %v", err)
	}

	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		log.Fatalf("Erro ao ouvir Multicast: %v", err)
	}
	defer conn.Close()

	fmt.Printf("====================================================\n")
	fmt.Printf("  OBSERVADOR DO SETOR: %s\n", *setor)
	fmt.Printf("====================================================\n")
	fmt.Printf("Aguardando leituras de sensores...\n\n")

	buf := make([]byte, 4096)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		var leitura models.LeituraSensor
		if err := json.Unmarshal(buf[:n], &leitura); err != nil {
			continue
		}

		if leitura.SetorID == *setor {
			critStr := "\033[1;32mBAIXA\033[0m"
			if leitura.Criticidade == models.CriticidadeAlta {
				critStr = "\033[1;31mALTA\033[0m"
			}
			fmt.Printf("[%s] Sensor: %-12s | Tipo: %-6s | Valor: %6.2f %-10s | Crit: %s\n",
				leitura.Timestamp.Format("15:04:05.000"),
				leitura.SensorID,
				leitura.Tipo,
				leitura.Valor,
				leitura.Unidade,
				critStr)
		}
	}
}
