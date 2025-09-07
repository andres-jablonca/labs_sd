package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

type Oferta struct {
	BotinInicial      string
	ProbExitoFranklin string
	ProbExitoTrevor   string
	RiesgoPolicial    string
}

func getRandomOferta(filePath string) (*Oferta, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var validRows []Oferta

	// Saltar la primera línea
	if scanner.Scan() {
	}

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ",") // prueba con coma
		fmt.Println("DEBUG:", fields)      // ← parece que tu archivo usa tabuladores

		// Asegurar que tenga al menos 4 columnas
		if len(fields) < 4 {
			continue
		}

		// Verificar que al menos uno de los primeros 4 no esté vacío
		valid := false
		for i := 0; i < 4; i++ {
			if strings.TrimSpace(fields[i]) != "" {
				valid = true
				break
			}
		}

		if valid {
			oferta := Oferta{
				BotinInicial:      strings.TrimSpace(fields[0]),
				ProbExitoFranklin: strings.TrimSpace(fields[1]),
				ProbExitoTrevor:   strings.TrimSpace(fields[2]),
				RiesgoPolicial:    strings.TrimSpace(fields[3]),
			}
			validRows = append(validRows, oferta)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if len(validRows) == 0 {
		return nil, fmt.Errorf("no se encontraron filas válidas")
	}

	// Selección aleatoria
	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(validRows))
	return &validRows[randomIndex], nil
}

func main() {
	oferta, err := getRandomOferta("ofertas_mediano.csv")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Oferta seleccionada:\n%+v\n", *oferta)
}
