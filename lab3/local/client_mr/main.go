package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	pb "lab3/proto"
	"log"
	"math/rand"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Estructura para representar el cliente (Pasajero Observador)
type Client struct {
	flightID     string
	NuevaVersion int64
	clientID     string
}

// Para almacenar el estado de cada vuelo individual en la función main
type FlightState struct {
	VersionGuardada int64
}

// Leer el archivo CSV y cargar los vuelos
func loadFlightsFromCSV(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error al abrir archivo CSV: %v", err)
	}
	defer file.Close()

	var flightIDs []string
	r := csv.NewReader(file)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error al leer el archivo CSV: %v", err)
	}

	for i, row := range rows {
		if i == 0 || len(row) < 2 {
			continue
		}
		flightID := row[1]
		flightIDs = append(flightIDs, flightID)
	}

	return flightIDs, nil
}

// Obtener el estado de un vuelo
func (c *Client) getFlightStatus(client pb.CentralBrokerClient, clientID string) (bool, error) {
	req := &pb.FlightRequest{
		FlightID:    c.flightID,
		Lastversion: c.NuevaVersion,
		ClientId:    c.clientID,
	}
	fmt.Printf("Consultando vuelo %s, enviando versión cliente V%d...\n", c.flightID, c.NuevaVersion)

	res, err := client.GetFlightStatus(context.Background(), req)
	if err != nil {
		// El manejo de error y log se hace aquí, pero devolvemos el error para que el main decida si morir
		st, ok := status.FromError(err)
		if ok && (st.Code() == codes.NotFound || st.Code() == codes.Unavailable) {
			log.Printf("Advertencia: Vuelo %s no encontrado/desactualizado o Broker caído. Causa: %s\n",
				c.flightID, st.Message())
			return false, err
		}
		log.Printf("Advertencia: Error al consultar estado de vuelo (%s). Error fatal: %v\n", c.flightID, err)
		return false, err
	}

	fmt.Printf("Vuelo %s (V%d): Estado = %s, Puerta = %s\n", res.FlightID, res.Version, res.Status, res.Gate)
	if res.Version > c.NuevaVersion {
		c.NuevaVersion = res.Version
		fmt.Printf("-> Cliente %s actualizó su versión a V%d\n", c.flightID, c.NuevaVersion)
	}
	return true, nil
}

func main() {
	// OBTENER VARIABLES DE ENTORNO
	idPtr := flag.String("id", "RYW-Generic", "ID del Cliente para logs")
	flag.Parse()

	clientIDLog := fmt.Sprintf("[%s] ", *idPtr)
	log.SetPrefix(clientIDLog)
	brokerAddr := os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		log.Fatalf("La variable de entorno BROKER_ADDR no está definida.")
	}

	time.Sleep(11 * time.Second)

	// CONEXIÓN
	conn, err := grpc.Dial(brokerAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con el Broker en %s: %v", brokerAddr, err)
	}
	defer conn.Close()

	client := pb.NewCentralBrokerClient(conn)
	csvFilePath := "/app/data/flight_updates.csv"

	// Cargar vuelos desde el archivo CSV
	flightIDs, err := loadFlightsFromCSV(csvFilePath)
	if err != nil {
		log.Fatalf("Error al cargar vuelos desde el CSV (%s): %v", csvFilePath, err)
	}

	if len(flightIDs) == 0 {
		log.Fatalf("No se encontraron IDs de vuelo en el CSV.")
	}

	rand.Seed(time.Now().UnixNano())
	flightStates := make(map[string]*FlightState)
	for _, id := range flightIDs {
		flightStates[id] = &FlightState{VersionGuardada: 0}
	}

	keys := make([]string, 0, len(flightIDs))
	for k := range flightStates {
		keys = append(keys, k)
	}

	fmt.Printf("------- PASAJEROS MR INICIADO -------\n")

	// --- CONTADOR DE FALLOS ---
	consecutiveFailures := 0
	const maxFailures = 3

	for {
		// 1. Seleccionar un vuelo aleatorio
		dormir := rand.Intn(5) + 1
		time.Sleep(time.Duration(dormir) * time.Second)
		if len(keys) == 0 {
			log.Println("No hay IDs de vuelo para consultar. Saliendo.")
			break
		}
		randomIndex := rand.Intn(len(keys))
		selectedFlightID := keys[randomIndex]

		state := flightStates[selectedFlightID]

		// 2. Crear un cliente temporal
		tempClient := &Client{
			flightID:     selectedFlightID,
			NuevaVersion: state.VersionGuardada,
		}

		// 3. Intentar obtener el estado
		success, err := tempClient.getFlightStatus(client, *idPtr)

		// 4. Analizar el resultado y manejar contador de suicidio
		if success {
			// Conexión exitosa, reiniciamos contador
			consecutiveFailures = 0

			state.VersionGuardada = tempClient.NuevaVersion
			time.Sleep(500 * time.Millisecond)
		} else {
			// Hubo error, analizamos si es de conexión
			if err != nil {
				st, ok := status.FromError(err)
				// Si el error es UNAVAILABLE (Broker caído/apagado) aumentamos contador
				if ok && st.Code() == codes.Unavailable {
					consecutiveFailures++
					log.Printf("[ALERTA] Fallo de conexión con Broker detectado (%d/%d).", consecutiveFailures, maxFailures)
				} else {
					// Si el error es NotFound (el vuelo no existe aun) el Broker SÍ responde,
					// así que la conexión está viva. Reiniciamos contador.
					consecutiveFailures = 0
				}
			}

			// Verificamos si debemos suicidarnos
			if consecutiveFailures >= maxFailures {
				log.Println("[SHUTDOWN] El Broker no responde tras 3 intentos. Cerrando Cliente MR.")
				os.Exit(0) // Finaliza el programa
			}

			time.Sleep(2 * time.Second)
		}
	}
}
