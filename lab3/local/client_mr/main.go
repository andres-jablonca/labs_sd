package main

import (
	"context"
	"encoding/csv"
	"fmt"
	pb "lab3/proto" // Asegúrate de que esta importación esté correcta
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
	flightID    string
	lastVersion int64 // La versión vectorial (o contador escalar) vista por el cliente.
}

// ⭐️ NUEVA ESTRUCTURA: Para almacenar el estado de cada vuelo individual en la función main
type FlightState struct {
	LastVersion int64
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
		if i == 0 || len(row) < 2 { // Saltear la primera fila (encabezados) o si no hay suficientes columnas
			continue
		}
		flightID := row[1] // Suponiendo que el flightID está en la segunda columna
		flightIDs = append(flightIDs, flightID)
	}

	return flightIDs, nil
}

// Obtener el estado de un vuelo, implementando la lógica de Monotonic Reads
func (c *Client) getFlightStatus(client pb.CentralBrokerClient) (bool, error) {
	req := &pb.FlightRequest{
		FlightID:    c.flightID,
		Lastversion: c.lastVersion,
	}

	fmt.Printf("Consultando vuelo %s, enviando versión cliente V%d...\n", c.flightID, c.lastVersion)

	res, err := client.GetFlightStatus(context.Background(), req)
	if err != nil {
		// --- Manejo de errores para saltar vuelos no inicializados ---
		st, ok := status.FromError(err)
		if ok && (st.Code() == codes.NotFound || st.Code() == codes.Unavailable) {
			// El vuelo no está inicializado o la versión no está lista.
			log.Printf("Advertencia: Vuelo %s no encontrado/desactualizado (V%d). Reintentando otro vuelo. Causa: %s\n",
				c.flightID, c.lastVersion, st.Message())
			return false, err // Retornar fallo.
		}

		// Otros errores gRPC (ej. conexión)
		log.Printf("Advertencia: Error al consultar estado de vuelo (%s). Error fatal: %v\n", c.flightID, err)
		return false, err
	}

	// 2. Mostrar el resultado
	fmt.Printf("Vuelo %s (V%d): Estado = %s, Puerta = %s\n", res.FlightID, res.Version, res.Status, res.Gate)

	// 3. Actualizar la versión del cliente
	if res.Version > c.lastVersion {
		c.lastVersion = res.Version
		fmt.Printf("-> Cliente %s actualizó su versión a V%d\n", c.flightID, c.lastVersion)
	}

	// Retornar éxito
	return true, nil
}

func main() {
	// OBTENER VARIABLES DE ENTORNO
	brokerAddr := os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		log.Fatalf("❌ La variable de entorno BROKER_ADDR no está definida.")
	}

	// CONEXIÓN
	conn, err := grpc.Dial(brokerAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con el Broker en %s: %v", brokerAddr, err)
	}
	defer conn.Close()

	// CORRECCIÓN CLAVE: Usar el cliente CentralBroker
	client := pb.NewCentralBrokerClient(conn)

	// RUTA DEL CSV
	csvFilePath := "/app/data/flight_updates.csv"

	// Cargar vuelos desde el archivo CSV
	flightIDs, err := loadFlightsFromCSV(csvFilePath)
	if err != nil {
		log.Fatalf("Error al cargar vuelos desde el CSV (%s): %v", csvFilePath, err)
	}

	if len(flightIDs) == 0 {
		log.Fatalf("No se encontraron IDs de vuelo en el CSV.")
	}

	// ⭐️ NUEVA LÓGICA: Inicializar el estado de todos los vuelos
	rand.Seed(time.Now().UnixNano())
	flightStates := make(map[string]*FlightState)
	for _, id := range flightIDs {
		flightStates[id] = &FlightState{LastVersion: 0}
	}

	// Slice de IDs para elegir al azar (más fácil de indexar)
	keys := make([]string, 0, len(flightIDs))
	for k := range flightStates {
		keys = append(keys, k)
	}

	// 4. Bucle principal de consultas con rotación y manejo de errores
	fmt.Printf("--- Cliente Monotonic Reads Iniciado. Rastreando %d vuelos. ---\n", len(keys))

	for {
		// 1. Seleccionar un vuelo aleatorio
		if len(keys) == 0 {
			log.Println("No hay IDs de vuelo para consultar. Saliendo.")
			break
		}
		randomIndex := rand.Intn(len(keys))
		selectedFlightID := keys[randomIndex]

		state := flightStates[selectedFlightID]

		// 2. Crear un cliente temporal con el estado del vuelo seleccionado
		tempClient := &Client{
			flightID:    selectedFlightID,
			lastVersion: state.LastVersion,
		}

		// 3. Intentar obtener el estado
		success, _ := tempClient.getFlightStatus(client)

		// 4. Analizar el resultado
		if success {
			// Caso A: Éxito. Guardar el nuevo estado y rotar inmediatamente.
			// Esto cubre el requisito: "cuando el vuelo AA-901 reciba su versión V1 entonces que guarde el estado V1 de ese vuelo y tire rand para pedir información de otro vuelo"
			state.LastVersion = tempClient.lastVersion // Guarda la nueva versión vista
			time.Sleep(500 * time.Millisecond)         // Pequeña espera para no saturar
		} else {
			// Caso B: Fallo (NotFound/Unavailable). Rotar inmediatamente para evitar repetición fallida.
			// Esto cubre el requisito: "si es que escoge un id y este no ha sido cargado aún [...] entonces que escoja otro id"
			time.Sleep(2 * time.Second) // Espera más larga antes de intentar otro vuelo
		}
	}
}
