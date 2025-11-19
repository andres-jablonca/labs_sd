package main

import (
	"context"
	"encoding/csv" // Nuevo: Para leer el CSV
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "lab3/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// Ruta corregida a /app/data/flight_updates.csv según tu docker-compose.yml
	CSV_FILE_PATH = "/app/data/flight_updates.csv"
	BROKER_ID     = "BROKER" // ID para el Vector Clock
)

// BrokerServer implementa el servicio CentralBroker (para Coordinador)
// y el servicio de gestión de datos para Consistencia Eventual.
type BrokerServer struct {
	pb.UnimplementedCentralBrokerServer

	mu              sync.Mutex
	datanodeClients []pb.DatanodeServiceClient // Clientes gRPC a los Datanodes
	peersAddr       []string                   // Direcciones de los Datanodes (para Broadcast)

	// Mapa para rastrear el último Vector Clock conocido para cada vuelo
	// Key: FlightID (ej. LA-500) | Value: Vector Clock (map[string]int64)
	flightVCMaps map[string]map[string]int64
	rrIndex      int
}

// ----------------------------------------------------
// 1. LÓGICA DE BROADCAST (Simulación CSV)
// ----------------------------------------------------

// En broker/main.go

func (s *BrokerServer) startEventSimulation() {
	log.Printf("Simulador de Eventos: Iniciando lectura de %s...", CSV_FILE_PATH)

	file, err := os.Open(CSV_FILE_PATH)
	if err != nil {
		log.Fatalf("❌ ERROR: No se pudo abrir el CSV en %s: %v", CSV_FILE_PATH, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("❌ ERROR: No se pudo leer el CSV: %v", err)
	}

	// Saltar la cabecera del CSV
	if len(records) > 0 {
		records = records[1:]
	}

	startTime := time.Now()

	for _, record := range records {
		// Asumimos el formato: [sim_time_sec, flight_id, update_type, update_value]
		simTime, _ := time.ParseDuration(record[0] + "s") // Convertir "10" a 10s
		flightID := record[1]
		updateType := record[2]
		updateValue := record[3]

		// Esperar hasta el tiempo de simulación correcto
		timeToWait := startTime.Add(simTime).Sub(time.Now())
		if timeToWait > 0 {
			time.Sleep(timeToWait)
		}

		s.mu.Lock()

		// 1. Obtener/Inicializar el Vector Clock (VC)
		currentVC, found := s.flightVCMaps[flightID]
		if !found {
			currentVC = make(map[string]int64)
		}

		// 2. Incrementar la entrada del Broker
		currentVC[BROKER_ID]++
		s.flightVCMaps[flightID] = currentVC

		// 3. Preparar la actualización (Status Map)
		statusUpdate := map[string]string{
			updateType:  updateValue,
			"flight_id": flightID, // Incluir el ID de vuelo para que el DN sepa qué dato está resolviendo
		}

		// 4. Crear el Request con el VC y la data
		req := &pb.UpdateFlightStatusRequest{
			FlightId:    flightID,
			Status:      statusUpdate,
			VectorClock: currentVC,
		}

		s.mu.Unlock()

		// 5. Iniciar Broadcast a todos los Datanodes
		s.broadcastUpdate(req, updateType) // ✅ CORREGIDO: Se pasa updateType
	}

	log.Println("Simulador de Eventos: CSV completado. Fin de la inyección de datos.")
}

// En broker/main.go
// ...
// broadcastUpdate envía el request a todos los Datanodes.
func (s *BrokerServer) broadcastUpdate(req *pb.UpdateFlightStatusRequest, updateType string) { // Usaremos updateType para el log
	log.Printf("BROADCAST: Vuelo %s, Tipo: %s, VC: %v", req.FlightId, updateType, req.VectorClock)
	for i, client := range s.datanodeClients {
		// Aumentar el timeout de 1s a 5s para darle más tiempo al Datanode
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10) // <-- CORRECCIÓN AQUÍ

		// Llama al nuevo RPC del Datanode
		_, err := client.UpdateFlightStatus(ctx, req)
		if err != nil {
			log.Printf("❌ ERROR Broadcast a %s: %v", s.peersAddr[i], err)
		}
		cancel()
	}
}

// ----------------------------------------------------
// 2. SERVICIOS DEL BROKER (Para Coordinador)
// ----------------------------------------------------

// UpdateFlightData (RYW) y GetFlightData (Lectura) no necesitan ser modificados por ahora,
// ya que el Check-in (RYW) ya estaba funcional usando estas llamadas.

func (s *BrokerServer) UpdateFlightData(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	// ... Lógica Round-Robin y reenvío a Datanode (código que ya tenías)
	return &pb.UpdateResponse{Success: true, Message: "Placeholder: Escritura re-enviada."}, nil
}

func (s *BrokerServer) GetFlightData(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	// ... Lógica Round-Robin para lectura (código que ya tenías)
	return &pb.ReadResponse{FlightId: req.FlightId, SeatAssignedToClient: "Placeholder: 21A"}, nil
}

// ----------------------------------------------------
// 3. FUNCIONES DE INICIALIZACIÓN Y MAIN
// ----------------------------------------------------

func getListenPort() string {
	port := os.Getenv("LISTEN_PORT")
	if port == "" {
		return ":50052"
	}
	return port
}

func getDatanodeAddresses() []string {
	addrs := os.Getenv("DATANODE_ADDRS")
	if addrs == "" {
		// Valor por defecto en caso de fallo de ENV
		return []string{"datanode1:50061", "datanode2:50062", "datanode3:50063"}
	}
	return strings.Split(addrs, ",")
}

// initDatanodeClients inicializa la conexión gRPC con todos los Datanodes.
func initDatanodeClients(peers []string) []pb.DatanodeServiceClient {
	var clients []pb.DatanodeServiceClient
	for _, addr := range peers {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
		if err != nil {
			log.Fatalf("❌ ERROR: No se pudo conectar a Datanode %s: %v", addr, err)
		}
		// No hacemos defer conn.Close() aquí porque los clientes se usan en un loop (Broadcast)
		clients = append(clients, pb.NewDatanodeServiceClient(conn))
		log.Printf("✅ Broker conectado a Datanode en %s", addr)
	}
	return clients
}

// GetFlightStatus maneja las solicitudes de Monotonic Reads de los clientes.
func (s *BrokerServer) GetFlightStatus(ctx context.Context, req *pb.FlightRequest) (*pb.FlightResponse, error) {
	s.mu.Lock()

	// 1. Lógica de Round Robin
	// Seleccionamos un Datanode para delegar la lectura.
	targetIndex := s.rrIndex % len(s.datanodeClients)
	s.rrIndex++ // Incrementar el índice para la siguiente solicitud

	selectedDatanodeClient := s.datanodeClients[targetIndex]
	selectedDatanodeAddr := s.peersAddr[targetIndex]

	s.mu.Unlock()

	log.Printf("INFO: Broker delega lectura Monotonic de Vuelo %s (V%d) a Datanode %s",
		req.GetFlightID(), req.GetLastversion(), selectedDatanodeAddr)

	// 2. Delegar la solicitud al Datanode
	// Nota: Asumimos que el DatanodeService también implementa GetFlightStatus,
	// tal como lo definimos en aerodist.proto.
	res, err := selectedDatanodeClient.GetFlightStatus(ctx, req)

	if err != nil {
		log.Printf("ADVERTENCIA: Datanode %s falló al procesar GetFlightStatus: %v", selectedDatanodeAddr, err)
		return nil, err
	}

	// 3. Devolver la respuesta al cliente MR
	return res, nil
}

func main() {
	port := getListenPort()
	peers := getDatanodeAddresses()

	// Crear conexiones gRPC antes de iniciar el servidor
	datanodeClients := initDatanodeClients(peers)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("❌ ERROR: Falló al escuchar el puerto %s: %v", port, err)
	}

	s := grpc.NewServer()

	server := &BrokerServer{
		datanodeClients: datanodeClients,
		peersAddr:       peers,
		flightVCMaps:    make(map[string]map[string]int64),
	}

	pb.RegisterCentralBrokerServer(s, server)

	// 1. Iniciar el goroutine de Simulación de Eventos (Broadcast)
	go server.startEventSimulation()

	log.Printf("🚀 Broker Central escuchando en %s. Listo para rutear y broadcast.", port)

	// 2. Iniciar el servidor gRPC
	if err := s.Serve(lis); err != nil {
		log.Fatalf("falló al servir: %v", err)
	}
}
