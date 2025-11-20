package main

import (
	"context"
	"encoding/csv" // Nuevo: Para leer el CSV
	"log"
	"math/rand"
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
		log.Fatalf("❌ ERROR: No se pudo abrir el CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("❌ ERROR: No se pudo leer el CSV: %v", err)
	}
	if len(records) > 0 {
		records = records[1:]
	} // Saltar header

	startTime := time.Now()

	for _, record := range records {
		// Parsear tiempo
		simTime, _ := time.ParseDuration(record[0] + "s")
		flightID := record[1]
		updateType := record[2]
		updateValue := record[3]

		// Esperar tiempo de simulación
		if wait := startTime.Add(simTime).Sub(time.Now()); wait > 0 {
			time.Sleep(wait)
		}

		s.mu.Lock()

		// 1. Obtener VC actual
		currentVC, found := s.flightVCMaps[flightID]
		if !found {
			currentVC = make(map[string]int64)
		}

		// ---------------------------------------------------------
		// 🔥 EL CAMBIO CLAVE PARA LOGRAR EL DIAGRAMA 🔥
		// Simulamos 2 fuentes de escritura para crear bifurcaciones
		// en la historia causal (Vectores concurrentes).
		// ---------------------------------------------------------
		sourceID := "BROKER_A"
		if rand.Intn(2) == 0 {
			sourceID = "BROKER_B"
		}

		currentVC[sourceID]++
		s.flightVCMaps[flightID] = currentVC
		// ---------------------------------------------------------

		statusUpdate := map[string]string{
			updateType:  updateValue,
			"flight_id": flightID,
		}

		// Copia defensiva del VC para enviar
		vcToSend := make(map[string]int64)
		for k, v := range currentVC {
			vcToSend[k] = v
		}

		req := &pb.UpdateFlightStatusRequest{
			FlightId:    flightID,
			Status:      statusUpdate,
			VectorClock: vcToSend,
		}
		s.mu.Unlock()

		// Enviar a todos los nodos
		s.broadcastUpdate(req, updateType)
	}
	log.Println("✅ Simulador: CSV completado.")
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

// -----------------------------------------------------------------------
// FUNCIONES PARA RYW (Coordinador -> Broker -> Datanode)
// -----------------------------------------------------------------------

// UpdateFlightData: Maneja la ESCRITURA del Check-in.
// Modificación Clave: Ahora devuelve el 'DatanodeId' para que el Coordinador haga Sticky Session.
func (s *BrokerServer) UpdateFlightData(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	s.mu.Lock()

	// 1. Balanceo de Carga (Round Robin)
	targetIndex := s.rrIndex % len(s.datanodeClients)
	s.rrIndex++

	client := s.datanodeClients[targetIndex]
	targetAddr := s.peersAddr[targetIndex] // Ej: "datanode1:50061"

	// 2. Identificar el ID del Datanode para devolverlo al Coordinador
	// Esto es vital para que funcione el Sticky Session.
	datanodeID := "DN-UNK"
	if strings.Contains(targetAddr, "datanode1") {
		datanodeID = "DN-1"
	} else if strings.Contains(targetAddr, "datanode2") {
		datanodeID = "DN-2"
	} else if strings.Contains(targetAddr, "datanode3") {
		datanodeID = "DN-3"
	}

	s.mu.Unlock()

	log.Printf("⚖️ Broker: Redirigiendo Check-in (RYW) de Cliente %s al Datanode %s (%s)", req.ClientId, datanodeID, targetAddr)

	// 3. Llamar al Datanode real (ApplyWrite)
	// El Datanode guardará el asiento en su mapa 'rywState'
	dnResp, err := client.ApplyWrite(ctx, req)
	if err != nil {
		log.Printf("❌ Error escribiendo en Datanode %s: %v", datanodeID, err)
		return &pb.UpdateResponse{Success: false, Message: "Fallo escritura en DN"}, err
	}

	// 4. Responder al Coordinador con el ID del nodo que hizo el trabajo
	return &pb.UpdateResponse{
		Success:    dnResp.Success,
		Message:    dnResp.Message,
		DatanodeId: datanodeID, // <--- ¡ESTO HABILITA EL STICKY SESSION!
	}, nil
}

// GetFlightData: Maneja la LECTURA de asientos (Fallback si falla el Sticky Read).
// Nota: Esto es distinto a GetFlightStatus (que es para ver si el vuelo está retrasado).
func (s *BrokerServer) GetFlightData(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.mu.Lock()
	// Round Robin simple
	targetIndex := s.rrIndex % len(s.datanodeClients)
	s.rrIndex++

	client := s.datanodeClients[targetIndex]
	targetAddr := s.peersAddr[targetIndex]
	s.mu.Unlock()

	log.Printf("🔄 Broker: Redirigiendo lectura de asiento (Fallback) de %s a %s", req.ClientId, targetAddr)

	// Llamada directa al Datanode (ReadData)
	return client.ReadData(ctx, req)
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
