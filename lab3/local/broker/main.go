package main

import (
	"context"
	"encoding/csv"
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
	CSV_FILE_PATH = "/app/data/flight_updates.csv"
)

type BrokerServer struct {
	pb.UnimplementedCentralBrokerServer

	mu              sync.Mutex
	datanodeClients []pb.DatanodeServiceClient
	peersAddr       []string

	flightVCMaps map[string]map[string]int64
	rrIndex      int
}

// ----------------------------------------------------
// 1. LÓGICA DE SIMULACIÓN (CSV)
// ----------------------------------------------------

func (s *BrokerServer) startEventSimulation() {
	log.Printf("[INIT] Simulador de Eventos: Iniciando lectura de %s...", CSV_FILE_PATH)

	file, err := os.Open(CSV_FILE_PATH)
	if err != nil {
		log.Fatalf("[FATAL] No se pudo abrir el CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("[FATAL] No se pudo leer el CSV: %v", err)
	}
	if len(records) > 0 {
		records = records[1:]
	}

	startTime := time.Now()

	for _, record := range records {
		simTime, _ := time.ParseDuration(record[0] + "s")
		flightID := record[1]
		updateType := record[2]
		updateValue := record[3]

		if wait := startTime.Add(simTime).Sub(time.Now()); wait > 0 {
			time.Sleep(wait)
		}

		s.mu.Lock()

		numPeers := len(s.datanodeClients)
		if numPeers == 0 {
			s.mu.Unlock()
			log.Println("[WARN] No hay Datanodes conectados para enviar actualización.")
			continue
		}

		// 1. ELEGIR DESTINO PRIMERO
		// Elegimos a qué nodo vamos a enviar el dato (0=DN1, 1=DN2, 2=DN3)
		targetIndex := rand.Intn(numPeers)

		// 2. ACTUALIZAR VECTOR CLOCK SEGÚN EL DESTINO
		// Si va a DN1 -> Aumenta "A"
		// Si va a DN2 -> Aumenta "B"
		// Si va a DN3 -> Aumenta "C"
		var clockEntity string
		switch targetIndex {
		case 0:
			clockEntity = "A" // Datanode 1
		case 1:
			clockEntity = "B" // Datanode 2
		case 2:
			clockEntity = "C" // Datanode 3
		default:
			clockEntity = "X" // Fallback
		}

		currentVC, found := s.flightVCMaps[flightID]
		if !found {
			currentVC = make(map[string]int64)
			// Inicializamos en 0 para que se vea bonito en el log [A:0 B:0 C:0]
			currentVC["A"] = 0
			currentVC["B"] = 0
			currentVC["C"] = 0
		}

		// Aumentar el reloj de la entidad correspondiente
		currentVC[clockEntity]++
		s.flightVCMaps[flightID] = currentVC

		statusUpdate := map[string]string{
			updateType:  updateValue,
			"flight_id": flightID,
		}

		// Copia defensiva para envío gRPC
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

		// 3. ENVIAR ESPECÍFICAMENTE AL NODO ELEGIDO
		s.dispatchToSpecificNode(targetIndex, req, updateType)
	}
	log.Println("[INFO] Simulador: CSV completado.")
}

// dispatchToSpecificNode envía el request a un nodo específico por índice.
func (s *BrokerServer) dispatchToSpecificNode(index int, req *pb.UpdateFlightStatusRequest, updateType string) {
	s.mu.Lock()
	// Doble chequeo de seguridad
	if index >= len(s.datanodeClients) {
		s.mu.Unlock()
		return
	}
	client := s.datanodeClients[index]
	targetAddr := s.peersAddr[index]
	s.mu.Unlock()

	log.Printf("[DISPATCH] Enviando %s (Vuelo %s) a %s. Vector: %v",
		updateType, req.FlightId, targetAddr, req.VectorClock)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	_, err := client.UpdateFlightStatus(ctx, req)
	if err != nil {
		log.Printf("[ERROR] Fallo al enviar a %s: %v", targetAddr, err)
	}
}

// ----------------------------------------------------
// 2. SERVICIOS DEL BROKER
// ----------------------------------------------------

func (s *BrokerServer) UpdateFlightData(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	s.mu.Lock()

	if len(s.datanodeClients) == 0 {
		s.mu.Unlock()
		return &pb.UpdateResponse{Success: false, Message: "No hay Datanodes disponibles"}, nil
	}

	targetIndex := s.rrIndex % len(s.datanodeClients)
	s.rrIndex++

	client := s.datanodeClients[targetIndex]
	targetAddr := s.peersAddr[targetIndex]

	datanodeID := "DN-UNK"
	if strings.Contains(targetAddr, "datanode1") {
		datanodeID = "DN-1"
	} else if strings.Contains(targetAddr, "datanode2") {
		datanodeID = "DN-2"
	} else if strings.Contains(targetAddr, "datanode3") {
		datanodeID = "DN-3"
	}
	s.mu.Unlock()

	log.Printf("[RYW] Broker: Redirigiendo Check-in de Cliente %s al Datanode %s (%s)", req.ClientId, datanodeID, targetAddr)

	dnResp, err := client.ApplyWrite(ctx, req)
	if err != nil {
		log.Printf("[ERROR] Fallo escritura en Datanode %s: %v", datanodeID, err)
		return &pb.UpdateResponse{Success: false, Message: "Fallo escritura en DN"}, err
	}

	return &pb.UpdateResponse{
		Success:    dnResp.Success,
		Message:    dnResp.Message,
		DatanodeId: datanodeID,
	}, nil
}

func (s *BrokerServer) GetFlightData(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.mu.Lock()
	if len(s.datanodeClients) == 0 {
		s.mu.Unlock()
		return nil, grpc.Errorf(grpc.Code(nil), "No Datanodes available")
	}
	targetIndex := s.rrIndex % len(s.datanodeClients)
	s.rrIndex++
	client := s.datanodeClients[targetIndex]
	targetAddr := s.peersAddr[targetIndex]
	s.mu.Unlock()

	log.Printf("[READ] Broker: Redirigiendo lectura de asiento (Fallback) de %s a %s", req.ClientId, targetAddr)
	return client.ReadData(ctx, req)
}

// ----------------------------------------------------
// 3. SETUP Y MAIN
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
		return []string{"datanode1:50061", "datanode2:50062", "datanode3:50063"}
	}
	return strings.Split(addrs, ",")
}

func initDatanodeClients(peers []string) []pb.DatanodeServiceClient {
	var clients []pb.DatanodeServiceClient
	for _, addr := range peers {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
		if err != nil {
			log.Fatalf("[FATAL] No se pudo conectar a Datanode %s: %v", addr, err)
		}
		clients = append(clients, pb.NewDatanodeServiceClient(conn))
		log.Printf("[INIT] Broker conectado a Datanode en %s", addr)
	}
	return clients
}

func (s *BrokerServer) GetFlightStatus(ctx context.Context, req *pb.FlightRequest) (*pb.FlightResponse, error) {
	s.mu.Lock()
	if len(s.datanodeClients) == 0 {
		s.mu.Unlock()
		return nil, grpc.Errorf(grpc.Code(nil), "No Datanodes available")
	}
	targetIndex := s.rrIndex % len(s.datanodeClients)
	s.rrIndex++
	selectedDatanodeClient := s.datanodeClients[targetIndex]
	selectedDatanodeAddr := s.peersAddr[targetIndex]
	s.mu.Unlock()

	log.Printf("[MONOTONIC] Broker delega lectura de Vuelo %s (V%d) a Datanode %s",
		req.GetFlightID(), req.GetLastversion(), selectedDatanodeAddr)

	return selectedDatanodeClient.GetFlightStatus(ctx, req)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	port := getListenPort()
	peers := getDatanodeAddresses()
	datanodeClients := initDatanodeClients(peers)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("[FATAL] Falló al escuchar el puerto %s: %v", port, err)
	}

	s := grpc.NewServer()
	server := &BrokerServer{
		datanodeClients: datanodeClients,
		peersAddr:       peers,
		flightVCMaps:    make(map[string]map[string]int64),
	}

	pb.RegisterCentralBrokerServer(s, server)

	go server.startEventSimulation()

	log.Printf("[INIT] Broker Central escuchando en %s. Modelo: Inyección A/B/C.", port)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("[FATAL] Falló al servir: %v", err)
	}
}
