package main

// broker.go
import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal" // Importante: Para capturar Ctrl+C
	"strings"
	"sync"
	"syscall" // Importante: Para definir las señales
	"time"

	pb "lab3/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	CSV_FILE_PATH = "/app/data/flight_updates.csv"
	REPORT_FILE   = "/app/output/Reporte.txt"
)

type BrokerServer struct {
	pb.UnimplementedCentralBrokerServer

	mu              sync.Mutex
	datanodeClients []pb.DatanodeServiceClient
	peersAddr       []string

	flightVCMaps map[string]map[string]int64
	rrIndex      int

	// --- ESTADÍSTICAS GLOBALES (Movidas al struct) ---
	totalEvents    int
	eventsByType   map[string]int
	eventsByFlight map[string]int
	// -------------------------------------------------
	mrTotalQueries   map[string]int
	mrSkippedQueries map[string]int // Consultas que devolvieron "Dato desactualizado" o "No encontrado"
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

		// --- ACTUALIZAR ESTADÍSTICAS EN TIEMPO REAL ---
		s.totalEvents++
		s.eventsByType[updateType]++
		s.eventsByFlight[flightID]++
		// ----------------------------------------------

		numPeers := len(s.datanodeClients)
		if numPeers == 0 {
			s.mu.Unlock()
			log.Println("[WARN] No hay Datanodes conectados para enviar actualización.")
			continue
		}

		// 1. ELEGIR DESTINO
		targetIndex := rand.Intn(numPeers)

		// 2. ACTUALIZAR VECTOR CLOCK
		var clockEntity string
		switch targetIndex {
		case 0:
			clockEntity = "A" // Datanode 1
		case 1:
			clockEntity = "B" // Datanode 2
		case 2:
			clockEntity = "C" // Datanode 3
		default:
			clockEntity = "X"
		}

		currentVC, found := s.flightVCMaps[flightID]
		if !found {
			currentVC = make(map[string]int64)
			currentVC["A"] = 0
			currentVC["B"] = 0
			currentVC["C"] = 0
		}

		currentVC[clockEntity]++
		s.flightVCMaps[flightID] = currentVC

		statusUpdate := map[string]string{
			updateType:  updateValue,
			"flight_id": flightID,
		}

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

		// 3. ENVIAR
		s.dispatchToSpecificNode(targetIndex, req, updateType)
	}
	log.Println("[INFO] Simulador: CSV completado.")
	time.Sleep(20 * time.Second)

	log.Println("[SHUTDOWN] Enviando señal de terminación a los Datanodes...")

	// Recorremos todos los clientes conectados
	for i, client := range s.datanodeClients {
		// Usamos un contexto rápido (1 segundo) por si un nodo ya se cayó no quedarnos pegados
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

		// Intentamos apagarlo
		_, err := client.Shutdown(ctx, &pb.NoParams{})
		if err != nil {
			log.Printf("[WARN] No se pudo apagar el Datanode #%d: %v", i+1, err)
		} else {
			log.Printf("[SHUTDOWN] Datanode #%d notificado correctamente.", i+1)
		}
		cancel()
	}

	log.Println("[SHUTDOWN] Intentando apagar al Coordinador...")
	coordinatorAddr := "coordinator:50055" // O carga esto de una variable de entorno
	connCoord, err := grpc.Dial(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		coordClient := pb.NewCheckInCoordinatorClient(connCoord)
		// Contexto corto de 1s
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		_, err := coordClient.Shutdown(ctx, &pb.NoParams{})
		if err != nil {
			log.Printf("[WARN] No se pudo contactar al Coordinador para apagarlo: %v", err)
		} else {
			log.Println("[SHUTDOWN] Orden enviada al Coordinador.")
		}
		connCoord.Close()
	}

	// ---------------------------------------------------------

	log.Println("[SHUTDOWN] Finalizando ejecución del Broker por término de CSV...")
	s.generateReport()
	time.Sleep(2 * time.Second)
	os.Exit(0)
}

// generateReport ahora lee del struct
func (s *BrokerServer) generateReport() {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Create(REPORT_FILE)
	if err != nil {
		log.Printf("[ERROR] No se pudo crear el archivo %s: %v", REPORT_FILE, err)
		return
	}
	defer f.Close()

	loc := time.FixedZone("CLT", -3*60*60)
	fechaFormateada := time.Now().In(loc).Format("2006-01-02 / 15:04:05")

	fmt.Fprintln(f, "--------------------------------------------------")
	fmt.Fprintln(f, "          REPORTE FINAL - AERODIST BROKER         ")
	fmt.Fprintln(f, "--------------------------------------------------")
	fmt.Fprintf(f, "Fecha de generación: %s\n", fechaFormateada)
	fmt.Fprintf(f, "Estado de finalización: %s\n\n", "Finalizado")

	fmt.Fprintf(f, "Total de eventos procesados (Writes): %d\n", s.totalEvents)
	fmt.Fprintln(f, "")

	fmt.Fprintln(f, "--- Desglose por Tipo de Actualización ---")
	for k, v := range s.eventsByType {
		fmt.Fprintf(f, "- %s: %d\n", k, v)
	}
	fmt.Fprintln(f, "")

	fmt.Fprintln(f, "--- Desglose por Vuelo (Top Activity) ---")
	for k, v := range s.eventsByFlight {
		fmt.Fprintf(f, "- Vuelo %s: %d actualizaciones\n", k, v)
	}
	fmt.Fprintln(f, "")

	fmt.Fprintln(f, "--- Estado Final de Relojes Vectoriales ---")
	for flightID, vc := range s.flightVCMaps {
		fmt.Fprintf(f, "- %s: %v\n", flightID, vc)
	}
	fmt.Fprintln(f, "")

	// NUEVA SECCIÓN: ESTADÍSTICAS CLIENTES MR
	fmt.Fprintln(f, "--- Estadísticas de Clientes Monotonic Reads ---")
	if len(s.mrTotalQueries) == 0 {
		fmt.Fprintln(f, "(No se registraron consultas MR)")
	} else {
		i := 1
		for clientAddr, total := range s.mrTotalQueries {
			skipped := s.mrSkippedQueries[clientAddr]
			fmt.Fprintf(f, "Cliente #%d (%s):\n", i, clientAddr)
			fmt.Fprintf(f, "   - Total Consultas: %d\n", total)
			fmt.Fprintf(f, "   - Consultas Fallidas/Espera: %d\n", skipped)
			fmt.Fprintf(f, "   - Consultas Exitosas: %d\n", total-skipped)
			i++
		}
	}

	fmt.Fprintln(f, "--------------------------------------------------")

	log.Printf("[INFO] Reporte generado exitosamente en %s", REPORT_FILE)
}

func (s *BrokerServer) dispatchToSpecificNode(index int, req *pb.UpdateFlightStatusRequest, updateType string) {
	s.mu.Lock()
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
// 2. SERVICIOS DEL BROKER (Sin cambios)
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

func (s *BrokerServer) GetFlightStatus(ctx context.Context, req *pb.FlightRequest) (*pb.FlightResponse, error) {
	s.mu.Lock()

	// 1. Obtener ID del mensaje (ya no usamos peer)
	clientID := req.ClientId // Viene del Proto
	if clientID == "" {
		clientID = "Anónimo"
	}

	// --- REGISTRAR ESTADÍSTICA ---
	if s.mrTotalQueries == nil {
		s.mrTotalQueries = make(map[string]int)
	}
	s.mrTotalQueries[clientID]++

	// Validación básica de datanodes
	if len(s.datanodeClients) == 0 {
		s.mu.Unlock()
		return nil, grpc.Errorf(grpc.Code(nil), "No Datanodes available")
	}

	// Round Robin
	targetIndex := s.rrIndex % len(s.datanodeClients)
	s.rrIndex++
	selectedDatanodeClient := s.datanodeClients[targetIndex]
	selectedDatanodeAddr := s.peersAddr[targetIndex]
	s.mu.Unlock()

	log.Printf("[MONOTONIC] Cliente %s consulta Vuelo %s (V%d) -> Delegado a %s",
		clientID, req.GetFlightID(), req.GetLastversion(), selectedDatanodeAddr)

	// 2. Llamada al Datanode
	resp, err := selectedDatanodeClient.GetFlightStatus(ctx, req)

	// 3. Registrar fallo si ocurre
	if err != nil {
		s.mu.Lock()
		if s.mrSkippedQueries == nil {
			s.mrSkippedQueries = make(map[string]int)
		}
		s.mrSkippedQueries[clientID]++
		s.mu.Unlock()
	}

	return resp, err
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

	// Inicializamos el servidor con los mapas vacíos para evitar Panic
	server := &BrokerServer{
		datanodeClients:  datanodeClients,
		peersAddr:        peers,
		flightVCMaps:     make(map[string]map[string]int64),
		eventsByType:     make(map[string]int),
		eventsByFlight:   make(map[string]int),
		mrTotalQueries:   make(map[string]int),
		mrSkippedQueries: make(map[string]int),
	}

	pb.RegisterCentralBrokerServer(s, server)

	// --- MANEJO DE SEÑALES (CTRL+C) ---
	// Creamos un canal para escuchar señales del sistema
	stopChan := make(chan os.Signal, 1)
	// Notificamos al canal si llega Ctrl+C (Interrupt) o SIGTERM (Docker stop)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		// Esperamos a que llegue una señal
		sig := <-stopChan
		log.Printf("[INFO] Señal recibida: %v. Generando reporte y apagando...", sig)

		// Generamos el reporte antes de morir
		server.generateReport()

		// Salimos exitosamente
		os.Exit(0)
	}()
	// ----------------------------------

	go server.startEventSimulation()

	log.Printf("[INIT] Broker Central escuchando en %s. Modelo: Inyección A/B/C.", port)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("[FATAL] Falló al servir: %v", err)
	}
}
