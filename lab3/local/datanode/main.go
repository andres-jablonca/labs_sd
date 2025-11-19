package main

import (
	"context"
	"log"
	"math/rand" // NUEVO: Para seleccionar peers aleatoriamente
	"net"
	"os"
	"strings"
	"sync"
	"time" // NUEVO: Para el loop de Gossip

	pb "lab3/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure" // NUEVO: Necesario para dial al peer
	"google.golang.org/grpc/status"
)

// Constantes para la red y peers
const (
	GOSSIP_INTERVAL = 3 * time.Second
)

// Estructura para almacenar el estado de un vuelo junto con su Vector Clock (VC).
type FlightData struct {
	Status map[string]string // Ej: {"estado": "Retrasado", "puerta": "A2", ...}
	VC     map[string]int64  // Vector Clock asociado a esta versión del dato (Ej: {"DN-1": 5, "DN-2": 2})
}

// DatanodeServer implementa un servicio (que el Broker llama) y almacena los datos.
type DatanodeServer struct {
	pb.UnimplementedDatanodeServiceServer

	// Data para RYW (Asignación de asientos - Key: FlightId:SeatNumber, Value: ClientId)
	rywState map[string]string

	// Data para Consistencia Eventual (Estado de vuelos - Key: FlightId, Value: FlightData)
	flightData map[string]FlightData

	mu sync.Mutex
	id string // ID del Datanode (DN-1, DN-2, etc.)

	// Configuración de red para Gossip
	peers []string // Lista de direcciones de otros Datanodes (Ej: "datanode2:50062")
}

// ----------------------------------------------------
// RYW/CHECK-IN (Mantenido, usando rywState)
// ----------------------------------------------------

// ApplyWrite maneja la asignación de asiento (Consistencia RYW).
func (s *DatanodeServer) ApplyWrite(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := req.FlightId + ":" + req.SeatNumber
	if _, occupied := s.rywState[key]; occupied {
		return &pb.UpdateResponse{Success: false, Message: "Asiento ya ocupado."}, nil
	}
	s.rywState[key] = req.ClientId

	log.Printf("Datanode %s: ESCRITURA RYW: Asiento %s asignado a %s en vuelo %s.", s.id, req.SeatNumber, req.ClientId, req.FlightId)

	return &pb.UpdateResponse{Success: true, Message: "Escritura aplicada."}, nil
}

// ReadData maneja la lectura de asiento (Consistencia RYW).
func (s *DatanodeServer) ReadData(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var assignedSeat string
	for seatKey, clientID := range s.rywState {
		if clientID == req.ClientId {
			parts := strings.Split(seatKey, ":")
			if len(parts) == 2 {
				assignedSeat = parts[1]
			}
			break
		}
	}

	log.Printf("Datanode %s: LECTURA RYW: Devolviendo estado para %s (Asiento: %s).", s.id, req.ClientId, assignedSeat)

	return &pb.ReadResponse{
		FlightId:             req.FlightId,
		SeatAssignedToClient: assignedSeat,
	}, nil
}

// ----------------------------------------------------
// CONSISTENCIA EVENTUAL (ACTUALIZACIONES DE VUELO)
// ----------------------------------------------------

// MergeVC fusiona dos Vector Clocks, tomando el máximo de cada componente.
func MergeVC(vc1, vc2 map[string]int64) map[string]int64 {
	merged := make(map[string]int64)
	// Inicializar con vc1
	for id, count := range vc1 {
		merged[id] = count
	}
	// Fusionar con vc2 (tomando el máximo)
	for id, count := range vc2 {
		if count > merged[id] {
			merged[id] = count
		}
	}
	return merged
}

// isCausallyPrior verifica si vc1 es causalmente anterior o igual a vc2.
func isCausallyPrior(vc1, vc2 map[string]int64) bool {
	for id, count1 := range vc1 {
		// Si hay un componente en vc1 que es mayor que el mismo componente en vc2, no es anterior.
		if count1 > vc2[id] {
			return false
		}
	}
	return true
}

// ResolveConflict aplica la política de resolución determinista.
// Por simplicidad, implementamos una política de fusión de estados simple y fusionamos los VCs.
func (s *DatanodeServer) ResolveConflict(existingData, newData FlightData) FlightData {
	log.Printf("Datanode %s: ⚠️ CONFLICTO DETECTADO para %s. Aplicando política de fusión.", s.id, newData.Status["flight_id"])

	// Fusionar estados (la última actualización recibida para un campo sobrescribe, es una simplificación)
	for k, v := range newData.Status {
		existingData.Status[k] = v
	}

	// El nuevo VC es la fusión de ambos VCs
	existingData.VC = MergeVC(existingData.VC, newData.VC)

	return existingData
}

// GetFlightStatus implementa la lógica de Monotonic Reads.
// Sólo devuelve el dato si su versión es >= a la versión que el cliente vio por última vez.
func (s *DatanodeServer) GetFlightStatus(ctx context.Context, req *pb.FlightRequest) (*pb.FlightResponse, error) {
	// Bloqueamos la data para lectura concurrente
	s.mu.Lock()
	defer s.mu.Unlock()

	flightID := req.GetFlightID()
	clientVersion := req.GetLastversion() // Versión escalar del cliente

	currentData, found := s.flightData[flightID]

	if !found {
		// Si el vuelo no se encuentra (aún no se ha recibido la actualización), devolvemos error.
		log.Printf("ADVERTENCIA: Vuelo %s no encontrado en Datanode %s. Reintentar.", flightID, s.id)
		return nil, status.Errorf(codes.NotFound, "Vuelo %s no encontrado.", flightID)
	}

	// Extraer la versión escalar del dato local.
	// ASUMIMOS que la versión escalar se rastrea con el componente "BROKER" en el Vector Clock.
	currentVersion, ok := currentData.VC["BROKER"]
	if !ok {
		// Si el componente 'BROKER' no existe, asumimos V0.
		currentVersion = 0
	}

	// =====================================================================
	// 1. LÓGICA DE MONOTONIC READS (Monotonicidad)
	// =====================================================================

	if currentVersion < clientVersion {
		// El dato local (V%d) es causalmente anterior al dato que el cliente ya vio (V%d).
		// NO debemos responder, devolvemos un error para que el cliente reintente (esperando el Gossip).
		log.Printf("ADVERTENCIA: Monotonic Read de %s falló. Datanode %s (V%d) está desactualizado respecto al cliente (V%d).",
			flightID, s.id, currentVersion, clientVersion)

		// Usamos codes.Unavailable para indicar que el recurso (la versión) no está listo.
		return nil, status.Errorf(codes.Unavailable, "Versión de vuelo (V%d) es inferior a la vista por el cliente (V%d). Reintente.",
			currentVersion, clientVersion)
	}

	// 2. Si currentVersion >= clientVersion, la lectura es segura y Monotónica.
	log.Printf("INFO: Monotonic Read de %s SATISFECHO en Datanode %s. Local V%d >= Cliente V%d.", flightID, s.id, currentVersion, clientVersion)

	// Construir y devolver la respuesta.
	// Asumimos que la actualización de estado contiene los campos "estado" y "puerta".
	res := &pb.FlightResponse{
		FlightID: flightID,
		// Debes mapear los campos del mapa Status a los campos del FlightResponse
		Status:  currentData.Status["estado"],
		Gate:    currentData.Status["puerta"],
		Version: currentVersion, // Devolver la versión del dato que se acaba de leer.
	}
	return res, nil
}

// UpdateFlightStatus es el RPC para recibir actualizaciones del Broker (CSV) o de otros Datanodes (Gossip).
func (s *DatanodeServer) UpdateFlightStatus(ctx context.Context, req *pb.UpdateFlightStatusRequest) (*pb.UpdateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	flightID := req.FlightId
	incomingData := FlightData{
		Status: req.Status,
		VC:     req.VectorClock,
	}

	existingData, found := s.flightData[flightID]

	// En datanode/main.go, dentro de UpdateFlightStatus:

	// ...

	if !found {
		// Nuevo dato: Simplemente almacenar el dato entrante con su VC.
		// NO se debe incrementar el VC del Datanode (s.id) al recibir una actualización.
		s.flightData[flightID] = incomingData

		// El Datanode debe asegurarse de que su propio ID exista en el VC (con valor 0, si no ha escrito nada)
		if incomingData.VC[s.id] == 0 {
			incomingData.VC[s.id] = 0 // Inicializar a 0 si no existe
		}

		log.Printf("Datanode %s: ALMACENADO Inicial de %s. VC: %v", s.id, flightID, incomingData.VC)
		return &pb.UpdateResponse{Success: true, Message: "Escritura aplicada (Nuevo dato)."}, nil
	}

	// ...

	vcLocal := existingData.VC
	vcIncoming := incomingData.VC

	isPrior := isCausallyPrior(vcLocal, vcIncoming)
	isDescendant := isCausallyPrior(vcIncoming, vcLocal)

	if isPrior {
		// Caso 1: Data entrante es más reciente o igual. Aceptar y actualizar.
		s.flightData[flightID] = incomingData
		//log.Printf("Datanode %s: ALMACENADO Causal de %s. Data entrante es más reciente. Nuevo VC: %v", s.id, flightID, incomingData.VC)

	} else if isDescendant {
		// Caso 2: Data entrante es desactualizada (Stale). Descartar.
		log.Printf("Datanode %s: DESCARTADO de %s. Data entrante es desactualizada. VC Local: %v", s.id, flightID, vcLocal)

	} else {
		// Caso 3: CONFLICTO (VCs concurrentes). Aplicar resolución.
		resolvedData := s.ResolveConflict(existingData, incomingData)
		s.flightData[flightID] = resolvedData
		log.Printf("Datanode %s: RESOLUCIÓN de CONFLICTO para %s. Fusionado VC: %v", s.id, flightID, resolvedData.VC)
	}

	return &pb.UpdateResponse{Success: true, Message: "Actualización de estado procesada."}, nil
}

// ----------------------------------------------------
// GOSSIP: Sincronización entre pares
// ----------------------------------------------------

func (s *DatanodeServer) gossipLoop() {
	// Inicializar la semilla de rand
	rand.Seed(time.Now().UnixNano())

	ticker := time.NewTicker(GOSSIP_INTERVAL)
	defer ticker.Stop()

	for range ticker.C {
		s.sendGossip()
	}
}

func (s *DatanodeServer) sendGossip() {

	// 1. ADQUIRIR LOCK, tomar un SNAPSHOT de la data y seleccionar el peer.
	s.mu.Lock()

	// Crear una copia profunda (Deep Copy/Snapshot) de la data de vuelos
	dataSnapshot := make(map[string]FlightData)
	for flightID, data := range s.flightData {
		// Copiar el VC y el Status (mapas)
		vcCopy := make(map[string]int64)
		for k, v := range data.VC {
			vcCopy[k] = v
		}
		statusCopy := make(map[string]string)
		for k, v := range data.Status {
			statusCopy[k] = v
		}
		dataSnapshot[flightID] = FlightData{
			Status: statusCopy,
			VC:     vcCopy,
		}
	}

	// Seleccionar un Datanode vecino aleatorio
	var availablePeers []string
	for _, p := range s.peers {
		// Asegura no seleccionarse a sí mismo como peer
		if !strings.Contains(p, s.id) {
			availablePeers = append(availablePeers, p)
		}
	}

	if len(availablePeers) == 0 {
		s.mu.Unlock()
		return
	}

	// Declaración y asignación de targetPeer
	targetPeer := availablePeers[rand.Intn(len(availablePeers))]

	// 2. LIBERAR el lock inmediatamente antes de cualquier operación de red.
	s.mu.Unlock()

	// Operaciones de red (gRPC) fuera del lock

	log.Printf("Gossip: Iniciando sincronización con peer %s...", targetPeer)

	// 3. Crear conexión gRPC con el peer. conn se declara aquí.
	// Usamos el import "google.golang.org/grpc/credentials/insecure" que ahora sí es necesario.
	conn, err := grpc.Dial(targetPeer, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithTimeout(2*time.Second))
	if err != nil {
		log.Printf("Gossip: Falló la conexión con %s: %v", targetPeer, err)
		return
	}
	defer conn.Close()

	client := pb.NewDatanodeServiceClient(conn)

	// 4. Enviar la data usando el SNAPSHOT COPIADO
	for flightID, data := range dataSnapshot {
		// Crear el request con el VC y la data del snapshot
		req := &pb.UpdateFlightStatusRequest{
			FlightId:    flightID,
			Status:      data.Status,
			VectorClock: data.VC,
		}

		// Timeout corto para Gossip (ej. 2 segundos)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		// Descartamos el error ya que Gossip es 'best-effort'
		_, _ = client.UpdateFlightStatus(ctx, req)
		cancel()
	}
	log.Printf("Gossip: Finalizada sincronización con %s.", targetPeer)
}

// ----------------------------------------------------
// FUNCIONES DE UTILIDAD PARA DOCKER
// ----------------------------------------------------

func getListenPort() string {
	port := os.Getenv("LISTEN_PORT")
	if port == "" {
		return ":50061"
	}
	return port
}

func getDatanodeId() string {
	id := os.Getenv("DATANODE_ID")
	if id == "" {
		return "DN-X"
	}
	return id
}

// getPeers define las direcciones internas de los otros Datanodes en Docker Compose.
func getPeers(currentID string) []string {
	// Usamos los nombres de servicio de Docker Compose (datanodeX) + el puerto de escucha.
	// Esto debe coincidir con la configuración en docker-compose.yml.
	allPeers := []string{
		"datanode1:50061",
		"datanode2:50062",
		"datanode3:50063",
	}

	return allPeers
}

// ----------------------------------------------------
// FUNCION MAIN - PUNTO DE ENTRADA NECESARIO
// ----------------------------------------------------
func main() {

	datanodeID := getDatanodeId()
	port := getListenPort()
	peers := getPeers(datanodeID)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("❌ ERROR: Falló al escuchar el puerto %s: %v", port, err)
	}

	s := grpc.NewServer()

	// Inicialización del servidor con la nueva estructura
	server := &DatanodeServer{
		rywState:   make(map[string]string),
		flightData: make(map[string]FlightData),
		id:         datanodeID,
		peers:      peers,
	}

	pb.RegisterDatanodeServiceServer(s, server) // Asumiendo que el nuevo RPC está en este servicio

	// 1. Iniciar el goroutine de Gossip (Consistencia Eventual)
	go server.gossipLoop()
	//log.Printf("Gossip: Iniciado con intervalo de %v. Peers: %v", GOSSIP_INTERVAL, peers)

	log.Printf("🚀 Datanode %s escuchando en %s", datanodeID, port)

	// 2. Inicia el servidor gRPC
	if err := s.Serve(lis); err != nil {
		log.Fatalf("falló al servir: %v", err)
	}
}
