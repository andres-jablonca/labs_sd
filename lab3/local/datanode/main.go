package main

import (
	"context"
	"flag" // IMPORTANTE: Para leer argumentos de línea de comandos
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	pb "lab3/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	GOSSIP_INTERVAL = 5 * time.Second
)

var (
	idPtr   = flag.String("id", "DN-1", "ID del Datanode (Ej: DN-1, DN-2, DN-3)")
	portPtr = flag.String("port", ":50061", "Puerto de escucha (Ej: :50061)")
)

// Estructura de datos
type FlightData struct {
	Status map[string]string
	VC     map[string]int64
}

// Servidor Datanode
type DatanodeServer struct {
	pb.UnimplementedDatanodeServiceServer
	rywState   map[string]string
	flightData map[string]FlightData
	mu         sync.Mutex
	id         string
	peers      []string
}

// =======================================================================
// LÓGICA DE SELECCIÓN DE PEERS DINÁMICA
// =======================================================================
// Devuelve la lista de compañeros basada en mi propia identidad.
func getPeers(myID string) []string {
	// Mapa maestro de la topología de la red
	// IMPORTANTE: Si mueves esto a VMs reales, cambia estos valores por las IPs reales (ej: "192.168.1.50:50062")
	allNodes := map[string]string{
		"DN-1": "datanode1:50061",
		"DN-2": "datanode2:50062",
		"DN-3": "datanode3:50063",
	}

	var peers []string
	for id, addr := range allNodes {
		// Soy peer de todos MENOS de mí mismo
		if id != myID {
			peers = append(peers, addr)
		}
	}
	return peers
}

// =======================================================================
// LÓGICA RYW (Check-in)
// =======================================================================
func (s *DatanodeServer) ApplyWrite(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := req.FlightId + ":" + req.SeatNumber
	if _, occupied := s.rywState[key]; occupied {
		return &pb.UpdateResponse{Success: false, Message: "Asiento ya ocupado."}, nil
	}
	s.rywState[key] = req.ClientId
	log.Printf("ESCRITURA RYW: Asiento %s asignado a %s en vuelo %s.", req.SeatNumber, req.ClientId, req.FlightId)
	return &pb.UpdateResponse{Success: true, Message: "Escritura aplicada."}, nil
}

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
	log.Printf("LECTURA RYW: Cliente %s -> Asiento: %s", req.ClientId, assignedSeat)
	return &pb.ReadResponse{
		FlightId:             req.FlightId,
		SeatAssignedToClient: assignedSeat,
	}, nil
}

// =======================================================================
// LÓGICA CONSISTENCIA EVENTUAL (Relojes & Conflictos)
// =======================================================================

func MergeVC(vc1, vc2 map[string]int64) map[string]int64 {
	merged := make(map[string]int64)
	for id, count := range vc1 {
		merged[id] = count
	}
	for id, count := range vc2 {
		if count > merged[id] {
			merged[id] = count
		}
	}
	return merged
}

func isCausallyPrior(vc1, vc2 map[string]int64) bool {
	for id, count1 := range vc1 {
		if count1 > vc2[id] {
			return false
		}
	}
	return true
}

func (s *DatanodeServer) ResolveConflict(existingData, newData FlightData) FlightData {
	log.Printf("¡CONFLICTO! Resolviendo %v vs %v...", existingData.VC, newData.VC)

	mergedVC := MergeVC(existingData.VC, newData.VC)
	statusExisting := existingData.Status["estado"]
	statusNew := newData.Status["estado"]

	finalStatusMap := make(map[string]string)
	for k, v := range existingData.Status {
		finalStatusMap[k] = v
	}
	for k, v := range newData.Status {
		finalStatusMap[k] = v
	}

	// REGLA: "Cancelado" gana
	if statusExisting == "Cancelado" || statusNew == "Cancelado" {
		finalStatusMap["estado"] = "Cancelado"
		log.Printf("Resolución: Ganó 'Cancelado'.")
	} else {
		log.Printf("Resolución: Fusión estándar.")
	}
	return FlightData{Status: finalStatusMap, VC: mergedVC}
}

func (s *DatanodeServer) UpdateFlightStatus(ctx context.Context, req *pb.UpdateFlightStatusRequest) (*pb.UpdateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	flightID := req.FlightId
	incomingData := FlightData{Status: req.Status, VC: req.VectorClock}
	existingData, found := s.flightData[flightID]

	if !found {
		s.flightData[flightID] = incomingData
		if s.flightData[flightID].VC[s.id] == 0 {
			s.flightData[flightID].VC[s.id] = 0
		}
		log.Printf("ALMACENADO Inicial de %s. VC: %v", flightID, incomingData.VC)
		return &pb.UpdateResponse{Success: true, Message: "Nuevo dato guardado."}, nil
	}

	vcLocal := existingData.VC
	vcIncoming := incomingData.VC
	isPrior := isCausallyPrior(vcLocal, vcIncoming)
	isDescendant := isCausallyPrior(vcIncoming, vcLocal)

	if isPrior {
		s.flightData[flightID] = incomingData
	} else if isDescendant {
		log.Printf("DESCARTADO %s (Dato viejo) estado: %v", flightID, incomingData.Status["estado"])
	} else {
		resolvedData := s.ResolveConflict(existingData, incomingData)
		s.flightData[flightID] = resolvedData
	}
	return &pb.UpdateResponse{Success: true}, nil
}

func (s *DatanodeServer) GetFlightStatus(ctx context.Context, req *pb.FlightRequest) (*pb.FlightResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	flightID := req.GetFlightID()
	clientVersion := req.GetLastversion()
	currentData, found := s.flightData[flightID]

	if !found {
		return nil, status.Errorf(codes.NotFound, "Vuelo no encontrado")
	}

	var currentScalar int64 = 0
	for k, v := range currentData.VC {
		if strings.Contains(k, "BROKER") {
			if v > currentScalar {
				currentScalar = v
			}
		}
	}

	if currentScalar < clientVersion {
		return nil, status.Errorf(codes.Unavailable, "Dato desactualizado")
	}

	return &pb.FlightResponse{
		FlightID: flightID,
		Status:   currentData.Status["estado"],
		Gate:     currentData.Status["puerta"],
		Version:  currentScalar,
	}, nil
}

// =======================================================================
// GOSSIP
// =======================================================================
func (s *DatanodeServer) gossipLoop() {
	ticker := time.NewTicker(GOSSIP_INTERVAL)
	defer ticker.Stop()
	for range ticker.C {
		s.sendGossip()
	}
}

func (s *DatanodeServer) sendGossip() {
	s.mu.Lock()
	dataSnapshot := make(map[string]FlightData)
	for fId, data := range s.flightData {
		vcCopy := make(map[string]int64)
		for k, v := range data.VC {
			vcCopy[k] = v
		}
		stCopy := make(map[string]string)
		for k, v := range data.Status {
			stCopy[k] = v
		}
		dataSnapshot[fId] = FlightData{Status: stCopy, VC: vcCopy}
	}

	if len(s.peers) == 0 {
		s.mu.Unlock()
		return
	}
	targetPeer := s.peers[rand.Intn(len(s.peers))]
	s.mu.Unlock()

	log.Printf("Gossip: Sincronizando con %s...", targetPeer)

	conn, err := grpc.Dial(targetPeer, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithTimeout(2*time.Second))
	if err != nil {
		log.Printf("Gossip falló conectando a %s: %v", targetPeer, err)
		return
	}
	defer conn.Close()
	client := pb.NewDatanodeServiceClient(conn)

	for fId, data := range dataSnapshot {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req := &pb.UpdateFlightStatusRequest{
			FlightId: fId, Status: data.Status, VectorClock: data.VC,
		}
		client.UpdateFlightStatus(ctx, req)
		cancel()
	}
}

// =======================================================================
// MAIN CON FLAGS
// =======================================================================
func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	myPeers := getPeers(*idPtr)

	// Iniciar Server
	lis, err := net.Listen("tcp", *portPtr)
	if err != nil {
		log.Fatalf("Falló al escuchar puerto %s: %v", *portPtr, err)
	}

	s := grpc.NewServer()
	server := &DatanodeServer{
		rywState:   make(map[string]string),
		flightData: make(map[string]FlightData),
		id:         *idPtr,
		peers:      myPeers,
	}

	pb.RegisterDatanodeServiceServer(s, server)

	go server.gossipLoop()

	log.Printf("Datanode %s iniciado en puerto %s (Peers: %v)", *idPtr, *portPtr, myPeers)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Falló al servir: %v", err)
	}
}
