package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	pb "lab3/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	PORT        = ":50055"
	BROKER_ADDR = "broker:50052"
	SESSION_TTL = 30 * time.Second // Tiempo de vida de la sesión "Sticky"
)

// Estructura para guardar la sesión del cliente
type SessionEntry struct {
	DatanodeID string
	LastAccess time.Time
}

type CoordinatorServer struct {
	pb.UnimplementedCheckInCoordinatorServer

	// Mapa de Sesiones: ClientID -> DatanodeID
	sessions map[string]SessionEntry
	mu       sync.Mutex
}

// Mapa auxiliar para resolver IDs a Direcciones (Hardcoded por simplicidad del lab)
var datanodeMap = map[string]string{
	"DN-1": "datanode1:50061",
	"DN-2": "datanode2:50062",
	"DN-3": "datanode3:50063",
}

// ---------------------------------------------------------------------------
// 1. ESCRITURA (ProcessCheckIn) -> Va al Broker, pero guarda quién lo atendió
// ---------------------------------------------------------------------------
func (s *CoordinatorServer) ProcessCheckIn(ctx context.Context, req *pb.CheckInRequest) (*pb.CheckInResponse, error) {
	log.Printf("Coordinador: Check-in recibido de Cliente %s para Vuelo %s Asiento %s", req.ClientId, req.FlightId, req.SeatNumber)

	// 1. Conectar al BROKER
	conn, err := grpc.Dial(BROKER_ADDR, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &pb.CheckInResponse{Success: false, Message: "Error conectando al Broker"}, err
	}
	defer conn.Close()
	brokerClient := pb.NewCentralBrokerClient(conn)

	// 2. Enviar solicitud de actualización al Broker
	updateReq := &pb.UpdateRequest{
		ClientId:    req.ClientId,
		FlightId:    req.FlightId,
		SeatNumber:  req.SeatNumber,
		RequestUuid: req.RequestUuid,
	}

	resp, err := brokerClient.UpdateFlightData(ctx, updateReq)
	if err != nil {
		log.Printf("Error RPC Broker: %v", err)
		return &pb.CheckInResponse{Success: false, Message: "Fallo en Broker"}, err
	}

	if resp.Success {
		// 3. STICKY SESSION: Guardar qué Datanode atendió esta escritura
		s.mu.Lock()
		s.sessions[req.ClientId] = SessionEntry{
			DatanodeID: resp.DatanodeId, // El proto nos devuelve quién escribió
			LastAccess: time.Now(),
		}
		s.mu.Unlock()
		log.Printf("Sesión Creada: Cliente %s pegado a %s (Sticky)", req.ClientId, resp.DatanodeId)
	}

	return &pb.CheckInResponse{
		Success:      resp.Success,
		Message:      resp.Message,
		ErrorDetails: "",
	}, nil
}

// ---------------------------------------------------------------------------
// 2. LECTURA (GetBoardingPass) -> Intenta ir directo al Datanode (Sticky Read)
// ---------------------------------------------------------------------------
func (s *CoordinatorServer) GetBoardingPass(ctx context.Context, req *pb.BoardingPassRequest) (*pb.BoardingPassResponse, error) {
	s.mu.Lock()
	session, exists := s.sessions[req.ClientId]
	// Validación simple de TTL
	if exists && time.Since(session.LastAccess) > SESSION_TTL {
		delete(s.sessions, req.ClientId)
		exists = false
		log.Printf("Sesión expirada para Cliente %s", req.ClientId)
	}
	s.mu.Unlock()

	// --- CAMINO A: STICKY READ (Directo al Datanode) ---
	if exists {
		targetAddr, ok := datanodeMap[session.DatanodeID]
		if ok {
			log.Printf("Sticky Read: Cliente %s redirigido directo a %s (%s)", req.ClientId, session.DatanodeID, targetAddr)

			// Conexión efímera al Datanode específico
			conn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				defer conn.Close()
				dnClient := pb.NewDatanodeServiceClient(conn)

				// Llamada directa
				dnResp, err := dnClient.ReadData(ctx, &pb.ReadRequest{ClientId: req.ClientId, FlightId: req.FlightId})
				if err == nil {
					// Éxito leyendo directo del Datanode donde escribimos
					return &pb.BoardingPassResponse{
						ClientId:     req.ClientId,
						FlightId:     dnResp.FlightId,
						SeatAssigned: dnResp.SeatAssignedToClient,
						Gate:         "Consultar Pantalla", // Dato dummy, el asiento es lo importante
					}, nil
				}
			}
			log.Printf("Falló Sticky Read con %s, haciendo fallback al Broker...", session.DatanodeID)
		}
	}

	// --- CAMINO B: FALLBACK (Al Broker) ---
	// Si no hay sesión o falló la conexión directa, le pedimos al Broker que busque.
	log.Printf("Lectura Standard: Consultando al Broker para Cliente %s", req.ClientId)

	conn, err := grpc.Dial(BROKER_ADDR, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	brokerClient := pb.NewCentralBrokerClient(conn)

	resp, err := brokerClient.GetFlightData(ctx, &pb.ReadRequest{ClientId: req.ClientId, FlightId: req.FlightId})
	if err != nil {
		return nil, err
	}

	return &pb.BoardingPassResponse{
		ClientId:     req.ClientId,
		FlightId:     resp.FlightId,
		SeatAssigned: resp.SeatAssignedToClient,
		Gate:         "Consultar Pantalla",
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("Falló al escuchar puerto %s: %v", PORT, err)
	}

	grpcServer := grpc.NewServer()
	coordinator := &CoordinatorServer{
		sessions: make(map[string]SessionEntry),
	}

	pb.RegisterCheckInCoordinatorServer(grpcServer, coordinator)

	log.Printf("Coordinador (Gateway) escuchando en %s", PORT)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Falló al servir gRPC: %v", err)
	}
}
