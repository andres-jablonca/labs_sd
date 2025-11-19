package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	pb "lab3/proto"
	"google.golang.org/grpc"
)

// DatanodeID es el ID del Datanode. SessionState mapea ClientID a DatanodeID.
type SessionState struct {
	DatanodeID string
	ExpiryTime time.Time
}
const (
    sessionTTL = time.Minute * 5 // Tiempo de vida de la sesión
    coordinatorAddr = ":50051"   // Puerto donde escucha el Coordinador
    brokerAddr = "localhost:50052" // Dirección del Broker Central
)
// ... (resto de tu código)

type CoordinatorServer struct {
	pb.UnimplementedCheckInCoordinatorServer
	sessionMap map[string]SessionState // client_id -> SessionState
	mu         sync.RWMutex
	brokerClient pb.CentralBrokerClient
}


// ProcessCheckIn maneja la ESCRITURA del cliente RYW.
func (s *CoordinatorServer) ProcessCheckIn(ctx context.Context, req *pb.CheckInRequest) (*pb.CheckInResponse, error) {
	// 1. Reenvía la escritura al Broker (el Broker elige el Datanode).
	log.Printf("Coordinador: Recibida escritura de %s. Reenviando a Broker...", req.ClientId)

	updateReq := &pb.UpdateRequest{
		ClientId:    req.ClientId,
		FlightId:    req.FlightId,
		SeatNumber:  req.SeatNumber,
		RequestUuid: req.RequestUuid,
	}

	// Llama al Broker Central (asume que s.brokerClient está inicializado)
	updateRes, err := s.brokerClient.UpdateFlightData(ctx, updateReq)
	if err != nil || !updateRes.Success {
		return &pb.CheckInResponse{Success: false, Message: "Error interno o de negocio."}, err
	}
	
	// 2. REGISTRA AFINIDAD DE SESIÓN[cite: 67].
	s.mu.Lock()
	s.sessionMap[req.ClientId] = SessionState{
		DatanodeID: updateRes.DatanodeId, // Datanode que procesó la escritura
		ExpiryTime: time.Now().Add(sessionTTL),
	}
	s.mu.Unlock()
	log.Printf("Coordinador: Escritura de %s procesada por DN: %s. Sesión sticky registrada.", req.ClientId, updateRes.DatanodeId)

	return &pb.CheckInResponse{Success: true, Message: "Check-in completado exitosamente."}, nil
}

// En coordinator/main.go

// La función GetFlightData debe recibir el VC del cliente y pasarlo al Broker:
// En coordinator/main.go

func (s *CoordinatorServer) GetFlightData(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
    log.Printf("Coordinador: Recibida solicitud de lectura RYW para %s. VC Cliente: %v", req.FlightId, req.ClientVectorClock)
    
    // 1. Reenviar el ReadRequest al Broker, incluyendo el VC del cliente
    brokerResp, err := s.brokerClient.GetFlightData(ctx, &pb.ReadRequest{
        FlightId:    req.FlightId,
        // ✅ Corregido: Los campos ahora existen en pb.ReadRequest
        ClientVectorClock: req.ClientVectorClock, 
    })
    
    if err != nil {
        log.Printf("❌ ERROR al obtener datos del Broker: %v", err)
        return nil, err
    }
    
    // 2. Devolver la respuesta del Broker, que incluye el VC del Datanode
    return &pb.ReadResponse{
        FlightId:             brokerResp.FlightId,
        SeatAssignedToClient: brokerResp.SeatAssignedToClient,
        // ✅ Corregido: El campo ahora existe en pb.ReadResponse
        VectorClock:          brokerResp.VectorClock, 
    }, nil
}

// GetBoardingPass maneja la LECTURA de confirmación RYW.
func (s *CoordinatorServer) GetBoardingPass(ctx context.Context, req *pb.BoardingPassRequest) (*pb.BoardingPassResponse, error) {
	s.mu.RLock()
	session, active := s.sessionMap[req.ClientId]
	s.mu.RUnlock()

	// 1. VERIFICA AFINIDAD ACTIVA[cite: 70].
	if active && session.ExpiryTime.After(time.Now()) {
		log.Printf("Coordinador: Lectura de %s. SESIÓN ACTIVA (%s). Redirigiendo a Datanode %s.", 
			req.ClientId, session.DatanodeID, session.DatanodeID)
		
		// 2. REDIRECCIÓN DIRECTA: Llama al Datanode específico (saltando el Broker).
		// *En una implementación real, aquí se necesitaría un cliente gRPC para cada Datanode*
		
		// Simulación de lectura afín exitosa (asume que el DN tiene los datos)
		// La llamada real sería a un método del Datanode, ej: datanodeClient.ReadData(ctx, readReq)
		
		return &pb.BoardingPassResponse{
			ClientId: req.ClientId,
			FlightId: req.FlightId,
			SeatAssigned: "21A", // Debe ser el dato escrito previamente
			Gate: "C7",
		}, nil 

	} else {
		// 3. REDIRECCIÓN BALANCEADA: Si no hay sesión o expiró, reenvía al Broker para balanceo[cite: 71].
		log.Printf("Coordinador: Lectura de %s. Sin sesión activa. Reenviando al Broker para balanceo.", req.ClientId)
		
		// Llama al Broker Central para lectura balanceada (el Broker elige cualquier DN).
		readReq := &pb.ReadRequest{ClientId: req.ClientId, FlightId: req.FlightId}
		readRes, err := s.brokerClient.GetFlightData(ctx, readReq)
		if err != nil {
			return nil, err
		}
		
		// Mapear la respuesta del Broker a la respuesta del Boarding Pass
		return &pb.BoardingPassResponse{
			ClientId: req.ClientId,
			FlightId: readRes.FlightId,
			SeatAssigned: readRes.SeatAssignedToClient,
			Gate: "C7", // Ejemplo
		}, nil
	}
}

// Función principal para iniciar el servidor del Coordinador
// ... (Todo el código de arriba: structs, ProcessCheckIn, GetBoardingPass)

// Función principal para iniciar el servidor del Coordinador
func main() {
    log.Println("Coordinador: Intentando conectar al Broker Central...")

    // 1. Conexión al Broker Central (Server del Broker)
    connBroker, err := grpc.Dial(brokerAddr, grpc.WithInsecure(), grpc.WithBlock()) 
    if err != nil { 
        log.Fatalf("❌ ERROR: No se pudo conectar al Broker en %s: %v", brokerAddr, err)
    }
    defer connBroker.Close()
    
    brokerClient := pb.NewCentralBrokerClient(connBroker)
    log.Printf("✅ Coordinador conectado al Broker en %s", brokerAddr)
    
    // 2. Inicialización del servidor Coordinador
    lis, err := net.Listen("tcp", coordinatorAddr)
    if err != nil {
        log.Fatalf("❌ ERROR: Falló al escuchar en %s: %v", coordinatorAddr, err)
    }

    s := grpc.NewServer()
    
    // 3. Registro del servicio
    pb.RegisterCheckInCoordinatorServer(s, &CoordinatorServer{
        sessionMap: make(map[string]SessionState),
        // Pasar la conexión inicializada al Broker
        brokerClient: brokerClient, 
    })

    log.Printf("🚀 Coordinador (Gateway de Check-in) escuchando en %v", lis.Addr())
    
    // 4. Iniciar el servidor
    if err := s.Serve(lis); err != nil {
        log.Fatalf("falló al servir: %v", err)
    }
}

