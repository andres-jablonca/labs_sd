package main

import (
    "context"
    "log"
    "net" // <-- Necesario para net.Listen
    "sync"

    pb "lab3/proto"
    "google.golang.org/grpc" // <-- Necesario para grpc.NewServer
)

// DatanodeServer implementa un servicio (que el Broker llama) y almacena los datos.
type DatanodeServer struct {
    pb.UnimplementedDatanodeServiceServer 
    flightState map[string]string 
    mu          sync.Mutex
    id          string 
}

// Implementación de ApplyWrite y ReadData (Tu código anterior)
func (s *DatanodeServer) ApplyWrite(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    // Lógica de escritura...
    key := req.FlightId + ":" + req.SeatNumber
    if _, occupied := s.flightState[key]; occupied {
        return &pb.UpdateResponse{Success: false, Message: "Asiento ya ocupado."}, nil
    }
    s.flightState[key] = req.ClientId
    
    log.Printf("Datanode %s: ESCRITURA: Asiento %s asignado a %s en vuelo %s.", s.id, req.SeatNumber, req.ClientId, req.FlightId)
    
    return &pb.UpdateResponse{Success: true, Message: "Escritura aplicada."}, nil
}

func (s *DatanodeServer) ReadData(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    // Lógica de lectura...
    var assignedSeat string
    for seatKey, clientID := range s.flightState {
        if clientID == req.ClientId {
            // Suponemos que el formato de seatKey es Vuelo:Asiento. Queremos solo el Asiento.
            // Para simplificar, asumimos que assignedSeat ya contiene solo el asiento.
            assignedSeat = seatKey 
            break
        }
    }
    
    log.Printf("Datanode %s: LECTURA: Devolviendo estado para %s (Asiento: %s).", s.id, req.ClientId, assignedSeat)

    return &pb.ReadResponse{
        FlightId: req.FlightId,
        SeatAssignedToClient: assignedSeat,
    }, nil
}

// ----------------------------------------------------
// FUNCION MAIN - PUNTO DE ENTRADA NECESARIO
// ----------------------------------------------------
func main() {
    // Usaremos puertos diferentes para cada Datanode (50061, 50062, etc.)
    const port = ":50063" 
    const datanodeID = "DN-3"

    lis, err := net.Listen("tcp", port)
    if err != nil {
        log.Fatalf("falló al escuchar el puerto %s: %v", port, err)
    }

    // Crea un nuevo servidor gRPC
    s := grpc.NewServer()
    
    // Registra tu servicio implementado
    pb.RegisterDatanodeServiceServer(s, &DatanodeServer{
        flightState: make(map[string]string),
        id: datanodeID, 
    })

    log.Printf("🚀 Datanode %s escuchando en %s", datanodeID, port)
    
    // Inicia el servidor gRPC
    if err := s.Serve(lis); err != nil {
        log.Fatalf("falló al servir: %v", err)
    }
}