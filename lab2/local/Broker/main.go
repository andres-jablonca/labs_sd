package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "lab2/proto" // Asegúrate que esta ruta coincida con tu go.mod

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Constantes del sistema (N=3, W=2, R=2)
const (
	brokerPort = ":50051"
	N          = 3 // Número total de réplicas
	W          = 2 // Número de confirmaciones de escritura requeridas
	// R = 2 // Número de lecturas requeridas (no usado en esta fase)
)

// Estructura para registrar cualquier entidad (Productor, DB Node, Consumidor)
type Entity struct {
	ID      string
	Type    string
	Address string
}

// BrokerServer implementa los dos servicios gRPC requeridos
type BrokerServer struct {
	pb.UnimplementedEntityManagementServer
	pb.UnimplementedOfferSubmissionServer

	entities map[string]Entity
	dbNodes  map[string]Entity // Subconjunto de Nodos DB
	mu       sync.Mutex
}

// NewBrokerServer inicializa la estructura del Broker
func NewBrokerServer() *BrokerServer {
	return &BrokerServer{
		entities: make(map[string]Entity),
		dbNodes:  make(map[string]Entity),
	}
}

// -------------------------------------------------------------------------
// FASE 1: Registro de Entidades (EntityManagementServer)
// -------------------------------------------------------------------------

func (s *BrokerServer) RegisterEntity(ctx context.Context, req *pb.RegistrationRequest) (*pb.RegistrationResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entityID := req.GetEntityId()

	if _, ok := s.entities[entityID]; ok {
		return &pb.RegistrationResponse{
			Success: false,
			Message: fmt.Sprintf("Entity %s already registered.", entityID),
		}, nil
	}

	entity := Entity{
		ID:      entityID,
		Type:    req.GetEntityType(),
		Address: req.GetAddress(),
	}
	s.entities[entityID] = entity

	if req.GetEntityType() == "DBNode" {
		s.dbNodes[entityID] = entity
	}

	log.Printf("[Registration] ✅ Successfully registered %s (%s). Total registered: %d", entityID, entity.Type, len(s.entities))

	return &pb.RegistrationResponse{
		Success: true,
		Message: "Registration successful. Welcome to the CyberDay system.",
	}, nil
}

// -------------------------------------------------------------------------
// FASE 2 & 3: Recepción y Escritura Distribuida (OfferSubmissionServer)
// -------------------------------------------------------------------------

func (s *BrokerServer) SendOffer(ctx context.Context, offer *pb.Offer) (*pb.OfferSubmissionResponse, error) {
	log.Printf("[Offer] Received %s from %s. Starting distributed write (N=%d, W=%d)...", offer.GetOfertaId(), offer.GetTienda(), N, W)

	// VALIDACIÓN: Verificar que el número de nodos activos cumpla N
	if len(s.dbNodes) < N {
		log.Printf("[Offer] 🛑 Offer %s REJECTED. Only %d/%d DB Nodes are active. Cannot guarantee N=%d.", offer.GetOfertaId(), len(s.dbNodes), N, N)
		return &pb.OfferSubmissionResponse{
			Accepted: false,
			Message:  fmt.Sprintf("Cannot guarantee N=%d replicas. Only %d DB Nodes are active.", N, len(s.dbNodes)),
		}, nil
	}

	var wg sync.WaitGroup
	confirmedWrites := 0
	var countMu sync.Mutex

	// 2. Escritura Concurrente en Nodos DB (N=3)
	for _, node := range s.dbNodes {
		wg.Add(1)
		go func(node Entity) {
			defer wg.Done()

			// Conexión al Nodo DB
			conn, err := grpc.Dial(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithTimeout(time.Second*3))
			if err != nil {
				log.Printf("[Write] ❌ Error connecting to %s: %v", node.ID, err)
				return
			}
			defer conn.Close()

			// pb.NewDBNodeClient ya estará definido si se actualizó el .proto
			dbClient := pb.NewDBNodeClient(conn)

			// Llamada a la función de escritura del Nodo DB (StoreOffer debe estar en DB_nodo/main.go)
			resp, err := dbClient.StoreOffer(ctx, offer)

			if err != nil || !resp.GetSuccess() {
				log.Printf("[Write] ❌ %s failed to store offer %s. Error: %v", node.ID, offer.GetOfertaId(), err)
				return
			}

			// Escritura exitosa
			log.Printf("[Write] ✅ %s confirmed storage of %s.", node.ID, offer.GetOfertaId())

			countMu.Lock()
			confirmedWrites++
			countMu.Unlock()
		}(node)
	}

	// Esperar a que terminen todas las llamadas
	wg.Wait()

	// 3. Evaluar Condición W=2
	if confirmedWrites >= W {
		log.Printf("[Offer] ✅ Offer %s ACCEPTED. W=%d confirmation achieved. (Fase 4: Notificación a Consumidores)", offer.GetOfertaId(), confirmedWrites)

		// 4. Filtrar y Notificar a Consumidores (Lógica de Fase 4 aquí)
		// ...

		return &pb.OfferSubmissionResponse{
			Accepted: true,
			Message:  "Offer accepted and distributed successfully.",
		}, nil
	}

	// Falla si no se cumple W=2
	log.Printf("[Offer] ❌ Offer %s REJECTED. Only %d/%d confirmed writes achieved. (W=%d required).", offer.GetOfertaId(), confirmedWrites, 2, W)
	return &pb.OfferSubmissionResponse{
		Accepted: false,
		Message:  fmt.Sprintf("Write failed: only %d confirmed writes achieved (W=%d required).", confirmedWrites, W),
	}, nil
}

// -------------------------------------------------------------------------
// Función Principal
// -------------------------------------------------------------------------

func main() {
	lis, err := net.Listen("tcp", brokerPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	brokerServer := NewBrokerServer()

	// 1. Registrar el servicio EntityManagement (Fase 1)
	pb.RegisterEntityManagementServer(s, brokerServer)

	// 2. Registrar el servicio OfferSubmission (Fase 2)
	pb.RegisterOfferSubmissionServer(s, brokerServer)

	log.Printf("Broker Central listening for registrations on %s...", brokerPort)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
