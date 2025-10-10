package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Constantes del sistema (N=3, W=2, R=2)
const (
	brokerPort = ":50051"
	N          = 3 // Número total de réplicas
	W          = 2 // Número de confirmaciones de escritura requeridas
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
			Message: fmt.Sprintf("Entidad %s ya se encuentra registrada.\n", entityID),
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

	fmt.Printf("[Registro] ✅ %s registrad@ correctamente (%s). Total de registrados: %d\n", entityID, entity.Type, len(s.entities))

	return &pb.RegistrationResponse{
		Success: true,
		Message: "Registro exitoso. Bienvenido al CyberDay!.\n",
	}, nil
}

// -------------------------------------------------------------------------
// FASE 2 & 3: Recepción y Escritura Distribuida (OfferSubmissionServer)
// -------------------------------------------------------------------------

func (s *BrokerServer) SendOffer(ctx context.Context, offer *pb.Offer) (*pb.OfferSubmissionResponse, error) {
	fmt.Printf("[Oferta %s recibida por parte de %s. Iniciando escritura distribuida (N=%d, W=%d)...\n", offer.GetOfertaId(), offer.GetTienda(), N, W)

	// VALIDACIÓN: Verificar que el número de nodos activos cumpla N
	if len(s.dbNodes) < N {
		fmt.Printf("[Oferta] 🛑 Oferta %s RECHAZADA. Sólo %d/%d nodos de DB se encuentran activos. No se puede garantizar N=%d.\n", offer.GetOfertaId(), len(s.dbNodes), N, N)
		return &pb.OfferSubmissionResponse{
			Accepted: false,
			Message:  fmt.Sprintf("No se puede garantizar N=%d replicas. Sólo hay %d nodos de DB activos.\n", N, len(s.dbNodes)),
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
				fmt.Printf("[Escritura] ❌ Error conectando con %s: %v\n", node.ID, err)
				return
			}
			defer conn.Close()

			dbClient := pb.NewDBNodeClient(conn)

			// Llamada a la función de escritura del Nodo DB
			resp, err := dbClient.StoreOffer(ctx, offer)

			if err != nil || !resp.GetSuccess() {
				fmt.Printf("[Escritura] ❌ %s falló en almacenar la oferta %s. Error: %v\n", node.ID, offer.GetOfertaId(), err)
				return
			}

			// Escritura exitosa
			fmt.Printf("[Escritura] ✅ %s confirma el almacenamiento de la oferta %s.\n", node.ID, offer.GetOfertaId())

			countMu.Lock()
			confirmedWrites++
			countMu.Unlock()
		}(node)
	}

	// Esperar a que terminen todas las llamadas
	wg.Wait()

	// 3. Evaluar Condición W=2
	if confirmedWrites >= W {
		fmt.Printf("[Oferta] ✅ Oferta %s ACEPTADA. W=%d confirmación de escritura exitosa. (Fase 4: Notificación a Consumidores)\n", offer.GetOfertaId(), confirmedWrites)

		// 4. Filtrar y Notificar a Consumidores (Lógica de Fase 4 aquí)
		// ...

		return &pb.OfferSubmissionResponse{
			Accepted: true,
			Message:  "Oferta aceptada y distribuida con éxito.\n",
		}, nil
	}

	// Falla si no se cumple W=2
	fmt.Printf("[Oferta] ❌ Oferta %s RECHAZADA. Sólo se confirmaron %d/%d escrituras. (W=%d requerido).\n", offer.GetOfertaId(), confirmedWrites, 2, W)
	return &pb.OfferSubmissionResponse{
		Accepted: false,
		Message:  fmt.Sprintf("Escritura fallida: sólo se confirmaron %d escrituras (W=%d requerido).\n", confirmedWrites, W),
	}, nil
}

// -------------------------------------------------------------------------
// Función Principal
// -------------------------------------------------------------------------

func main() {
	lis, err := net.Listen("tcp", brokerPort)
	if err != nil {
		fmt.Printf("Fallo al escuchar en: %v\n", err)
		return
	}

	s := grpc.NewServer()
	brokerServer := NewBrokerServer()

	// 1. Registrar el servicio EntityManagement (Fase 1)
	pb.RegisterEntityManagementServer(s, brokerServer)

	// 2. Registrar el servicio OfferSubmission (Fase 2)
	pb.RegisterOfferSubmissionServer(s, brokerServer)

	fmt.Printf("Broker central escuchando y esperando registros en %s...\n", brokerPort)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("Fallo al servir: %v\n", err)
		return
	}
}
