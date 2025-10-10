package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// -------------------------------------------------------------------------
// --- 1. Variables Globales y Estructuras (Usa flags, no constantes fijas)
// -------------------------------------------------------------------------

const brokerAddress = "broker:50051"

var (
	// ⚠️ CORRECCIÓN 1: Definimos las banderas para que el ID y el Puerto se lean de la línea de comandos.
	dbNodeID   = flag.String("id", "DB1", "ID único del nodo DB (e.g., DB1, DB2, DB3).")
	dbNodePort = flag.String("port", ":50061", "Puerto local del servidor gRPC del Nodo DB.")
)

// DBNodeServer implementa el servicio DBNode que el Broker llama (Fase 3).
type DBNodeServer struct {
	pb.UnimplementedEntityManagementServer
	pb.UnimplementedDBNodeServer // Necesario para evitar el error 'Unimplemented'

	entityID string

	// Almacenamiento replicado para la Fase 3
	data map[string]*pb.Offer
	mu   sync.Mutex
}

func NewDBNodeServer(id string) *DBNodeServer {
	return &DBNodeServer{
		entityID: id,
		data:     make(map[string]*pb.Offer),
	}
}

// -------------------------------------------------------------------------
// --- 2. Fase 1: Registro (Modificado para usar el ID de la bandera)
// -------------------------------------------------------------------------

func registerWithBroker(client pb.EntityManagementClient, server *DBNodeServer) {
	fmt.Printf("Coordinando el registro con el Broker...\n")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	req := &pb.RegistrationRequest{
		EntityId:   server.entityID, // ⚠️ Usa el ID de la estructura
		EntityType: "DBNode",
		Address:    "localhost" + *dbNodePort, // ⚠️ Usa el puerto de la bandera
	}

	resp, err := client.RegisterEntity(ctx, req)
	if err != nil {
		fmt.Printf("❌ No se logró conectar con el broker: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Respuesta del Broker: Éxito=%t, Mensaje=%s\n", resp.Success, resp.Message)

	if !resp.Success {
		os.Exit(1)
	}
}

// -------------------------------------------------------------------------
// --- 3. Fase 3: Almacenamiento (StoreOffer)
// -------------------------------------------------------------------------

// StoreOffer implementa el método que el Broker llama para guardar la oferta.
func (s *DBNodeServer) StoreOffer(ctx context.Context, offer *pb.Offer) (*pb.StoreOfferResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Lógica de guardado: Almacena la oferta usando su ID único como clave
	s.data[offer.GetOfertaId()] = offer

	// Simulación de escritura exitosa
	fmt.Printf("✅ Oferta almacenada con éxito %s (P: %d, S: %d). Ítems totales: %d\n",
		offer.GetOfertaId(),
		offer.GetPrecio(),
		offer.GetStock(),
		len(s.data))

	return &pb.StoreOfferResponse{
		Success: true,
		Message: fmt.Sprintf("Oferta %s almacenada con éxito en %s.\n", offer.GetOfertaId(), s.entityID),
	}, nil
}

// -------------------------------------------------------------------------
// --- 4. Función Principal (Con servidor gRPC para recibir peticiones)
// -------------------------------------------------------------------------

func main() {
	// 1. Lee los valores de las banderas (-id, -port)
	flag.Parse()

	// Crea la instancia del servidor DBNode, pasando el ID real
	dbServer := NewDBNodeServer(*dbNodeID)

	// 2. Conexión y Registro con el Broker (Fase 1)
	conn, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("No se logró conexión con broker: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewEntityManagementClient(conn)
	registerWithBroker(client, dbServer)

	fmt.Printf("Registro completo. Empezando a escuchar en %s...\n", *dbNodePort)

	// 3. Inicia el servidor gRPC del Nodo DB (Para recibir StoreOffer del Broker)
	lis, err := net.Listen("tcp", *dbNodePort)
	if err != nil {
		fmt.Printf("Fallo al escuchar en %s: %v\n", *dbNodePort, err)
		os.Exit(1)
	}

	s := grpc.NewServer()

	// Registrar ambos servicios en el Nodo DB
	pb.RegisterEntityManagementServer(s, dbServer) // Aunque no lo necesite, es buena práctica registrar la estructura
	pb.RegisterDBNodeServer(s, dbServer)           // ⚠️ Necesario para la Fase 3

	fmt.Printf("Listo para almacenar ofertas en %s...\n", *dbNodePort)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("Fallo al servir: %v\n", err)
		os.Exit(1)
	}
}
