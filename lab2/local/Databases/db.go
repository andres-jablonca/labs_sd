package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// -------------------------------------------------------------------------
// --- 1. Variables Globales y Estructuras
// -------------------------------------------------------------------------

// Dirección del Broker usando su nombre de servicio Docker: "broker"
const brokerAddress = "broker:50095"

var (
	dbNodeID   = flag.String("id", "DB1", "ID único del nodo DB (e.g., DB1, DB2, DB3).")
	dbNodePort = flag.String("port", ":50061", "Puerto local del servidor gRPC del Nodo DB.")
)

// FASE 5: Variables de estado para la simulación de fallos
var (
	isFailing = false
	failureMu sync.Mutex
)

// DBNodeServer implementa los servicios requeridos (incluyendo el de control para la FASE 5).
type DBNodeServer struct {
	pb.UnimplementedEntityManagementServer
	pb.UnimplementedDBNodeServer
	pb.UnimplementedDBControlServer // 💡 FASE 5: Servicio para recibir órdenes de fallo

	entityID string
	data     map[string]*pb.Offer
	mu       sync.Mutex
}

func NewDBNodeServer(id string) *DBNodeServer {
	return &DBNodeServer{
		entityID: id,
		data:     make(map[string]*pb.Offer),
	}
}

// -------------------------------------------------------------------------
// --- Recuperación y Resincronización
// -------------------------------------------------------------------------

// GetOfferHistory devuelve todas las ofertas almacenadas localmente.
func (s *DBNodeServer) GetOfferHistory(ctx context.Context, req *pb.RecoveryRequest) (*pb.RecoveryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var offersToSend []*pb.Offer

	// Itera sobre el mapa s.data y copia todas las ofertas a la respuesta.
	for _, offer := range s.data {
		offersToSend = append(offersToSend, offer)
	}

	fmt.Printf("[Recuperación] Nodo %s solicitó historial. Enviando %d ofertas.\n", req.GetRequestingNodeId(), len(offersToSend))

	return &pb.RecoveryResponse{
		Offers: offersToSend,
	}, nil
}

// Función de Resincronización al inicio (sin cambios)
func performResync(myID string, peerAddresses []string, s *DBNodeServer) {
	fmt.Printf("[%s] Iniciando resincronización...\n", myID)

	// Intentar conectarse a cualquier peer disponible.
	for _, peerAddress := range peerAddresses {
		fmt.Printf("[%s] Intentando conectar con peer %s...\n", myID, peerAddress)

		conn, err := grpc.Dial(peerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("[%s] ❌ Fallo al conectar con peer %s: %v. Probando otro.\n", myID, peerAddress, err)
			continue
		}

		client := pb.NewDBNodeClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
		req := &pb.RecoveryRequest{RequestingNodeId: myID}

		// 1. Solicitar historial
		resp, err := client.GetOfferHistory(ctx, req)
		cancel()
		conn.Close() // Cerrar la conexión inmediatamente después del uso.

		if err != nil {
			fmt.Printf("[%s] ❌ Error solicitando historial a %s: %v. Probando otro.\n", myID, peerAddress, err)
			continue
		}

		// 2. Guardar historial recibido
		s.mu.Lock()
		offersRecovered := 0
		for _, offer := range resp.GetOffers() {
			// Re-almacenar en su mapa s.data
			s.data[offer.GetOfertaId()] = offer
			offersRecovered++
		}
		s.mu.Unlock()

		fmt.Printf("✅ [%s] Sincronización exitosa. Se recuperaron %d ofertas de %s.\n", myID, offersRecovered, peerAddress)
		return // Sincronización exitosa, salir de la función.
	}

	fmt.Printf("[%s] ⚠️ Advertencia: No se pudo resincronizar con ningún peer. Iniciando vacío.\n", myID)
}

// -------------------------------------------------------------------------
// --- 2. Fase 1: Registro
// -------------------------------------------------------------------------

func registerWithBroker(client pb.EntityManagementClient, server *DBNodeServer) {
	fmt.Printf("Coordinando el registro con el Broker...\n")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// Usar el ID de la entidad en minúsculas para coincidir con el nombre del servicio Docker (ej: DB1 -> db1).
	dockerServiceName := strings.ToLower(server.entityID)
	addressToRegister := dockerServiceName + *dbNodePort // ej: "db1:50061"

	req := &pb.RegistrationRequest{
		EntityId:   server.entityID,
		EntityType: "DBNode",
		Address:    addressToRegister,
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
// --- 3. FASE 3: Almacenamiento (StoreOffer)
// -------------------------------------------------------------------------

// StoreOffer implementa el método que el Broker llama para guardar la oferta.
func (s *DBNodeServer) StoreOffer(ctx context.Context, offer *pb.Offer) (*pb.StoreOfferResponse, error) {
	// 💡 FASE 5: Chequeo de estado de fallo
	failureMu.Lock()
	if isFailing {
		failureMu.Unlock()
		fmt.Printf("[FALLO] ❌ Oferta %s recibida pero ignorada (Nodo en fallo).\n", offer.GetOfertaId())

		// Simular un fallo/timeout forzando que la respuesta demore más que el timeout del Broker (3s).
		// Esperamos 4 segundos para garantizar el timeout en el Broker.
		time.Sleep(time.Second * 4) 
		// Devolver error gRPC.
		return nil, fmt.Errorf("simulated failure: node is down for %s", s.entityID) 
	}
	failureMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Lógica de guardado
	s.data[offer.GetOfertaId()] = offer

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
// --- 5. FASE 5: Control de Fallos (SimulateFailure)
// -------------------------------------------------------------------------

// SimulateFailure implementa el método que el Broker llama para forzar una caída temporal.
func (s *DBNodeServer) SimulateFailure(ctx context.Context, req *pb.FailureRequest) (*pb.FailureResponse, error) {
	duration := time.Duration(req.GetDurationSeconds()) * time.Second

	failureMu.Lock()
	if isFailing {
		failureMu.Unlock()
		return &pb.FailureResponse{
			Success: false,
			Message: "Ya en estado de fallo. Ignorando nueva solicitud.",
		}, nil
	}
	isFailing = true
	failureMu.Unlock()

	fmt.Printf("[FALLO] 🛑 Simulación de fallo INICIADA: Dejando de responder escrituras por %s...\n", duration)

	// Gorutina que esperará la duración del fallo y restaurará el estado
	go func() {
		time.Sleep(duration)
		failureMu.Lock()
		isFailing = false
		failureMu.Unlock()
		fmt.Printf("[FALLO] ✅ Simulación de fallo FINALIZADA. Respondiendo escrituras nuevamente.\n")
	}()

	return &pb.FailureResponse{
		Success: true,
		Message: fmt.Sprintf("Fallo de %s activado.\n", duration),
	}, nil
}

// -------------------------------------------------------------------------
// --- 4. Función Principal
// -------------------------------------------------------------------------

func main() {
	flag.Parse()
	dbServer := NewDBNodeServer(*dbNodeID)

	// Conexión y Registro con el Broker (Fase 1)
	conn, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("No se logró conexión con broker: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewEntityManagementClient(conn)
	registerWithBroker(client, dbServer)

	fmt.Printf("Registro completo. Empezando a escuchar en %s...\n", *dbNodePort)

	// Inicia el servidor gRPC del Nodo DB
	lis, err := net.Listen("tcp", *dbNodePort)
	if err != nil {
		fmt.Printf("Fallo al escuchar en %s: %v\n", *dbNodePort, err)
		os.Exit(1)
	}

	s := grpc.NewServer()

	pb.RegisterEntityManagementServer(s, dbServer)
	pb.RegisterDBNodeServer(s, dbServer)
	pb.RegisterDBControlServer(s, dbServer) // 💡 FASE 5: Registrar el nuevo servicio de control

	fmt.Printf("Listo para almacenar ofertas en %s...\n", *dbNodePort)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("Fallo al servir: %v\n", err)
		os.Exit(1)
	}
}