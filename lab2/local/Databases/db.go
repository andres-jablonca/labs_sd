package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// -------------------------------------------------------------------------
// --- 1. Variables Globales y Estructuras
// -------------------------------------------------------------------------

// Dirección del Broker usando su nombre de servicio Docker: "broker"
const brokerAddress = "broker:50095"

// 💡 CONSTANTES DE FALLO AUTÓNOMO
const (
	FailureCheckInterval = 10 * time.Second // Revisar si fallar cada 15 segundos
	FailureProbability   = 0.40             // 20% de probabilidad de caer en cada chequeo
	MinFailureDuration   = 5 * time.Second  // Duración mínima del fallo
	MaxFailureDuration   = 15 * time.Second // Duración máxima del fallo
)

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
		req := &pb.RecoveryRequest{RequestingNodeId: myID}

		// 1. Solicitar historial
		resp, err := client.GetOfferHistory(context.Background(), req)
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
		// Retornar un error de gRPC específico para que el Broker sepa que el nodo está inaccesible temporalmente.
		return &pb.StoreOfferResponse{
			Success: false,
			Message: fmt.Sprintf("❌ %s está en estado de fallo y no acepta escrituras.", s.entityID),
		}, status.Error(codes.Unavailable, "Nodo DB no disponible")
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

// startFailureDaemon corre un bucle que decide probabilísticamente si iniciar un fallo.
func (s *DBNodeServer) startFailureDaemon() {
	fmt.Printf("[%s] 🛠️ Daemon de fallos iniciado. Probabilidad: %.0f%% cada %s.\n",
		s.entityID, FailureProbability*100, FailureCheckInterval.String())

	for {
		time.Sleep(FailureCheckInterval)

		// 1. Evitar iniciar un nuevo fallo si ya está en curso
		failureMu.Lock()
		if isFailing {
			failureMu.Unlock()
			continue
		}
		failureMu.Unlock()

		// 2. Comprobar la probabilidad
		if rand.Float64() < FailureProbability {
			// Calcular duración aleatoria entre Min y Max
			durationSeconds := rand.Intn(int(MaxFailureDuration.Seconds()-MinFailureDuration.Seconds())) + int(MinFailureDuration.Seconds())
			duration := time.Duration(durationSeconds) * time.Second

			// Iniciar el fallo en una nueva goroutine para no bloquear el daemon
			go s.simulateFailure(duration)
		}
	}
}

// simulateFailure bloquea las escrituras por la duración especificada.
func (s *DBNodeServer) simulateFailure(duration time.Duration) {
	// 1. Establecer el estado de fallo
	failureMu.Lock()
	isFailing = true
	fmt.Printf("[%s] 🛑 FALLO AUTÓNOMO INICIADO: Bloqueando escrituras por %s...\n", s.entityID, duration)
	failureMu.Unlock()

	// 2. Dormir por la duración del fallo
	time.Sleep(duration)

	// 3. Restaurar el estado
	failureMu.Lock()
	isFailing = false
	failureMu.Unlock()

	// 4. Solicitar resincronización con un peer (si es posible)
	peerAddresses := []string{}
	if s.entityID != "DB1" {
		peerAddresses = append(peerAddresses, "db1:50061")
	}
	if s.entityID != "DB2" {
		peerAddresses = append(peerAddresses, "db2:50062")
	}
	if s.entityID != "DB3" {
		peerAddresses = append(peerAddresses, "db3:50063")
	}

	if len(peerAddresses) > 0 {
		performResync(s.entityID, peerAddresses, s)
	} else {
		fmt.Printf("[%s] ⚠️ No hay peers disponibles para resincronización.\n", s.entityID)
	}

	// 5. Confirmación de restauración

	fmt.Printf("[%s] ✅ FALLO AUTÓNOMO FINALIZADO. Respondiendo escrituras nuevamente.\n", s.entityID)

}

// -------------------------------------------------------------------------
// --- 4. Función Principal
// -------------------------------------------------------------------------

func main() {
	flag.Parse()
	dbServer := NewDBNodeServer(*dbNodeID)

	// 💡 NUEVO: Sembrar el generador de números aleatorios
	rand.Seed(time.Now().UnixNano())

	// 💡 NUEVO: Iniciar el daemon de fallos en una goroutine
	go dbServer.startFailureDaemon()

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

	fmt.Printf("Listo para almacenar ofertas en %s...\n", *dbNodePort)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("Fallo al servir: %v\n", err)
		os.Exit(1)
	}

}
