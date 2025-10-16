// db.go
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
	"google.golang.org/grpc/credentials/insecure"
)

// -----------------------------------------------------------------------------
// Config (sin tocar docker-compose)
// -----------------------------------------------------------------------------

const brokerAddress = "broker:50095"

var (
	dbNodeID   = flag.String("id", "DB1", "ID del nodo DB (DB1|DB2|DB3)")
	dbNodePort = flag.String("port", ":50061", "Puerto gRPC del nodo DB")
)

// -----------------------------------------------------------------------------
// Estado en memoria + fallo
// -----------------------------------------------------------------------------

var (
	isFailing bool
	failMu    sync.Mutex
)

type DBNodeServer struct {
	pb.UnimplementedEntityManagementServer
	pb.UnimplementedDBNodeServer

	entityID string
	mu       sync.RWMutex
	data     map[string]*pb.Offer // oferta_id -> Offer (idempotente)
}

func NewDBNodeServer(id string) *DBNodeServer {
	return &DBNodeServer{
		entityID: id,
		data:     make(map[string]*pb.Offer),
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// peers según compose (sin modificar compose)
func peersFor(id string) []string {
	peers := []string{}
	if id != "DB1" {
		peers = append(peers, "db1:50061")
	}
	if id != "DB2" {
		peers = append(peers, "db2:50062")
	}
	if id != "DB3" {
		peers = append(peers, "db3:50063")
	}
	return peers
}

// Registro con Broker (Fase 1)
func registerWithBroker(client pb.EntityManagementClient, server *DBNodeServer) {
	fmt.Println("Registrando con Broker…")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dockerServiceName := strings.ToLower(server.entityID)
	addressToRegister := dockerServiceName + *dbNodePort // ej: "db1:50061"

	req := &pb.RegistrationRequest{
		EntityId:   server.entityID,
		EntityType: "DBNode",
		Address:    addressToRegister,
	}

	resp, err := client.RegisterEntity(ctx, req)
	if err != nil {
		fmt.Printf("❌ No se pudo registrar con el broker: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Broker respondió: success=%t, msg=%s\n", resp.GetSuccess(), resp.GetMessage())
	if !resp.GetSuccess() {
		os.Exit(1)
	}
}

// Resincroniza con un peer usando SOLO GetOfferHistory
func Resincronizar(myID string, s *DBNodeServer) {
	addrs := peersFor(myID)
	if len(addrs) == 0 {
		return
	}

	for _, addr := range addrs {
		fmt.Printf("[%s] Resincronizando desde %s ...\n", myID, addr)
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("[%s] no conecta con %s: %v\n", myID, addr, err)
			continue
		}
		client := pb.NewDBNodeClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		resp, err := client.GetOfferHistory(ctx, &pb.RecoveryRequest{RequestingNodeId: myID})
		cancel()
		conn.Close()

		if err != nil || resp == nil {
			fmt.Printf("[%s] GetOfferHistory falló con %s: %v\n", myID, addr, err)
			continue
		}

		// Merge idempotente por oferta_id
		added := 0
		s.mu.Lock()
		for _, of := range resp.GetOffers() {
			if of == nil || of.GetOfertaId() == "" {
				continue
			}
			if _, ok := s.data[of.GetOfertaId()]; !ok {
				s.data[of.GetOfertaId()] = of
				added++
			}
		}
		total := len(s.data)
		s.mu.Unlock()
		fmt.Printf("✅ [%s] Sync completo desde %s | nuevas=%d | total=%d\n", myID, addr, added, total)
		return // con un peer sano basta
	}
	fmt.Printf("[%s] ⚠️ No se pudo resincronizar con ningún peer\n", myID)
}

// -----------------------------------------------------------------------------
// Fallos automáticos recurrentes: cada 25s hay 10% de caer 15s
// -----------------------------------------------------------------------------

const (
	FailureCheckInterval = 25 * time.Second
	FailureProbability   = 0.10
	FailureDuration      = 15 * time.Second
)

func (s *DBNodeServer) IniciarDaemonDeFallos() {
	ticker := time.NewTicker(FailureCheckInterval)
	defer ticker.Stop()

	fmt.Printf("[%s] 🛠️ Daemon de fallos: fallos con una probabilidad de %.0f%% y duración de %s (Cada %s)\n",
		s.entityID, FailureProbability*100, FailureDuration, FailureCheckInterval)

	for range ticker.C {
		failMu.Lock()
		currentlyFailing := isFailing
		failMu.Unlock()
		if currentlyFailing {
			continue
		}

		if rand.Float64() < FailureProbability {
			go func() {
				failMu.Lock()
				if isFailing {
					failMu.Unlock()
					return
				}
				isFailing = true
				failMu.Unlock()

				fmt.Printf("🛑 [%s] CAÍDA INESPERADA! (Duración de %s s)\n", s.entityID, FailureDuration)
				time.Sleep(FailureDuration)

				failMu.Lock()
				isFailing = false
				failMu.Unlock()
				fmt.Printf("✅ [%s] LEVANTÁNDOSE NUEVAMENTE... INICIANDO RESINCRONIZACIÓN...\n", s.entityID)

				Resincronizar(s.entityID, s)
			}()
		}
	}
}

// -----------------------------------------------------------------------------
// Implementación servicios
// -----------------------------------------------------------------------------

// StoreOffer: escritura idempotente; rechaza si "caído"
func (s *DBNodeServer) StoreOffer(ctx context.Context, offer *pb.Offer) (*pb.StoreOfferResponse, error) {
	failMu.Lock()
	failing := isFailing
	failMu.Unlock()
	if failing {
		return &pb.StoreOfferResponse{
			Success: false,
			Message: fmt.Sprintf("%s en fallo: no acepta escrituras", s.entityID),
		}, nil
	}

	if offer.GetOfertaId() == "" {
		return &pb.StoreOfferResponse{Success: false, Message: "oferta_id vacío"}, nil
	}

	s.mu.Lock()
	if _, exists := s.data[offer.GetOfertaId()]; !exists {
		s.data[offer.GetOfertaId()] = offer
	}
	total := len(s.data)
	s.mu.Unlock()

	fmt.Printf("📝 [%s] StoreOffer %s | total=%d\n", s.entityID, offer.GetOfertaId(), total)
	return &pb.StoreOfferResponse{Success: true, Message: "ok"}, nil
}

// GetOfferHistory: devuelve snapshot completo (para recuperación)
func (s *DBNodeServer) GetOfferHistory(ctx context.Context, req *pb.RecoveryRequest) (*pb.RecoveryResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*pb.Offer, 0, len(s.data))
	for _, of := range s.data {
		out = append(out, of)
	}
	fmt.Printf("📦 [%s] Enviando snapshot a %s (ofertas=%d)\n", s.entityID, req.GetRequestingNodeId(), len(out))
	return &pb.RecoveryResponse{Offers: out}, nil
}

// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	dbServer := NewDBNodeServer(*dbNodeID)

	// Registro con Broker
	conn, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("No se logró conexión con broker: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	emClient := pb.NewEntityManagementClient(conn)
	registerWithBroker(emClient, dbServer)

	// Servidor gRPC
	fmt.Printf("[%s] Escuchando en %s…\n", *dbNodeID, *dbNodePort)
	lis, err := net.Listen("tcp", *dbNodePort)
	if err != nil {
		fmt.Printf("Listen %s falló: %v\n", *dbNodePort, err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	pb.RegisterEntityManagementServer(s, dbServer)
	pb.RegisterDBNodeServer(s, dbServer)

	// Inicia el daemon de fallos periódicos (10% cada 25s, 15s caído)
	go dbServer.IniciarDaemonDeFallos()

	if err := s.Serve(lis); err != nil {
		fmt.Printf("Serve falló: %v\n", err)
		os.Exit(1)
	}
}
