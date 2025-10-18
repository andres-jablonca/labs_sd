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

// -------------------------------------------------------------------------
// Config
// -------------------------------------------------------------------------

const brokerAddress = "broker:50095"

var (
	dbNodeID   = flag.String("id", "DB1", "ID del nodo DB (DB1|DB2|DB3)")
	dbNodePort = flag.String("port", ":50061", "Puerto gRPC del nodo DB")
)

// -------------------------------------------------------------------------
// Estado + servidor
// -------------------------------------------------------------------------

var (
	isFailing bool
	failMu    sync.Mutex
)

const (
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Reset  = "\033[0m"
)

type DBNodeServer struct {
	pb.UnimplementedEntityManagementServer
	pb.UnimplementedDBNodeServer
	pb.UnimplementedFinalizacionServer

	entityID string
	mu       sync.RWMutex
	data     map[string]*pb.Offer
	stopCh   chan struct{}
}

func NewDBNodeServer(id string) *DBNodeServer {
	return &DBNodeServer{
		entityID: id,
		data:     make(map[string]*pb.Offer),
		stopCh:   make(chan struct{}),
	}
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

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

func registerWithBroker(client pb.EntityManagementClient, server *DBNodeServer) {
	fmt.Println("Registrando con Broker…")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dockerServiceName := strings.ToLower(server.entityID)
	addressToRegister := dockerServiceName + *dbNodePort

	req := &pb.RegistrationRequest{
		EntityId:   server.entityID,
		EntityType: "DBNode",
		Address:    addressToRegister,
	}

	resp, err := client.RegisterEntity(ctx, req)
	if err != nil || !resp.GetSuccess() {
		fmt.Printf("❌ Registro con broker falló: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Broker respondió: success=%t, msg=%s\n", resp.GetSuccess(), resp.GetMessage())
}

func Resincronizar(myID string, s *DBNodeServer) {
	addrs := peersFor(myID)
	for _, addr := range addrs {
		fmt.Printf("[%s] Resincronizando desde %s …\n", myID, addr)
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		client := pb.NewDBNodeClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		resp, err := client.GetOfferHistory(ctx, &pb.RecoveryRequest{RequestingNodeId: myID})
		cancel()
		conn.Close()
		if err != nil || resp == nil {
			continue
		}

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
		fmt.Printf("✅ [%s] Sync desde %s | nuevas=%d | total=%d\n", myID, addr, added, total)
		return
	}
	fmt.Printf("[%s] ⚠️ No se pudo resincronizar con ningún peer\n", myID)
}

// -------------------------------------------------------------------------
// Fallos automáticos (10% cada 25s, 15s caído)
// -------------------------------------------------------------------------

const (
	FailureCheckInterval = 25 * time.Second
	FailureProbability   = 0.10
	FailureDuration      = 15 * time.Second
)

func (s *DBNodeServer) IniciarDaemonDeFallos() {
	ticker := time.NewTicker(FailureCheckInterval)
	defer ticker.Stop()

	fmt.Printf("[%s] 🛠️ Daemon de fallos: prob=%.0f%%, caída=%s, cada=%s\n",
		s.entityID, FailureProbability*100, FailureDuration, FailureCheckInterval)

	time.Sleep(time.Duration(rand.Intn(5)+1) * time.Second)

	for {
		select {
		case <-s.stopCh:
			fmt.Printf("[%s] 📴 Daemon de fallos detenido por finalización\n", s.entityID)
			return
		case <-ticker.C:
			failMu.Lock()
			down := isFailing
			failMu.Unlock()
			if down {
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

					fmt.Printf("🛑 [%s] CAÍDA INESPERADA… (%s)\n", s.entityID, FailureDuration)
					time.Sleep(FailureDuration)

					failMu.Lock()
					isFailing = false
					failMu.Unlock()
					fmt.Printf("✅ [%s] LEVANTADO… resincronizando\n", s.entityID)

					Resincronizar(s.entityID, s)
				}()
			}
		}
	}
}

// -------------------------------------------------------------------------
// Implementación servicios
// -------------------------------------------------------------------------

func (s *DBNodeServer) StoreOffer(ctx context.Context, offer *pb.Offer) (*pb.StoreOfferResponse, error) {
	failMu.Lock()
	failing := isFailing
	failMu.Unlock()
	if failing {
		return &pb.StoreOfferResponse{Success: false, Message: s.entityID + " en fallo"}, nil
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

func (s *DBNodeServer) GetOfferHistory(ctx context.Context, req *pb.RecoveryRequest) (*pb.RecoveryResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*pb.Offer, 0, len(s.data))
	for _, of := range s.data {
		out = append(out, of)
	}
	fmt.Printf("📦 [%s] Enviando historial a %s (ofertas=%d)\n", s.entityID, req.GetRequestingNodeId(), len(out))
	return &pb.RecoveryResponse{Offers: out}, nil
}

// Finalización: detener daemon y limpiar estado
func (s *DBNodeServer) InformarFinalizacion(ctx context.Context, req *pb.EndingNotify) (*pb.EndingConfirm, error) {
	if !req.GetFin() {
		return &pb.EndingConfirm{Bdconfirm: false}, nil
	}
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
	failMu.Lock()
	isFailing = false
	failMu.Unlock()
	return &pb.EndingConfirm{Bdconfirm: true}, nil
}

// -------------------------------------------------------------------------
// main
// -------------------------------------------------------------------------

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	dbServer := NewDBNodeServer(*dbNodeID)

	conn, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("No se logró conexión con broker: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	emClient := pb.NewEntityManagementClient(conn)
	registerWithBroker(emClient, dbServer)

	fmt.Printf("[%s] Escuchando en %s…\n", *dbNodeID, *dbNodePort)
	lis, err := net.Listen("tcp", *dbNodePort)
	if err != nil {
		fmt.Printf("Listen %s falló: %v\n", *dbNodePort, err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	pb.RegisterEntityManagementServer(s, dbServer)
	pb.RegisterDBNodeServer(s, dbServer)
	pb.RegisterFinalizacionServer(s, dbServer)

	go dbServer.IniciarDaemonDeFallos()

	if err := s.Serve(lis); err != nil {
		fmt.Printf("Serve falló: %v\n", err)
		os.Exit(1)
	}
}
