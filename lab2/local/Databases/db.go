package main

import (
    "context"
    "flag"
    "fmt"
    "net"
    "os"
    "strings" // ⚠️ Importar strings
    "sync"
    "time"

    pb "lab2/proto"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

// -------------------------------------------------------------------------
// --- 1. Variables Globales y Estructuras (Usa flags, no constantes fijas)
// -------------------------------------------------------------------------

// Dirección del Broker usando su nombre de servicio Docker: "broker"
const brokerAddress = "broker:50095" 

var (
    dbNodeID   = flag.String("id", "DB1", "ID único del nodo DB (e.g., DB1, DB2, DB3).")
    dbNodePort = flag.String("port", ":50061", "Puerto local del servidor gRPC del Nodo DB.")
)

// DBNodeServer implementa el servicio DBNode que el Broker llama (Fase 3).
type DBNodeServer struct {
    pb.UnimplementedEntityManagementServer
    pb.UnimplementedDBNodeServer 

    entityID string
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
// --- 2. Fase 1: Registro (CORREGIDO para usar la dirección de Docker Compose)
// -------------------------------------------------------------------------

func registerWithBroker(client pb.EntityManagementClient, server *DBNodeServer) {
    fmt.Printf("Coordinando el registro con el Broker...\n")

    ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
    defer cancel()
    
    // ⚠️ CORRECCIÓN CLAVE: Usar el ID de la entidad en minúsculas
    // para que coincida con el nombre del servicio Docker (ej: DB1 -> db1).
    dockerServiceName := strings.ToLower(server.entityID) 
    addressToRegister := dockerServiceName + *dbNodePort // ej: "db1:50061"

    req := &pb.RegistrationRequest{
        EntityId:   server.entityID, 
        EntityType: "DBNode",
        Address:    addressToRegister, // ⬅️ Usamos la dirección de red interna de Docker
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
// --- 4. Función Principal (Con servidor gRPC para recibir peticiones)
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

    fmt.Printf("Listo para almacenar ofertas en %s...\n", *dbNodePort)
    if err := s.Serve(lis); err != nil {
        fmt.Printf("Fallo al servir: %v\n", err)
        os.Exit(1)
    }
}