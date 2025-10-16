package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	// Importar para manejar CSV
	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// -------------------------------------------------------------------------
// --- 1. Constantes y Variables Globales ---
// -------------------------------------------------------------------------

const brokerAddress = "broker:50095"
const outputDir = "/app/output"

var (
	isFailing bool
	failMu    sync.Mutex
)

func getCSVFileName(entityID string) string {
	// 💡 CAMBIO: Ahora usamos outputDir + el nombre del archivo
	return fmt.Sprintf("%s/registro_ofertas_%s.csv", outputDir, entityID)
}

// consumidor.go (Añadir o modificar helpers de CSV)

// writeOfferToCSV añade una oferta al archivo CSV del consumidor.
func (s *ConsumerServer) writeOfferToCSV(fileName string, offer *pb.Offer) error {
	// 1. Abrir el archivo en modo append (agregar)
	// Usamos O_APPEND para no sobrescribir el contenido.
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error al abrir el archivo CSV: %v", err)
	}
	defer file.Close()

	// 2. Crear el escritor CSV
	writer := csv.NewWriter(file)
	defer writer.Flush() // Asegura que los datos se escriban al disco al salir de la función.

	// 3. Preparar los datos de la oferta
	// Estos campos deben coincidir con la cabecera definida en initCSVFile.
	record := []string{
		offer.GetProducto(),
		fmt.Sprintf("%d", offer.GetPrecio()), // Convertir int64 a string
		offer.GetTienda(),
		offer.GetCategoria(),
		// Puedes añadir más campos del proto si los necesitas, como OfertaId, Stock, Descuento, etc.
	}

	// 4. Escribir el registro
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("error al escribir el registro CSV: %v", err)
	}

	return nil
}

var (
	entityID   = flag.String("id", "C1-1", "ID único del consumidor.")
	entityPort = flag.String("port", ":50071", "Puerto local del servidor gRPC del Consumidor.")
)

// ConsumerServer implementa el servicio Consumer que el Broker llama (Fase 4).
type ConsumerServer struct {
	pb.UnimplementedConsumerServer
	entityID string
}

type Oferta struct {
	producto  string
	precio    int64
	tienda    string
	categoria string
}

var RegistroOfertas []Oferta

func NewConsumerServer(id string) *ConsumerServer {
	return &ConsumerServer{
		entityID: id,
	}
}

// -------------------------------------------------------------------------
// --- 2. Fase 1: Registro (CORREGIDO USANDO HOSTNAME) ---
// -------------------------------------------------------------------------

func registerWithBroker(client pb.EntityManagementClient) {
	fmt.Printf("[%s] Coordinando el registro con el Broker...\n", *entityID)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// 💡 CORRECCIÓN CLAVE: Usar la variable de entorno HOSTNAME para obtener el nombre del servicio Docker.
	dockerServiceName := os.Getenv("HOSTNAME")
	if dockerServiceName == "" {
		// Fallback si HOSTNAME no está definido (aunque en Docker Compose siempre debería estarlo)
		dockerServiceName = strings.ToLower(*entityID)
		fmt.Printf("[%s] ⚠️ Advertencia: HOSTNAME no definido. Usando ID en minúsculas: %s\n", *entityID, dockerServiceName)
	}

	// La dirección ahora es el nombre del servicio + el puerto (ej: consumer-group-1:50071)
	addressToRegister := dockerServiceName + *entityPort

	fmt.Printf("[%s] Registrando con Address: %s\n", *entityID, addressToRegister)

	req := &pb.RegistrationRequest{
		EntityId:   *entityID,
		EntityType: "Consumer",
		Address:    addressToRegister,
	}

	resp, err := client.RegisterEntity(ctx, req)
	if err != nil {
		fmt.Printf("[%s] ❌ No se logró conectar con el broker: %v\n", *entityID, err)
		os.Exit(1)
	}

	fmt.Printf("[%s] Respuesta del Broker: %s\n", *entityID, resp.Message)

	if !resp.Success {
		os.Exit(1)
	}
}

// -------------------------------------------------------------------------
// --- 3. Fase 4: Recepción de Ofertas (Implementación gRPC) ---
// -------------------------------------------------------------------------

// ReceiveOffer es llamado por el Broker para notificar una nueva oferta.
func (s *ConsumerServer) ReceiveOffer(ctx context.Context, offer *pb.Offer) (*pb.ConsumerResponse, error) {
	fmt.Printf("[%s] 🎉 NUEVA OFERTA: %s (P: %d, T: %s, Cat: %s)\n",
		s.entityID,
		offer.GetProducto(),
		offer.GetPrecio(),
		offer.GetTienda(),
		offer.GetCategoria())

	var ofertaaux Oferta
	ofertaaux.producto = offer.GetProducto()
	ofertaaux.precio = offer.GetPrecio()
	ofertaaux.tienda = offer.GetTienda()
	ofertaaux.categoria = offer.GetCategoria()
	RegistroOfertas = append(RegistroOfertas, ofertaaux)

	// --- Lógica para escribir en el CSV ---

	// 2. 💡 Lógica de Escritura CSV (Solo si la oferta pasa los filtros)
	fileName := getCSVFileName(s.entityID)
	if err := s.writeOfferToCSV(fileName, offer); err != nil {
		fmt.Printf("🛑 [%s] Error al escribir la oferta %s en CSV: %v\n", s.entityID, offer.GetOfertaId(), err)

		// Aunque la escritura falló, gRPC debe devolver un mensaje para no bloquear al Broker.
		return &pb.ConsumerResponse{
			Success: false,
			Message: fmt.Sprintf("Error interno al escribir en CSV: %v", err),
		}, nil
	}

	// Lógica de filtrado (opcional, por ahora solo confirmamos recepción)
	return &pb.ConsumerResponse{
		Success: true,
		Message: fmt.Sprintf("Oferta %s recibida y procesada por %s.\n", offer.GetOfertaId(), s.entityID),
	}, nil
}

func Resincronizar(myID string, s *ConsumerServer) {
	// Falta implementar que solicite historico a broker y que broker lo solicite a DBs con R>=2
	broker_addr := "broker:50095"
	conn, err := grpc.Dial(broker_addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[%s] no conecta con %s: %v\n", myID, broker_addr, err)
	}
	conn.Close()
}

const (
	FailureCheckInterval = 25 * time.Second
	FailureProbability   = 0.10
	FailureDuration      = 10 * time.Second
)

func (s *ConsumerServer) IniciarDaemonDeFallos() {
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

				fmt.Printf("🛑 [%s] CAÍDA INESPERADA... (Duración de %s s)\n", s.entityID, FailureDuration)
				time.Sleep(FailureDuration)

				failMu.Lock()
				isFailing = false
				failMu.Unlock()
				fmt.Printf("✅ [%s] LEVANTÁNDOSE NUEVAMENTE... SOLICITANDO HISTÓRICO DE OFERTAS... (No implementado aún :v)\n", s.entityID)

				Resincronizar(s.entityID, s)
			}()
		}
	}
}

// -------------------------------------------------------------------------
// --- 4. Función Principal (Con servidor gRPC para recibir peticiones) ---
// -------------------------------------------------------------------------

func main() {
	flag.Parse()
	consumerServer := NewConsumerServer(*entityID)

	// 1. Conexión y Registro con el Broker (Fase 1)
	connReg, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[%s] No se logró conexión con broker para el registro: %v\n", *entityID, err)
		os.Exit(1)
	}
	defer connReg.Close()

	clientReg := pb.NewEntityManagementClient(connReg)
	registerWithBroker(clientReg)

	fmt.Printf("[%s] Registro completo. Empezando a escuchar en %s...\n", *entityID, *entityPort)

	// 2. Inicia el servidor gRPC del Consumidor (para recibir notificaciones del Broker)
	lis, err := net.Listen("tcp", *entityPort)
	if err != nil {
		fmt.Printf("[%s] Fallo al escuchar en %s: %v\n", *entityID, *entityPort, err)
		os.Exit(1)
	}

	s := grpc.NewServer()

	// Registrar el servicio Consumer (Fase 4)
	pb.RegisterConsumerServer(s, consumerServer)

	go consumerServer.IniciarDaemonDeFallos()

	fmt.Printf("[%s] Listo para recibir ofertas en %s...\n", *entityID, *entityPort)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("[%s] Fallo al servir: %v\n", *entityID, err)
		os.Exit(1)
	}
}
