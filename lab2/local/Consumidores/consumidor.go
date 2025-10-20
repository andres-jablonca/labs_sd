package main

import (
	"context"
	"encoding/csv"
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
// Constantes / Globals
// -------------------------------------------------------------------------

const brokerAddress = "broker:50095"
const outputDir = "/app/output"

const (
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Reset  = "\033[0m"
)

var (
	isFailing bool
	failMu    sync.Mutex
)

var (
	entityID   = flag.String("id", "C1-1", "ID único del consumidor.")
	entityPort = flag.String("port", ":50071", "Puerto local gRPC del consumidor.")
)

// -------------------------------------------------------------------------

type ConsumerServer struct {
	pb.UnimplementedConsumerServer
	pb.UnimplementedFinalizacionServer

	entityID   string
	stopCh     chan struct{}
	grpcServer *grpc.Server
}

type Oferta struct {
	id        string
	producto  string
	precio    int64
	tienda    string
	categoria string
}

var RegistroOfertas []Oferta

func NewConsumerServer(id string) *ConsumerServer {
	return &ConsumerServer{entityID: id, stopCh: make(chan struct{})}
}

func getCSVFileName(entityID string) string {
	return fmt.Sprintf("%s/registro_ofertas_%s.csv", outputDir, entityID)
}

// Crear CSV (cabecera) si no existe
func initCSVIfNeeded(fileName string) error {
	// Extraer el directorio de la ruta utilizando strings.Split
	dir := ""
	parts := strings.Split(fileName, "/")
	if len(parts) > 1 {
		dir = strings.Join(parts[:len(parts)-1], "/")
	}

	// Crear directorio si no existe
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	// Crear el archivo si no existe
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		f, err := os.Create(fileName)
		if err != nil {
			return err
		}
		defer f.Close()
		w := csv.NewWriter(f)
		defer w.Flush()
		return w.Write([]string{"producto", "precio", "tienda", "categoria"})
	}
	return nil
}

// Volcar TODAS las ofertas de memoria al CSV (reemplaza archivo)
func dumpAllToCSV(fileName string, ofertas []Oferta) error {
	if err := initCSVIfNeeded(fileName); err != nil {
		return err
	}
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"producto", "precio", "tienda", "categoria"})
	for _, o := range ofertas {
		if err := w.Write([]string{
			o.producto,
			fmt.Sprintf("%d", o.precio),
			o.tienda,
			o.categoria,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *ConsumerServer) ShutdownSoon() {
	go func() {
		// Espera corta para asegurar que el EndingConfirm ya se envió
		time.Sleep(600 * time.Millisecond)
		if s.grpcServer != nil {
			s.grpcServer.GracefulStop() // cierra listeners y deja terminar RPCs en curso
		}
	}()
}

// -------------------------------------------------------------------------
// Registro con Broker
// -------------------------------------------------------------------------

func registerWithBroker(client pb.EntityManagementClient) {
	fmt.Printf("[%s] Coordinando el registro con el Broker...\n", *entityID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dockerServiceName := os.Getenv("HOSTNAME")
	if dockerServiceName == "" {
		dockerServiceName = strings.ToLower(*entityID)
	}
	addressToRegister := dockerServiceName + *entityPort

	req := &pb.RegistrationRequest{
		EntityId:   *entityID,
		EntityType: "Consumer",
		Address:    addressToRegister,
	}
	resp, err := client.RegisterEntity(ctx, req)
	if err != nil || !resp.GetSuccess() {
		fmt.Printf("[%s] Registro fallido: %v\n", *entityID, err)
		os.Exit(1)
	}
	fmt.Printf("[%s] Respuesta del Broker: %s\n", *entityID, resp.GetMessage())
}

// -------------------------------------------------------------------------
// Recepción de Ofertas (solo memoria; CSV se genera al final)
// -------------------------------------------------------------------------

func (s *ConsumerServer) ReceiveOffer(ctx context.Context, offer *pb.Offer) (*pb.ConsumerResponse, error) {
	failMu.Lock()
	failing := isFailing
	failMu.Unlock()
	if failing {
		return &pb.ConsumerResponse{Success: false, Message: "Consumidor en fallo; descartada"}, nil
	}

	fmt.Printf("[%s] NUEVA OFERTA RECIBIDA: %s (Precio: %d, Tienda: %s, Categoría: %s)\n",
		s.entityID, offer.GetProducto(), offer.GetPrecio(), offer.GetTienda(), offer.GetCategoria())

	RegistroOfertas = append(RegistroOfertas, Oferta{
		id:        offer.GetOfertaId(),
		producto:  offer.GetProducto(),
		precio:    offer.GetPrecio(),
		tienda:    offer.GetTienda(),
		categoria: offer.GetCategoria(),
	})

	return &pb.ConsumerResponse{Success: true, Message: "OK memoria"}, nil
}

// -------------------------------------------------------------------------
// Resincronización (solo cuando vuelve de caída)
// -------------------------------------------------------------------------

func Resincronizar(myID string, s *ConsumerServer) {
	conn, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[%s] no conecta con broker para recovery: %v\n", myID, err)
		return
	}
	defer conn.Close()

	client := pb.NewRecoveryClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetFilteredHistory(ctx, &pb.HistoryRequest{ConsumerId: myID})
	if err != nil || resp == nil {
		fmt.Printf("[%s] Resincronización falló: %v\n", myID, err)
		return
	}

	known := make(map[string]struct{}, len(RegistroOfertas))
	for _, o := range RegistroOfertas {
		known[o.id] = struct{}{}
	}

	added := 0
	for _, of := range resp.GetOffers() {
		if of == nil || of.GetOfertaId() == "" {
			continue
		}
		if _, ok := known[of.GetOfertaId()]; ok {
			continue
		}
		RegistroOfertas = append(RegistroOfertas, Oferta{
			id:        of.GetOfertaId(),
			producto:  of.GetProducto(),
			precio:    of.GetPrecio(),
			tienda:    of.GetTienda(),
			categoria: of.GetCategoria(),
		})
		added++
	}
	fmt.Printf(Green+"[%s] Resincronización completa: Ofertas recibidas=%d | Ofertas nuevas=%d | Ofertas totales=%d\n"+Reset,
		myID, len(resp.GetOffers()), added, len(RegistroOfertas))
}

// -------------------------------------------------------------------------
// Daemon de fallos (prob. 8% cada 25s, 10s de caída)
// -------------------------------------------------------------------------

const (
	FailureCheckInterval = 25 * time.Second
	FailureProbability   = 0.15
	FailureDuration      = 15 * time.Second
)

func (s *ConsumerServer) reportarCaidaABroker() {
	conn, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[%s] Error conectando al broker para reportar caída: %v\n", s.entityID, err)
		return
	}
	defer conn.Close()

	client := pb.NewCaidaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.FailNotify{
		Id:   s.entityID,
		Type: "Consumer",
	}

	resp, err := client.InformarCaida(ctx, req)
	if err != nil {
		fmt.Printf("[%s] Error reportando caída al broker: %v\n", s.entityID, err)
		return
	}

	if resp.GetAck() {

	}
}

func (s *ConsumerServer) IniciarDaemonDeFallos() {
	ticker := time.NewTicker(FailureCheckInterval)
	defer ticker.Stop()

	fmt.Printf("[%s] Daemon de fallos: prob=%.0f%%, caída=%s, cada=%s\n",
		s.entityID, FailureProbability*100, FailureDuration, FailureCheckInterval)

	time.Sleep(time.Duration(rand.Intn(5)+1) * time.Second)

	for {
		select {
		case <-s.stopCh:
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

					fmt.Printf(Red+"[%s] CAÍDA INESPERADA... (%s)\n"+Reset, s.entityID, FailureDuration)
					s.reportarCaidaABroker()
					time.Sleep(FailureDuration)

					failMu.Lock()
					isFailing = false
					// Marcar que se está en proceso de recuperación
					select {
					case <-s.stopCh:
						// Si ya se inició la finalización, no intentar resincronizar
						fmt.Printf(Yellow+"[%s] Sistema finalizado - omitiendo resincronización\n"+Reset, s.entityID)
					default:
						// Solo resincronizar si el sistema no ha finalizado
						fmt.Printf(Green+"[%s] LEVANTADO NUEVAMENTE, SOLICITANDO RESINCRINIZACIÓN...\n"+Reset, s.entityID)
						Resincronizar(s.entityID, s)
					}
					failMu.Unlock()
				}()
			}
		}
	}
}

/// -------------------------------------------------------------------------
// Finalización (Broker -> Consumer)
// -------------------------------------------------------------------------

func (s *ConsumerServer) InformarFinalizacion(ctx context.Context, req *pb.EndingNotify) (*pb.EndingConfirm, error) {
	// Verificar si el consumidor estuvo caído durante la finalización
	// Si está caído actualmente O si se recuperó recientemente durante la ventana de finalización
	failMu.Lock()
	failing := isFailing
	failMu.Unlock()

	if failing {
		fmt.Printf(Red+"[%s] CONSUMIDOR CAÍDO - No se genera CSV final\n"+Reset, s.entityID)
		// Programa el apagado y retorna confirmación (false)
		s.ShutdownSoon()
		return &pb.EndingConfirm{Consumerconfirm: false}, nil
	}

	if !req.GetFin() {
		// No es fin real; no apagamos.
		return &pb.EndingConfirm{Consumerconfirm: false}, nil
	}

	// detener daemon y limpiar fallo
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
	failMu.Lock()
	isFailing = false
	failMu.Unlock()

	fn := getCSVFileName(s.entityID)
	if err := dumpAllToCSV(fn, RegistroOfertas); err != nil {
		fmt.Printf("[%s] Error al generar CSV final: %v\n", s.entityID, err)
		// Programa apagado aunque haya fallado la escritura (tu requerimiento es terminar de todos modos)
		s.ShutdownSoon()
		return &pb.EndingConfirm{Consumerconfirm: false}, nil
	}
	fmt.Printf(Green+"[%s] CSV final generado con %d ofertas\n"+Reset, s.entityID, len(RegistroOfertas))

	// Programa el apagado y retorna confirmación (true)
	s.ShutdownSoon()
	return &pb.EndingConfirm{Consumerconfirm: true}, nil
}

// -------------------------------------------------------------------------
// main
// -------------------------------------------------------------------------

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	consumer := NewConsumerServer(*entityID)

	connReg, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[%s] No conecta broker registro: %v\n", *entityID, err)
		os.Exit(1)
	}
	defer connReg.Close()
	clientReg := pb.NewEntityManagementClient(connReg)
	registerWithBroker(clientReg)

	lis, err := net.Listen("tcp", *entityPort)
	if err != nil {
		fmt.Printf("[%s] Listen %s error: %v\n", *entityID, *entityPort, err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	consumer.grpcServer = s
	pb.RegisterConsumerServer(s, consumer)
	pb.RegisterFinalizacionServer(s, consumer)

	go consumer.IniciarDaemonDeFallos()

	fmt.Printf("[%s] Listo para recibir ofertas en %s...\n", *entityID, *entityPort)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("[%s] Serve error: %v\n", *entityID, err)
		os.Exit(1)
	}
}
