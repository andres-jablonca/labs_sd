package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
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
	Red          = "\033[31m"
	Green        = "\033[32m"
	Yellow       = "\033[33m"
	Blue         = "\033[34m"
	Reset        = "\033[0m"
	prefsCSVPath = "Consumidores/consumidores.csv"
)

var (
	isFailing bool
	failMu    sync.Mutex
)

var (
	entityID       = flag.String("id", "C1-1", "ID único del consumidor.")
	entityPort     = flag.String("port", ":50071", "Puerto local gRPC del consumidor.")
	strictMismatch = flag.Bool("strict_mismatch", true, "Si true, ofertas no relevantes responden success=false al broker.")
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
	fecha     string
	producto  string
	precio    int64
	tienda    string
	categoria string
}

var RegistroOfertas []Oferta

// -------------------- Preferencias locales (mismo modelo del broker) --------------------

type Preference struct {
	Categories map[string]bool
	Stores     map[string]bool
	MaxPrice   int64 // -1 => sin límite
}

var (
	myPrefs     *Preference
	myPrefsOnce sync.Once
)

func NewConsumerServer(id string) *ConsumerServer {
	return &ConsumerServer{entityID: id, stopCh: make(chan struct{})}
}

func GenerarNombresCSV(entityID string) string {
	return fmt.Sprintf("%s/ofertas_%s.csv", outputDir, entityID)
}

// --------- Carga de preferencias (replica de broker.loadConsumerPreferences para 1 ID) ---------

func loadMyPreferences(path, id string) (*Preference, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error al abrir %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	if _, err := r.Read(); err != nil { // lee cabecera
		return nil, fmt.Errorf("error leyendo cabecera de %s: %w", path, err)
	}

	for {
		rec, err := r.Read()
		if err != nil {
			break
		}
		if len(rec) < 4 {
			continue
		}
		if strings.TrimSpace(rec[0]) != id {
			continue
		}

		categories := make(map[string]bool)
		if strings.ToLower(rec[1]) != "null" && rec[1] != "" {
			for _, cat := range strings.Split(rec[1], ";") {
				categories[strings.TrimSpace(cat)] = true
			}
		}
		stores := make(map[string]bool)
		if strings.ToLower(rec[2]) != "null" && rec[2] != "" {
			for _, st := range strings.Split(rec[2], ";") {
				stores[strings.TrimSpace(st)] = true
			}
		}
		var maxPrice int64 = -1
		if strings.ToLower(rec[3]) != "null" && rec[3] != "" {
			if p, err := strconv.ParseInt(rec[3], 10, 64); err == nil {
				maxPrice = p
			}
		}

		return &Preference{
			Categories: categories,
			Stores:     stores,
			MaxPrice:   maxPrice,
		}, nil
	}
	return nil, nil
}

// --------- Comparador de relevancia (copiado de broker.isRelevant) ---------

func isRelevantLocal(offer *pb.Offer, prefs *Preference) bool {
	if prefs == nil {
		return true
	}
	if len(prefs.Stores) > 0 {
		if _, ok := prefs.Stores[offer.GetTienda()]; !ok {
			return false
		}
	}
	if len(prefs.Categories) > 0 {
		if _, ok := prefs.Categories[offer.GetCategoria()]; !ok {
			return false
		}
	}
	if prefs.MaxPrice != -1 && offer.GetPrecio() > prefs.MaxPrice {
		return false
	}
	return true
}

// -------------------------------------------------------------------------
// CSV helpers
// -------------------------------------------------------------------------

func initCSVIfNeeded(fileName string) error {
	dir := ""
	parts := strings.Split(fileName, "/")
	if len(parts) > 1 {
		dir = strings.Join(parts[:len(parts)-1], "/")
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		f, err := os.Create(fileName)
		if err != nil {
			return err
		}
		defer f.Close()
		w := csv.NewWriter(f)
		defer w.Flush()
		return w.Write([]string{"oferta_id", "fecha", "producto", "precio", "tienda", "categoria"})
	}
	return nil
}

func EscribirCSV(fileName string, ofertas []Oferta) error {
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
	_ = w.Write([]string{"oferta_id", "fecha", "producto", "precio", "tienda", "categoria"})
	for _, o := range ofertas {
		if err := w.Write([]string{
			o.id,
			o.fecha,
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
		time.Sleep(600 * time.Millisecond)
		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
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
// Recepción de Ofertas (filtrado 1:1 con broker)
// -------------------------------------------------------------------------

func (s *ConsumerServer) ReceiveOffer(ctx context.Context, offer *pb.Offer) (*pb.ConsumerResponse, error) {
	failMu.Lock()
	failing := isFailing
	failMu.Unlock()
	if failing {
		return &pb.ConsumerResponse{Success: false, Message: "Consumidor en fallo; descartada"}, nil
	}

	// Verificación idéntica a la del broker
	if !isRelevantLocal(offer, myPrefs) {
		fmt.Printf(Red+"[%s] OFERTA RECIBIDA PERO NO COINCIDE CON PREFERENCIAS -> ignorada | [%s | %s | %d]\n"+Reset,
			s.entityID, offer.GetCategoria(), offer.GetTienda(), offer.GetPrecio())
		if *strictMismatch {
			return &pb.ConsumerResponse{Success: false, Message: "Irrelevante según preferencias"}, nil
		}
		return &pb.ConsumerResponse{Success: true, Message: "Irrelevante (no almacenada)"}, nil
	}

	fmt.Printf("[%s] NUEVA OFERTA RECIBIDA: %s (ID: %s,Precio: %d, Tienda: %s, Categoría: %s)\n",
		s.entityID, offer.GetOfertaId(), offer.GetProducto(), offer.GetPrecio(), offer.GetTienda(), offer.GetCategoria())

	RegistroOfertas = append(RegistroOfertas, Oferta{
		id:        offer.GetOfertaId(),
		fecha:     offer.GetFecha(),
		producto:  offer.GetProducto(),
		precio:    offer.GetPrecio(),
		tienda:    offer.GetTienda(),
		categoria: offer.GetCategoria(),
	})

	return &pb.ConsumerResponse{Success: true, Message: "OK memoria"}, nil
}

// -------------------------------------------------------------------------
// Resincronización
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
		// El broker ya filtra con su isRelevant; mantenemos la misma verificación por simetría
		if isRelevantLocal(of, myPrefs) {
			RegistroOfertas = append(RegistroOfertas, Oferta{
				id:        of.GetOfertaId(),
				fecha:     of.GetFecha(),
				producto:  of.GetProducto(),
				precio:    of.GetPrecio(),
				tienda:    of.GetTienda(),
				categoria: of.GetCategoria(),
			})
			added++
		}
	}
	fmt.Printf(Green+"[%s] Resincronización completa: Ofertas recibidas=%d | Ofertas nuevas=%d | Ofertas totales=%d\n"+Reset,
		myID, len(resp.GetOffers()), added, len(RegistroOfertas))
}

// -------------------------------------------------------------------------
// Daemon de fallos
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
					select {
					case <-s.stopCh:
						fmt.Printf(Yellow+"[%s] Sistema finalizado - omitiendo resincronización\n"+Reset, s.entityID)
					default:
						fmt.Printf(Green+"[%s] LEVANTADO NUEVAMENTE, SOLICITANDO RESINCRINIZACIÓN...\n"+Reset, s.entityID)
						Resincronizar(s.entityID, s)
					}
					failMu.Unlock()
				}()
			}
		}
	}
}

// -------------------------------------------------------------------------
// Finalización
// -------------------------------------------------------------------------

func (s *ConsumerServer) InformarFinalizacion(ctx context.Context, req *pb.EndingNotify) (*pb.EndingConfirm, error) {
	failMu.Lock()
	failing := isFailing
	failMu.Unlock()

	if failing {
		fmt.Printf(Red+"[%s] CAÍDO. No genera CSV final\n"+Reset, s.entityID)
		s.ShutdownSoon()
		return &pb.EndingConfirm{Consumerconfirm: false}, nil
	}

	if !req.GetFin() {
		return &pb.EndingConfirm{Consumerconfirm: false}, nil
	}

	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
	failMu.Lock()
	isFailing = false
	failMu.Unlock()

	fn := GenerarNombresCSV(s.entityID)
	if err := EscribirCSV(fn, RegistroOfertas); err != nil {
		fmt.Printf("[%s] Error al generar CSV final: %v\n", s.entityID, err)
		s.ShutdownSoon()
		return &pb.EndingConfirm{Consumerconfirm: false}, nil
	}
	fmt.Printf(Green+"[%s] CSV final generado con %d ofertas\n"+Reset, s.entityID, len(RegistroOfertas))

	s.ShutdownSoon()
	return &pb.EndingConfirm{Consumerconfirm: true}, nil
}

// -------------------------------------------------------------------------
// main
// -------------------------------------------------------------------------

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	// Cargar preferencias al inicio (opcional; igual se hace lazy en ReceiveOffer)
	if p, err := loadMyPreferences(prefsCSVPath, *entityID); err != nil {
		fmt.Printf(Red+"[%s] Error preferencias inicial: %v\n"+Reset, *entityID, err)
	} else {
		myPrefs = p
		fmt.Printf("Preferencias cargadas desde archivo csv.\n")
	}

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
