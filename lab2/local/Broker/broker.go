// broker.go
package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"maps"
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
// Constantes
// -------------------------------------------------------------------------

const (
	brokerPort = ":50095"
	N          = 3 // réplicas
	W          = 2 // confirmaciones requeridas

	consumerPreferencesFile = "Broker/consumidores.csv"
)

// -------------------------------------------------------------------------
// Métricas (globales simples para imprimir al final)
// -------------------------------------------------------------------------

var (
	contador_registrados int64
	ofertas_parisio      int64
	ofertas_riploy       int64
	ofertas_falabellox   int64
	terminacionMu        sync.Mutex
	sistemaTerminado     bool

	aceptadas_totales   int64
	escrituras_totales  int64
	escrituras_exitosas int64

	nodos_caidos_al_finalizar int64

	caidasPorNodo        = map[string]int{} // fallos al escribir en nodo
	caidasPorConsumidor  = map[string]int{} // fallos al notificar a consumidor
	ofertasPorConsumidor = map[string]int{} // notificaciones exitosas

	resyncOKPorConsumidor = map[string]int{}  // Recovery exitoso (R=2) por consumidor
	confirmCSVConsumidor  = map[string]bool{} // si el consumer confirmó CSV final
)

// -------------------------------------------------------------------------
// ESTRUCTURAS
// -------------------------------------------------------------------------

type Entity struct {
	ID      string
	Type    string
	Address string
}

type ConsumerPreference struct {
	ID         string
	Categories map[string]bool
	Stores     map[string]bool
	MaxPrice   int64
}

type BrokerServer struct {
	pb.UnimplementedEntityManagementServer
	pb.UnimplementedOfferSubmissionServer
	pb.UnimplementedConfirmarInicioServer
	pb.UnimplementedRecoveryServer

	entities      map[string]Entity
	dbNodes       map[string]Entity
	consumers     map[string]Entity
	consumerPrefs map[string]ConsumerPreference
	mu            sync.Mutex
}

func NewBrokerServer() *BrokerServer {
	return &BrokerServer{
		entities:      make(map[string]Entity),
		dbNodes:       make(map[string]Entity),
		consumers:     make(map[string]Entity),
		consumerPrefs: make(map[string]ConsumerPreference),
	}
}

const (
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Reset  = "\033[0m"
)

// -------------------------------------------------------------------------
// Preferencias y filtrado
// -------------------------------------------------------------------------

func (s *BrokerServer) loadConsumerPreferences() error {
	file, err := os.Open(consumerPreferencesFile)
	if err != nil {
		return fmt.Errorf("error al abrir %s: %w", consumerPreferencesFile, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	if _, err := reader.Read(); err != nil {
		if err == io.EOF {
			return fmt.Errorf("archivo %s vacío", consumerPreferencesFile)
		}
		return fmt.Errorf("error cabecera %s: %w", consumerPreferencesFile, err)
	}

	prefsMap := make(map[string]ConsumerPreference)
	recordsRead := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(record) < 4 {
			continue
		}

		id := record[0]

		categories := make(map[string]bool)
		if strings.ToLower(record[1]) != "null" && record[1] != "" {
			for cat := range strings.SplitSeq(record[1], ";") {
				categories[strings.TrimSpace(cat)] = true
			}
		}
		stores := make(map[string]bool)
		if strings.ToLower(record[2]) != "null" && record[2] != "" {
			for st := range strings.SplitSeq(record[2], ";") {
				stores[strings.TrimSpace(st)] = true
			}
		}
		var maxPrice int64 = -1
		if strings.ToLower(record[3]) != "null" && record[3] != "" {
			if p, err := strconv.ParseInt(record[3], 10, 64); err == nil {
				maxPrice = p
			}
		}

		prefsMap[id] = ConsumerPreference{
			ID:         id,
			Categories: categories,
			Stores:     stores,
			MaxPrice:   maxPrice,
		}
		recordsRead++
	}

	s.mu.Lock()
	s.consumerPrefs = prefsMap
	s.mu.Unlock()

	fmt.Printf("Preferencias de consumidores cargadas.\n")
	return nil
}

func (s *BrokerServer) isRelevant(offer *pb.Offer, prefs ConsumerPreference) bool {
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
// Fase 1: Registro
// -------------------------------------------------------------------------

func (s *BrokerServer) RegisterEntity(ctx context.Context, req *pb.RegistrationRequest) (*pb.RegistrationResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := req.GetEntityId()
	if _, exists := s.entities[id]; exists {
		return &pb.RegistrationResponse{Success: false, Message: "Entidad ya registrada\n"}, nil
	}

	ent := Entity{
		ID:      id,
		Type:    req.GetEntityType(),
		Address: req.GetAddress(),
	}
	s.entities[id] = ent

	switch ent.Type {
	case "DBNode":
		s.dbNodes[id] = ent

	case "Consumer":
		if _, ok := s.consumerPrefs[id]; !ok {
			delete(s.entities, id)
			fmt.Printf(Red+"[Registro]"+Reset+" Consumidor %s RECHAZADO. No encontrado en consumidores.csv.\n", id)
			return &pb.RegistrationResponse{
				Success: false,
				Message: "Registro fallido. Su ID de consumidor no está en la lista de preferencias.\n",
			}, nil
		}
		s.consumers[id] = ent

	default:
	}

	fmt.Printf(Green+"[Registro]"+Reset+" %s registrad@ correctamente (%s) en %s. Total de registrados: %d\n",
		id, ent.Type, ent.Address, len(s.entities))

	contador_registrados++
	return &pb.RegistrationResponse{
		Success: true,
		Message: "Registro exitoso. Bienvenido al CyberDay!.\n",
	}, nil
}

func (s *BrokerServer) Confirmacion(ctx context.Context, _ *pb.ConfirmRequest) (*pb.ConfirmResponse, error) {
	if contador_registrados < 18 {
		return &pb.ConfirmResponse{Ready: false}, nil
	}
	return &pb.ConfirmResponse{Ready: true}, nil
}

// -------------------------------------------------------------------------
// Notificación a consumidores (filtrado por preferencias)
// -------------------------------------------------------------------------

func (s *BrokerServer) notifyConsumers(offer *pb.Offer) {
	s.mu.Lock()
	cons := make(map[string]Entity, len(s.consumers))
	maps.Copy(cons, s.consumers)
	prefsCopy := make(map[string]ConsumerPreference, len(s.consumerPrefs))
	maps.Copy(prefsCopy, s.consumerPrefs)
	s.mu.Unlock()

	var wg sync.WaitGroup
	var mu sync.Mutex
	countOK := 0

	for _, c := range cons {
		prefs, ok := prefsCopy[c.ID]
		if !ok || !s.isRelevant(offer, prefs) {
			continue
		}
		wg.Add(1)
		go func(consumer Entity) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
			defer cancel()

			conn, err := grpc.Dial(consumer.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				mu.Lock()
				caidasPorConsumidor[consumer.ID]++
				mu.Unlock()
				return
			}
			defer conn.Close()

			cli := pb.NewConsumerClient(conn)
			resp, err := cli.ReceiveOffer(ctx, offer)
			if err != nil || resp == nil || !resp.GetSuccess() {
				mu.Lock()
				caidasPorConsumidor[consumer.ID]++
				mu.Unlock()
				return
			}

			mu.Lock()
			ofertasPorConsumidor[consumer.ID]++
			countOK++
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	fmt.Printf(Blue+"\n[Notificación]"+Reset+" Oferta %s -> %d confirmaciones\n\n", offer.GetOfertaId(), countOK)
}

// -------------------------------------------------------------------------
// Fase 2 & 3: Escritura distribuida
// -------------------------------------------------------------------------

func (s *BrokerServer) SendOffer(ctx context.Context, offer *pb.Offer) (*pb.OfferSubmissionResponse, error) {
	terminacionMu.Lock()
	if sistemaTerminado {
		terminacionMu.Unlock()
		return &pb.OfferSubmissionResponse{Accepted: false, Message: "Cyberday finalizado", Termino: true}, nil
	}
	terminacionMu.Unlock()

	fmt.Printf(Yellow+"[Oferta"+Reset+" %s"+Yellow+"]"+Reset+" Iniciando escritura distribuida (N=%d, W=%d)\n", offer.GetOfertaId(), N, W)

	if len(s.dbNodes) < N {
		return &pb.OfferSubmissionResponse{
			Accepted: false,
			Message:  fmt.Sprintf("No hay N=%d DBs activas\n", N),
			Termino:  false,
		}, nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	confirmed := 0

	s.mu.Lock()
	nodes := make([]Entity, 0, len(s.dbNodes))
	for _, n := range s.dbNodes {
		nodes = append(nodes, n)
	}
	s.mu.Unlock()

	escrituras_totales += int64(len(nodes))

	for _, node := range nodes {
		wg.Add(1)
		go func(n Entity) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			conn, err := grpc.Dial(n.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				mu.Lock()
				caidasPorNodo[n.ID]++
				mu.Unlock()
				return
			}
			defer conn.Close()

			cli := pb.NewDBNodeClient(conn)
			resp, err := cli.StoreOffer(ctx, offer)
			if err != nil || resp == nil || !resp.GetSuccess() {
				mu.Lock()
				caidasPorNodo[n.ID]++
				mu.Unlock()
				return
			}
			mu.Lock()
			confirmed++
			mu.Unlock()
		}(node)
	}

	wg.Wait()
	if confirmed >= W {
		aceptadas_totales++
		escrituras_exitosas++
		go s.notifyConsumers(offer)

		terminacionMu.Lock()
		defer terminacionMu.Unlock()

		switch offer.GetTienda() {
		case "Parisio":
			ofertas_parisio++
		case "Falabellox":
			ofertas_falabellox++
		case "Riploy":
			ofertas_riploy++
		}

		if ofertas_falabellox >= 20 && ofertas_parisio >= 20 && ofertas_riploy >= 20 {
			fmt.Println("\n=======================================================")
			fmt.Println("🛑 Límite de ofertas alcanzado! Finalizando CyberDay...")
			fmt.Println("=======================================================")
			sistemaTerminado = true
			s.notifyFinalizationNoWaitAndPrintMetrics()
			return &pb.OfferSubmissionResponse{Accepted: true, Message: "Finalizado", Termino: true}, nil
		}

		return &pb.OfferSubmissionResponse{Accepted: true, Message: "OK", Termino: false}, nil
	}

	return &pb.OfferSubmissionResponse{
		Accepted: false,
		Message:  fmt.Sprintf("W no alcanzado: %d/%d", confirmed, W),
		Termino:  false,
	}, nil
}

// -------------------------------------------------------------------------
// Recovery (Consumer -> Broker): R=2 + filtro por preferencias
// -------------------------------------------------------------------------

func offersEqual(a, b []*pb.Offer) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]*pb.Offer, len(a))
	for _, of := range a {
		if of == nil || of.GetOfertaId() == "" {
			continue
		}
		m[of.GetOfertaId()] = of
	}
	for _, of := range b {
		if of == nil || of.GetOfertaId() == "" {
			return false
		}
		ref, ok := m[of.GetOfertaId()]
		if !ok {
			return false
		}
		if ref.GetProducto() != of.GetProducto() ||
			ref.GetPrecio() != of.GetPrecio() ||
			ref.GetTienda() != of.GetTienda() ||
			ref.GetCategoria() != of.GetCategoria() ||
			ref.GetDescuento() != of.GetDescuento() ||
			ref.GetStock() != of.GetStock() ||
			ref.GetFecha() != of.GetFecha() {
			return false
		}
	}
	return true
}

func (s *BrokerServer) fetchHistoryFromDB(node Entity, myID string, timeout time.Duration) ([]*pb.Offer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.Dial(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewDBNodeClient(conn)
	resp, err := client.GetOfferHistory(ctx, &pb.RecoveryRequest{RequestingNodeId: myID})
	if err != nil {
		return nil, err
	}
	return resp.GetOffers(), nil
}

type RecoveryServer struct {
	pb.UnimplementedRecoveryServer
	broker *BrokerServer
}

func (r *RecoveryServer) GetFilteredHistory(ctx context.Context, req *pb.HistoryRequest) (*pb.HistoryResponse, error) {
	consumerID := req.GetConsumerId()
	if consumerID == "" {
		return &pb.HistoryResponse{Offers: nil}, nil
	}

	r.broker.mu.Lock()
	nodes := make([]Entity, 0, len(r.broker.dbNodes))
	for _, n := range r.broker.dbNodes {
		nodes = append(nodes, n)
	}
	prefs, havePrefs := r.broker.consumerPrefs[consumerID]
	r.broker.mu.Unlock()

	if len(nodes) < 2 {
		return &pb.HistoryResponse{Offers: nil}, nil
	}

	type hs struct {
		arr []*pb.Offer
		err error
	}
	results := make([]hs, len(nodes))

	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, node Entity) {
			defer wg.Done()
			arr, err := r.broker.fetchHistoryFromDB(node, "BROKER", 3*time.Second)
			results[i] = hs{arr, err}
		}(i, n)
	}
	wg.Wait()

	var chosen []*pb.Offer
found:
	for i := range results {
		if results[i].err != nil || results[i].arr == nil {
			continue
		}
		for j := i + 1; j < len(results); j++ {
			if results[j].err != nil || results[j].arr == nil {
				continue
			}
			if offersEqual(results[i].arr, results[j].arr) {
				chosen = results[i].arr
				break found
			}
		}
	}

	if chosen == nil {
		return &pb.HistoryResponse{Offers: nil}, nil
	}

	// marcar resync exitoso para ese consumidor
	r.broker.mu.Lock()
	resyncOKPorConsumidor[consumerID]++
	r.broker.mu.Unlock()

	if havePrefs {
		filtered := make([]*pb.Offer, 0, len(chosen))
		for _, of := range chosen {
			if of != nil && r.broker.isRelevant(of, prefs) {
				filtered = append(filtered, of)
			}
		}
		return &pb.HistoryResponse{Offers: filtered}, nil
	}
	return &pb.HistoryResponse{Offers: chosen}, nil
}

// -------------------------------------------------------------------------
// Finalización: disparo sin esperar + métricas finales
// -------------------------------------------------------------------------

func (s *BrokerServer) informarFinAConsumer(consumer Entity, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.Dial(consumer.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf(Red+"[Fin]"+Reset+" No conecta con consumidor %s: %v\n", consumer.ID, err)
		return
	}
	defer conn.Close()

	c := pb.NewFinalizacionClient(conn)
	resp, err := c.InformarFinalizacion(ctx, &pb.EndingNotify{Fin: true})
	if err == nil && resp != nil && resp.GetConsumerconfirm() {
		s.mu.Lock()
		confirmCSVConsumidor[consumer.ID] = true
		s.mu.Unlock()
		fmt.Printf(Green+"[Fin]"+Reset+" Consumidor %s confirmó CSV final\n", consumer.ID)
	}
}

func (s *BrokerServer) informarFinADB(node Entity, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.Dial(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf(Red+"[Fin]"+Reset+" No conecta con DB %s: %v\n", node.ID, err)
		s.mu.Lock()
		nodos_caidos_al_finalizar++
		s.mu.Unlock()
		return
	}
	defer conn.Close()

	c := pb.NewFinalizacionClient(conn)
	resp, err := c.InformarFinalizacion(ctx, &pb.EndingNotify{Fin: true})
	if err != nil || resp == nil || !resp.GetBdconfirm() {
		fmt.Printf("[Fin] DB %s no confirmó finalización\n", node.ID)
		s.mu.Lock()
		nodos_caidos_al_finalizar++
		s.mu.Unlock()
		return
	}
	fmt.Printf("[Fin] DB %s confirmó finalización\n", node.ID)
}

func (s *BrokerServer) notifyFinalizationNoWaitAndPrintMetrics() {
	fmt.Println("[Fin] Notificando finalización...")

	s.mu.Lock()
	dbs := make([]Entity, 0, len(s.dbNodes))
	for _, n := range s.dbNodes {
		dbs = append(dbs, n)
	}
	cons := make([]Entity, 0, len(s.consumers))
	for _, c := range s.consumers {
		cons = append(cons, c)
	}
	s.mu.Unlock()

	for _, db := range dbs {
		go s.informarFinADB(db, 2*time.Second)
	}
	for _, co := range cons {
		go s.informarFinAConsumer(co, 3*time.Second)
	}

	// pequeña ventana para respuestas inmediatas sin bloquear
	time.Sleep(800 * time.Millisecond)

	fmt.Println("\n================= MÉTRICAS FINALES (BROKER) =================")
	fmt.Printf("Ofertas aceptadas por el broker: %d\n", aceptadas_totales)
	fmt.Printf("Escrituras totales (Todas las BD): %d\n", escrituras_totales)
	fmt.Printf("Escrituras exitosas (w=2): %d\n", escrituras_exitosas)

	fmt.Printf("Nodos de BD caídos al finalizar: %d\n", nodos_caidos_al_finalizar)

	fmt.Println("Resincronizaciones tras caídas de BDs:")
	for id, n := range caidasPorNodo {
		fmt.Printf("  %s: %d\n", id, n)
	}

	fmt.Println("Caídas por Consumidor:")
	for id, n := range caidasPorConsumidor {
		fmt.Printf("  %s: %d\n", id, n)
	}

	fmt.Println("Resincronizaciones exitosas por Consumidor:")
	for id, n := range resyncOKPorConsumidor {
		fmt.Printf("  %s: %d\n", id, n)
	}

	fmt.Println("Confirmaciones de generación de CSV:")
	for id := range s.consumers {
		ok := confirmCSVConsumidor[id]
		fmt.Printf("  %s: %t\n", id, ok)
	}

	fmt.Println("Cantidad de ofertas recibidas por Consumidores:")
	for id, n := range ofertasPorConsumidor {
		fmt.Printf("  %s: %d\n", id, n)
	}
	fmt.Printf("==============================================================\n")
}

// -------------------------------------------------------------------------
// main
// -------------------------------------------------------------------------

func main() {
	rand.Seed(time.Now().UnixNano())

	lis, err := net.Listen("tcp", brokerPort)
	if err != nil {
		fmt.Printf("Listen error: %v\n", err)
		return
	}

	s := grpc.NewServer()
	bs := NewBrokerServer()

	if err := bs.loadConsumerPreferences(); err != nil {
		fmt.Printf("Preferencias error: %v\n", err)
		return
	}

	pb.RegisterEntityManagementServer(s, bs)
	pb.RegisterConfirmarInicioServer(s, bs)
	pb.RegisterOfferSubmissionServer(s, bs)

	recoveryServer := &RecoveryServer{broker: bs}
	pb.RegisterRecoveryServer(s, recoveryServer)

	fmt.Printf("Broker escuchando en %s...\n", brokerPort)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("Serve error: %v\n", err)
	}
}
