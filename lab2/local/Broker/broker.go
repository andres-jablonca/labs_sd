// broker.go
package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	TOPE_OFERTAS            = 25
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

	escrituras_totales  int64
	escrituras_exitosas int64
	escrituras_fallidas int64
	lecturas_exitosas   int64
	lecturas_fallidas   int64

	nodos_caidos_al_finalizar int64
	caidos_arreglo            []string

	// Mutex para cada mapa global
	caidasMu            sync.Mutex
	caidasPorNodo       = map[string]int{} // fallos al escribir en nodo
	caidasPorConsumidor = map[string]int{} // fallos al notificar a consumidor

	ofertasMu            sync.Mutex
	ofertasPorConsumidor = map[string]int{} // notificaciones exitosas

	resyncMu              sync.Mutex
	resyncOKPorConsumidor = map[string]int{} // Recovery exitoso (R=2) por consumidor

	confirmMu            sync.Mutex
	confirmCSVConsumidor = map[string]bool{} // si el consumer confirmó CSV final
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
	grpcServer    *grpc.Server
}

const (
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Reset  = "\033[0m"
)

func NewBrokerServer() *BrokerServer {
	return &BrokerServer{
		entities:      make(map[string]Entity),
		dbNodes:       make(map[string]Entity),
		consumers:     make(map[string]Entity),
		consumerPrefs: make(map[string]ConsumerPreference),
	}
}

func (s *BrokerServer) ShutdownSoon() {
	go func() {
		// Espera corta para asegurar que el EndingConfirm ya se envió
		time.Sleep(600 * time.Millisecond)
		if s.grpcServer != nil {
			s.grpcServer.GracefulStop() // cierra listeners y deja terminar RPCs en curso
		}
	}()
}

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
		return fmt.Errorf("error con cabecera %s: %w", consumerPreferencesFile, err)
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
			for _, cat := range strings.Split(record[1], ";") {
				categories[strings.TrimSpace(cat)] = true
			}
		}
		stores := make(map[string]bool)
		if strings.ToLower(record[2]) != "null" && record[2] != "" {
			for _, st := range strings.Split(record[2], ";") {
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

	fmt.Printf("Preferencias de %d consumidores cargadas.\n", recordsRead)
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

	fmt.Printf("[Registro] %s registrad@ correctamente (%s) en %s. Total de registrados: %d\n",
		id, ent.Type, ent.Address, len(s.entities))

	// Usar atomic para contador global
	atomic.AddInt64(&contador_registrados, 1)
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
	// Copiar datos bajo lock
	s.mu.Lock()
	cons := make(map[string]Entity, len(s.consumers))
	for k, v := range s.consumers {
		cons[k] = v
	}
	prefsCopy := make(map[string]ConsumerPreference, len(s.consumerPrefs))
	for k, v := range s.consumerPrefs {
		prefsCopy[k] = v
	}
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
				return
			}
			defer conn.Close()

			cli := pb.NewConsumerClient(conn)
			resp, err := cli.ReceiveOffer(ctx, offer)
			if err != nil || resp == nil || !resp.GetSuccess() {
				return
			}

			// Proteger acceso al mapa global
			ofertasMu.Lock()
			ofertasPorConsumidor[consumer.ID]++
			ofertasMu.Unlock()

			mu.Lock()
			countOK++
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	fmt.Printf(Yellow+"[Oferta %s]"+Reset+" Oferta enviada correctamente a %d consumidores\n\n\n", offer.GetOfertaId(), countOK)
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

	// Verificar límites por tienda antes de procesar
	terminacionMu.Lock()
	switch offer.GetTienda() {
	case "Parisio":
		if ofertas_parisio >= TOPE_OFERTAS {
			terminacionMu.Unlock()
			return &pb.OfferSubmissionResponse{
				Accepted: false,
				Message:  "Límite de ofertas alcanzado para Parisio",
				Termino:  false,
			}, nil
		}
	case "Falabellox":
		if ofertas_falabellox >= TOPE_OFERTAS {
			terminacionMu.Unlock()
			return &pb.OfferSubmissionResponse{
				Accepted: false,
				Message:  "Límite de ofertas alcanzado para Falabellox",
				Termino:  false,
			}, nil
		}
	case "Riploy":
		if ofertas_riploy >= TOPE_OFERTAS {
			terminacionMu.Unlock()
			return &pb.OfferSubmissionResponse{
				Accepted: false,
				Message:  "Límite de ofertas alcanzado para Riploy",
				Termino:  false,
			}, nil
		}
	}
	terminacionMu.Unlock()

	fmt.Printf(Yellow+"[Oferta %s]"+Reset+" Iniciando escritura distribuida (N=%d, W=%d)\n", offer.GetOfertaId(), N, W)

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

	for _, node := range nodes {
		wg.Add(1)
		go func(n Entity) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			conn, err := grpc.Dial(n.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			defer conn.Close()

			cli := pb.NewDBNodeClient(conn)
			resp, err := cli.StoreOffer(ctx, offer)
			if err != nil || resp == nil || !resp.GetSuccess() {
				return
			}
			mu.Lock()
			confirmed++
			escrituras_totales++
			mu.Unlock()
		}(node)
	}

	wg.Wait()
	if confirmed >= W {
		escrituras_exitosas++
		fmt.Printf(Yellow+"[Oferta %s]"+Reset+" Escritura exitosa, se cumple W=2\n", offer.GetOfertaId())
		go s.notifyConsumers(offer)

		terminacionMu.Lock()
		defer terminacionMu.Unlock()

		if !sistemaTerminado {
			switch offer.GetTienda() {
			case "Parisio":
				ofertas_parisio++
			case "Falabellox":
				ofertas_falabellox++
			case "Riploy":
				ofertas_riploy++
			}
		}

		// Verificar si todas las tiendas alcanzaron el límite
		if ofertas_falabellox >= TOPE_OFERTAS && ofertas_parisio >= TOPE_OFERTAS && ofertas_riploy >= TOPE_OFERTAS {
			time.Sleep(2 * time.Second)
			fmt.Println("\n=======================================================")
			fmt.Println("Límite de ofertas alcanzado! Finalizando CyberDay...")
			fmt.Println("=======================================================")
			sistemaTerminado = true
			s.notifyFinalizationNoWaitAndPrintMetrics()
			return &pb.OfferSubmissionResponse{Accepted: true, Message: "Finalizado", Termino: true}, nil
		}

		return &pb.OfferSubmissionResponse{Accepted: true, Message: "OK", Termino: false}, nil
	} else {
		escrituras_fallidas++
		fmt.Printf(Yellow+"[Oferta %s]"+Reset+" Escritura no exitosa, no se cumple W=2\n", offer.GetOfertaId())
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

	fmt.Printf("Solicitud de histórico por parte de %s\n", consumerID)

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
	matchesFound := false // Variable para verificar si se encuentran coincidencias
found:
	for i := 0; i < len(results); i++ {
		if results[i].err != nil || results[i].arr == nil {
			continue
		}
		for j := i + 1; j < len(results); j++ {
			if results[j].err != nil || results[j].arr == nil {
				continue
			}
			if offersEqual(results[i].arr, results[j].arr) {
				chosen = results[i].arr
				matchesFound = true
				break found
			}
		}
	}

	resyncMu.Lock()
	resyncOKPorConsumidor[consumerID]++
	resyncMu.Unlock()

	// Si se encontró una coincidencia, se imprime la lectura exitosa
	if matchesFound {
		lecturas_exitosas++
		fmt.Println("Lectura exitosa, se cumple R=2")
	} else {
		// Si no hay coincidencias, se imprime la lectura no exitosa
		lecturas_fallidas++
		fmt.Println("Lectura no exitosa, no se cumple R=2")
		// Devolvemos un historial vacío en caso de no encontrar coincidencias
		return &pb.HistoryResponse{Offers: nil}, nil
	}

	if chosen == nil {
		return &pb.HistoryResponse{Offers: nil}, nil
	}

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
		fmt.Printf(Blue+"[Fin]"+Reset+" No conecta con consumidor %s: %v\n", consumer.ID, err)
		confirmMu.Lock()
		confirmCSVConsumidor[consumer.ID] = false
		confirmMu.Unlock()
		return
	}
	defer conn.Close()

	c := pb.NewFinalizacionClient(conn)
	resp, err := c.InformarFinalizacion(ctx, &pb.EndingNotify{Fin: true})
	if err != nil {
		// El consumidor está caído (timeout, connection refused, etc.)
		fmt.Printf(Red+"[Fin]"+Reset+" Consumidor %s CAÍDO - No se pudo contactar: %v\n", consumer.ID, err)
		confirmMu.Lock()
		confirmCSVConsumidor[consumer.ID] = false
		confirmMu.Unlock()
		return
	}

	if resp != nil && resp.GetConsumerconfirm() {
		confirmMu.Lock()
		confirmCSVConsumidor[consumer.ID] = true
		confirmMu.Unlock()
		fmt.Printf(Green+"[Fin]"+Reset+" Consumidor %s confirmó CSV final\n", consumer.ID)
	} else {
		confirmMu.Lock()
		confirmCSVConsumidor[consumer.ID] = false
		confirmMu.Unlock()
		fmt.Printf(Red+"[Fin]"+Reset+" Consumidor %s NO confirmó CSV final\n", consumer.ID)
	}
}

func (s *BrokerServer) informarFinADB(node Entity, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.Dial(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf(Blue+"[Fin]"+Reset+" DB %s CAÍDA - No se pudo conectar: %v\n", node.ID, err)
		s.mu.Lock()
		nodos_caidos_al_finalizar++
		caidos_arreglo = append(caidos_arreglo, node.ID)
		s.mu.Unlock()
		return
	}
	defer conn.Close()

	c := pb.NewFinalizacionClient(conn)
	resp, err := c.InformarFinalizacion(ctx, &pb.EndingNotify{Fin: true})

	// Si hay error de conexión o timeout, la BD está caída
	if err != nil {
		fmt.Printf(Red+"[Fin]"+Reset+" DB %s CAÍDA - Error de comunicación: %v\n", node.ID, err)
		s.mu.Lock()
		nodos_caidos_al_finalizar++
		caidos_arreglo = append(caidos_arreglo, node.ID)
		s.mu.Unlock()
		return
	}

	// Si la BD responde pero no confirma (estaba caída durante finalización)
	if resp == nil || !resp.GetBdconfirm() {
		fmt.Printf(Red+"[Fin]"+Reset+" DB %s CAÍDA DURANTE FINALIZACIÓN - No confirmó\n", node.ID)
		s.mu.Lock()
		nodos_caidos_al_finalizar++
		caidos_arreglo = append(caidos_arreglo, node.ID)
		s.mu.Unlock()
		return
	}

	fmt.Printf(Green+"[Fin]"+Reset+" DB %s confirmó finalización correctamente\n", node.ID)
}

func (s *BrokerServer) notifyFinalizationNoWaitAndPrintMetrics() {
	fmt.Println(Blue + "[Fin]" + Reset + " Notificando finalización...")

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
		go s.informarFinAConsumer(co, 2*time.Second)
	}

	go func() {
		time.Sleep(5 * time.Second) // Esperar suficiente tiempo
		s.generarReporteTXT()
		s.ShutdownSoon()
	}()
}

type CaidaServer struct {
	pb.UnimplementedCaidaServer
	broker *BrokerServer
}

func (c *CaidaServer) InformarCaida(ctx context.Context, req *pb.FailNotify) (*pb.FailACK, error) {
	fmt.Printf(Red+"[Caída] %s (%s) reportó una caída\n"+Reset, req.GetId(), req.GetType())

	caidasMu.Lock()
	if req.GetType() == "DBNode" {
		caidasPorNodo[req.GetId()]++
	} else if req.GetType() == "Consumer" {
		caidasPorConsumidor[req.GetId()]++
	}
	caidasMu.Unlock()

	return &pb.FailACK{Ack: true}, nil
}

func (s *BrokerServer) generarReporteTXT() {
	// Ruta dentro del contenedor
	reportPath := "/app/Broker/Reporte.txt"

	file, err := os.Create(reportPath)
	if err != nil {
		fmt.Printf("Error creando archivo de reporte: %v\n", err)
		return
	}
	defer file.Close()

	// Escribir el reporte
	fmt.Fprintln(file, "================= REPORTE FINAL =================")

	fmt.Fprintf(file, "\nOfertas por tienda:\n")
	fmt.Fprintf(file, "  Parisio: %d\n", ofertas_parisio)
	fmt.Fprintf(file, "  Falabellox: %d\n", ofertas_falabellox)
	fmt.Fprintf(file, "  Riploy: %d\n\n", ofertas_riploy)

	fmt.Fprintf(file, "Escrituras totales (Suma de escrituras en cada BD): %d\n", escrituras_totales)
	fmt.Fprintf(file, "Escrituras exitosas (W=2): %d\n", escrituras_exitosas)
	fmt.Fprintf(file, "Escrituras fallidas (W<2): %d\n\n", escrituras_fallidas)

	fmt.Fprintf(file, "Lecturas exitosas (R=2): %d\n", lecturas_exitosas)
	fmt.Fprintf(file, "Lecturas fallidas (R<2): %d\n\n", lecturas_fallidas)

	fmt.Fprintf(file, "Nodos BD caídos al finalizar: %d\n", nodos_caidos_al_finalizar)
	for _, id := range caidos_arreglo {
		fmt.Fprintf(file, "  %s\n", id)
	}

	fmt.Fprintln(file, "\nCaídas por nodo DB:")
	for id, n := range caidasPorNodo {
		fmt.Fprintf(file, "  %s: %d\n", id, n)
	}

	fmt.Fprintln(file, "\nCaídas por Consumidor:")
	for id, n := range caidasPorConsumidor {
		fmt.Fprintf(file, "  %s: %d\n", id, n)
	}

	fmt.Fprintln(file, "\nResincronizaciones exitosas por Consumidor:")
	for id, n := range resyncOKPorConsumidor {
		fmt.Fprintf(file, "  %s: %d\n", id, n)
	}

	fmt.Fprintln(file, "\nResumen por Consumidor:")
	// Obtener todos los IDs de consumidores y ordenarlos para consistencia
	s.mu.Lock()
	consumerIDs := make([]string, 0, len(s.consumers))
	for id := range s.consumers {
		consumerIDs = append(consumerIDs, id)
	}
	s.mu.Unlock()

	// Ordenar los IDs para mejor presentación
	//sort.Strings(consumerIDs) // Opcional: descomenta si quieres orden alfabético

	for _, id := range consumerIDs {
		ofertasRecibidas := ofertasPorConsumidor[id]
		csvGenerado := confirmCSVConsumidor[id]
		estadoCSV := "generado"
		if !csvGenerado {
			estadoCSV = "no generado"
		}
		fmt.Fprintf(file, "  %s: %d ofertas recibidas (recibidas directamente, no por resincronización). Archivo CSV %s\n", id, ofertasRecibidas, estadoCSV)
	}

	conclusion := ""

	// Caso 1: Sistema óptimo - sin fallos y consistencia perfecta
	if nodos_caidos_al_finalizar == 0 && escrituras_fallidas == 0 && lecturas_fallidas == 0 {
		conclusion = "✅ SISTEMA ÓPTIMO: El sistema demostró alta disponibilidad y consistencia perfecta. " +
			"No hubo caídas de nodos DB, todas las escrituras cumplieron W=2 y todas las lecturas R=2. " +
			"La tolerancia a fallos funcionó correctamente y los consumidores recibieron todas sus ofertas relevantes."
	} else if nodos_caidos_al_finalizar > 0 && escrituras_fallidas == 0 && lecturas_fallidas == 0 { // Caso 2: Sistema robusto - fallos manejados correctamente
		conclusion = "🔄 SISTEMA ROBUSTO: El sistema mantuvo la consistencia a pesar de fallos. " +
			fmt.Sprintf("Aunque %d nodo(s) DB cayeron temporalmente, ", nodos_caidos_al_finalizar) +
			"las escrituras y lecturas se completaron exitosamente gracias a la replicación. " +
			"La tolerancia a fallos funcionó según lo especificado."
	} else if escrituras_fallidas > 0 || lecturas_fallidas > 0 { // Caso 3: Sistema con degradación controlada
		fallos := ""
		if escrituras_fallidas > 0 {
			fallos += fmt.Sprintf("%d escrituras fallidas (W<2)", escrituras_fallidas)
		}
		if lecturas_fallidas > 0 {
			if fallos != "" {
				fallos += " y "
			}
			fallos += fmt.Sprintf("%d lecturas fallidas (R<2)", lecturas_fallidas)
		}

		conclusion = "⚠️  SISTEMA CON DEGRADACIÓN: El sistema mantuvo disponibilidad pero con pérdida parcial de consistencia. " +
			fmt.Sprintf("Se presentaron %s debido a fallos simultáneos en múltiples nodos. ", fallos) +
			"Sin embargo, el núcleo del sistema permaneció operativo y la mayoría de operaciones se completaron exitosamente."
	} else if nodos_caidos_al_finalizar >= 2 && (escrituras_fallidas > 0 || lecturas_fallidas > 0) { // Caso 4: Sistema crítico - múltiples fallos
		conclusion = "❌ SISTEMA CRÍTICO: El sistema experimentó fallos significativos que afectaron la consistencia. " +
			fmt.Sprintf("Con %d nodos DB caídos y operaciones fallidas, ", nodos_caidos_al_finalizar) +
			"la disponibilidad se mantuvo pero la consistencia bajo reglas DynamoDB no pudo garantizarse completamente."
	} else if len(resyncOKPorConsumidor) > 0 && escrituras_fallidas == 0 { // Caso 5: Sistema recuperado exitosamente
		totalResync := 0
		for _, n := range resyncOKPorConsumidor {
			totalResync += n
		}
		conclusion = "🔄 SISTEMA RECUPERADO: El sistema superó fallos temporales mediante resincronización exitosa. " +
			fmt.Sprintf("Se recuperaron %d consumidores y se mantuvo la consistencia en escrituras. ", totalResync) +
			"La replicación eventual funcionó correctamente para restaurar el estado del sistema."
	} else { // Caso default para cualquier escenario no cubierto
		conclusion = "📊 SISTEMA FUNCIONAL: El sistema operó dentro de parámetros aceptables. " +
			"Se presentaron algunos fallos pero el núcleo distributivo mantuvo operatividad. " +
			"La consistencia se preservó en la mayoría de operaciones críticas."
	}

	fmt.Fprintf(file, "\nConlusión final:\n%s\n", conclusion)

	fmt.Fprintln(file, "==============================================================")

	fmt.Printf(Blue + "[Reporte]" + Reset + " Reporte generado exitosamente\n")

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
	bs.grpcServer = s

	if err := bs.loadConsumerPreferences(); err != nil {
		fmt.Printf("Preferencias error: %v\n", err)
		return
	}

	// Registrar todos los servicios
	pb.RegisterEntityManagementServer(s, bs)
	pb.RegisterConfirmarInicioServer(s, bs)
	pb.RegisterOfferSubmissionServer(s, bs)

	recoveryServer := &RecoveryServer{broker: bs}
	pb.RegisterRecoveryServer(s, recoveryServer)

	// Registrar el servicio Caida
	caidaServer := &CaidaServer{broker: bs}
	pb.RegisterCaidaServer(s, caidaServer)

	fmt.Printf("Broker escuchando en %s...\n", brokerPort)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("Serve error: %v\n", err)
	}
}
