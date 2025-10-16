package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
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

// Constantes del sistema (N=3, W=2, R=2)
const (
	brokerPort = ":50095"
	N          = 3 // Número total de réplicas
	W          = 2 // Número de confirmaciones de escritura requeridas

	consumerPreferencesFile = "Broker/consumidores.csv"
)

var contador_registrados int64 = 0
var ofertas_parisio int64 = 0
var ofertas_riploy int64 = 0
var ofertas_falabellox int64 = 0
var terminacionMu sync.Mutex
var sistemaTerminado bool = false // 💡 NUEVA BANDERA DE ESTADO

// -------------------------------------------------------------------------
// ESTRUCTURAS DE DATOS
// -------------------------------------------------------------------------

// Estructura para registrar cualquier entidad (Productor, DB Node, Consumidor)
type Entity struct {
	ID      string
	Type    string
	Address string
}

// Estructura para almacenar las preferencias de filtrado de un consumidor
type ConsumerPreference struct {
	ID         string
	Categories map[string]bool // Mapa para búsqueda rápida (separa por ;)
	Stores     map[string]bool // Mapa para búsqueda rápida (separa por ;)
	MaxPrice   int64           // Usa int64 para coincidir con el tipo del offer.precio
}

// BrokerServer implementa los servicios gRPC requeridos
type BrokerServer struct {
	pb.UnimplementedEntityManagementServer
	pb.UnimplementedOfferSubmissionServer
	pb.UnimplementedConfirmarInicioServer

	entities      map[string]Entity
	dbNodes       map[string]Entity             // Subconjunto de Nodos DB
	consumers     map[string]Entity             // FASE 4: Mapa para almacenar los consumidores registrados
	consumerPrefs map[string]ConsumerPreference // Almacena las preferencias cargadas del CSV
	mu            sync.Mutex
}

// NewBrokerServer inicializa la estructura del Broker
func NewBrokerServer() *BrokerServer {
	return &BrokerServer{
		entities:      make(map[string]Entity),
		dbNodes:       make(map[string]Entity),
		consumers:     make(map[string]Entity),
		consumerPrefs: make(map[string]ConsumerPreference), // Inicializar el mapa de preferencias
	}
}

// -------------------------------------------------------------------------
// LÓGICA DE CARGA Y FILTRADO
// -------------------------------------------------------------------------

// loadConsumerPreferences lee el archivo CSV y carga las preferencias
func (s *BrokerServer) loadConsumerPreferences() error {
	file, err := os.Open(consumerPreferencesFile)
	if err != nil {
		return fmt.Errorf("error al abrir %s: %w", consumerPreferencesFile, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Saltar la cabecera
	if _, err := reader.Read(); err != nil {
		if err == io.EOF {
			return fmt.Errorf("el archivo %s está vacío", consumerPreferencesFile)
		}
		return fmt.Errorf("error al leer la cabecera de %s: %w", consumerPreferencesFile, err)
	}

	prefsMap := make(map[string]ConsumerPreference)
	recordsRead := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error al leer registro del CSV: %w", err)
		}

		if len(record) < 4 {
			fmt.Printf("⚠️ Advertencia: Registro incompleto para el consumidor %s, omitiendo.\n", record[0])
			continue
		}

		// 1. Parsear ID
		id := record[0]

		// 2. Parsear Categoría
		categories := make(map[string]bool)
		if strings.ToLower(record[1]) != "null" && record[1] != "" {
			for _, cat := range strings.Split(record[1], ";") {
				categories[strings.TrimSpace(cat)] = true
			}
		}

		// 3. Parsear Tienda
		stores := make(map[string]bool)
		if strings.ToLower(record[2]) != "null" && record[2] != "" {
			for _, store := range strings.Split(record[2], ";") {
				stores[strings.TrimSpace(store)] = true
			}
		}

		// 4. Parsear Precio Máximo
		var maxPrice int64 = -1 // Usamos -1 como indicador de "null" (sin límite)
		if strings.ToLower(record[3]) != "null" && record[3] != "" {
			parsedPrice, err := strconv.ParseInt(record[3], 10, 64)
			if err == nil {
				maxPrice = parsedPrice
			} else {
				fmt.Printf("⚠️ Advertencia: Precio máximo inválido para %s ('%s'), usando sin límite.\n", id, record[3])
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

	fmt.Printf("[Broker] ✅ Preferencias de %d consumidores cargadas correctamente.\n", recordsRead)
	return nil
}

// isRelevant implementa la lógica de filtrado de la oferta para un consumidor específico
func (s *BrokerServer) isRelevant(offer *pb.Offer, prefs ConsumerPreference) bool {
	// 1. Filtrar por Tienda
	if len(prefs.Stores) > 0 {
		if _, exists := prefs.Stores[offer.GetTienda()]; !exists {
			// El consumidor especificó tiendas, y la oferta no viene de una de ellas.
			return false
		}
	}

	// 2. Filtrar por Categoría
	if len(prefs.Categories) > 0 {
		if _, exists := prefs.Categories[offer.GetCategoria()]; !exists {
			// El consumidor especificó categorías, y la oferta no es de una de ellas.
			return false
		}
	}

	// 3. Filtrar por Precio Máximo
	if prefs.MaxPrice != -1 { // Si MaxPrice no es el indicador de "null" (-1)
		// offer.GetPrecio() es int64, prefs.MaxPrice es int64. Tipos coinciden.
		if offer.GetPrecio() > prefs.MaxPrice {
			return false
		}
	}

	// La oferta cumple con todos los criterios
	return true
}

// -------------------------------------------------------------------------
// FASE 1: Registro de Entidades (EntityManagementServer)
// -------------------------------------------------------------------------

func (s *BrokerServer) RegisterEntity(ctx context.Context, req *pb.RegistrationRequest) (*pb.RegistrationResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entityID := req.GetEntityId()

	if _, ok := s.entities[entityID]; ok {
		return &pb.RegistrationResponse{
			Success: false,
			Message: fmt.Sprintf("Entidad %s ya se encuentra registrada.\n", entityID),
		}, nil
	}

	entity := Entity{
		ID:      entityID,
		Type:    req.GetEntityType(),
		Address: req.GetAddress(),
	}
	s.entities[entityID] = entity

	if req.GetEntityType() == "DBNode" {
		s.dbNodes[entityID] = entity
	} else if req.GetEntityType() == "Consumer" {
		// FASE 4: Solo registramos consumidores si tienen preferencias cargadas.
		if _, ok := s.consumerPrefs[entityID]; !ok {
			fmt.Printf("[Registro] 🛑 Consumidor %s RECHAZADO. No encontrado en consumidores.csv.\n", entityID)
			return &pb.RegistrationResponse{
				Success: false,
				Message: "Registro fallido. Su ID de consumidor no está en la lista de preferencias.\n",
			}, nil
		}
		s.consumers[entityID] = entity
	}

	fmt.Printf("[Registro] ✅ %s registrad@ correctamente (%s) en %s. Total de registrados: %d\n", entityID, entity.Type, entity.Address, len(s.entities))
	contador_registrados++
	return &pb.RegistrationResponse{
		Success: true,
		Message: "Registro exitoso. Bienvenido al CyberDay!.\n",
	}, nil
}

// -------------------------------------------------------------------------
// FASE 4: Notificación a Consumidores con Filtrado
// -------------------------------------------------------------------------

// notifyConsumers maneja la iteración, el filtrado y la llamada gRPC a los consumidores.
func (s *BrokerServer) notifyConsumers(offer *pb.Offer) {
	s.mu.Lock()
	// Copia el mapa de consumidores para evitar bloquear el registro
	consumersToNotify := make(map[string]Entity, len(s.consumers))
	for k, v := range s.consumers {
		consumersToNotify[k] = v
	}
	s.mu.Unlock()

	var wg sync.WaitGroup
	notificationsSent := 0
	var countMu sync.Mutex

	fmt.Printf("[Notificación] Iniciando notificación para %d consumidores registrados (Oferta %s)...\n", len(consumersToNotify), offer.GetOfertaId())

	for _, consumer := range consumersToNotify {
		// 1. Lógica de Filtrado de Suscripción
		prefs, ok := s.consumerPrefs[consumer.ID]
		if !ok {
			fmt.Printf("⚠️ Consumidor %s registrado pero sin preferencias cargadas. Omitiendo.\n", consumer.ID)
			continue
		}

		if !s.isRelevant(offer, prefs) {
			fmt.Printf("⏭️ Consumidor %s no está interesado en la oferta %s (no cumple filtro).\n", consumer.ID, offer.GetOfertaId())
			continue
		}

		// Si es relevante, se notifica
		wg.Add(1)
		go func(consumer Entity) {
			defer wg.Done()

			// Contexto con timeout corto para la notificación (tolerancia a la caída)
			notifyCtx, notifyCancel := context.WithTimeout(context.Background(), time.Second*1)
			defer notifyCancel()

			// 1. Conexión al Consumidor
			conn, err := grpc.Dial(consumer.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				// Este error maneja la desconexión abrupta (Fase 5 - consumidor)
				fmt.Printf("[Notificación] ❌ Consumidor %s falló en recibir oferta (conexión/caída): %v\n", consumer.ID, err)
				return
			}
			defer conn.Close()

			// 2. Cliente del servicio Consumer (basado en cyberday.proto)
			consumerClient := pb.NewConsumerClient(conn)

			// 3. Llamada al método de lectura (ReceiveOffer)
			resp, err := consumerClient.ReceiveOffer(notifyCtx, offer)

			if err != nil || !resp.GetSuccess() {
				msg := "Error de RPC"
				if resp != nil {
					msg = resp.GetMessage()
				}
				fmt.Printf("[Notificación] ❌ Oferta %s falló en ser confirmada por %s. Error: %s\n", offer.GetOfertaId(), consumer.ID, msg)
				return
			}

			// Notificación exitosa
			fmt.Printf("[Notificación] ✅ Oferta %s enviada y confirmada por %s.\n", offer.GetOfertaId(), consumer.ID)
			countMu.Lock()
			notificationsSent++
			countMu.Unlock()
		}(consumer)
	}

	wg.Wait()
	fmt.Printf("[Notificación] Distribución completada para oferta %s. Notificaciones exitosas: %d\n", offer.GetOfertaId(), notificationsSent)
}

func (s *BrokerServer) Confirmacion(ctx context.Context, offer *pb.ConfirmRequest) (*pb.ConfirmResponse, error) {
	if contador_registrados < 18 {
		return &pb.ConfirmResponse{
			Ready: false,
		}, nil
	}
	return &pb.ConfirmResponse{
		Ready: true,
	}, nil
}

// -------------------------------------------------------------------------
// FASE 5: Simulación de Fallos
// -------------------------------------------------------------------------

// simulateDBFailure envía una señal RPC a un nodo DB para que simule un fallo temporal.
func (s *BrokerServer) simulateDBFailure(nodeID string, duration time.Duration) {
	s.mu.Lock()
	node, ok := s.dbNodes[nodeID]
	s.mu.Unlock()

	if !ok {
		fmt.Printf("[CONTROL] 🛑 No se encontró el Nodo DB %s para simular el fallo.\n", nodeID)
		return
	}

	// Conexión al Nodo DB
	conn, err := grpc.Dial(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[CONTROL] ❌ Error conectando a %s para enviar señal de fallo: %v\n", node.ID, err)
		return
	}
	defer conn.Close()

	// 💡 Cliente del nuevo servicio DBControl
	dbClient := pb.NewDBControlClient(conn)

	// Contexto con timeout
	failCtx, failCancel := context.WithTimeout(context.Background(), time.Second*5)
	defer failCancel()

	req := &pb.FailureRequest{
		DurationSeconds: int32(duration.Seconds()),
	}

	// Llamada a la función de fallo del Nodo DB
	resp, err := dbClient.SimulateFailure(failCtx, req)

	if err != nil || !resp.GetSuccess() {
		fmt.Printf("[CONTROL] ❌ Falló RPC a %s para simular fallo. Error: %v\n", node.ID, err)
		return
	}

	fmt.Printf("[CONTROL] ✅ Señal de FALLO enviada exitosamente a %s: %s\n", node.ID, resp.GetMessage())
}

// -------------------------------------------------------------------------
// FASE 2 & 3: Recepción y Escritura Distribuida (OfferSubmissionServer)
// -------------------------------------------------------------------------

func (s *BrokerServer) SendOffer(ctx context.Context, offer *pb.Offer) (*pb.OfferSubmissionResponse, error) {
	terminacionMu.Lock() // Bloquea acceso a variables de terminación
	if sistemaTerminado {
		terminacionMu.Unlock()
		return &pb.OfferSubmissionResponse{
			Accepted: false, // No se acepta porque el Cyberday finalizó
			Message:  "El Cyberday ha finalizado. No se aceptan más ofertas.",
			Termino:  true,
		}, nil
	}
	terminacionMu.Unlock() // Libera el mutex si no hemos terminado aún

	fmt.Printf("[Oferta %s recibida por parte de %s. Iniciando escritura distribuida (N=%d, W=%d)...\n", offer.GetOfertaId(), offer.GetTienda(), N, W)

	// VALIDACIÓN: Verificar que el número de nodos activos cumpla N
	if len(s.dbNodes) < N {
		fmt.Printf("[Oferta] 🛑 Oferta %s RECHAZADA. Sólo %d/%d nodos de DB se encuentran activos. No se puede garantizar N=%d.\n", offer.GetOfertaId(), len(s.dbNodes), N, N)
		return &pb.OfferSubmissionResponse{
			Accepted: false,
			Message:  fmt.Sprintf("No se puede garantizar N=%d replicas. Sólo hay %d nodos de DB activos.\n", N, len(s.dbNodes)),
		}, nil
	}

	var wg sync.WaitGroup
	confirmedWrites := 0
	var countMu sync.Mutex

	// 2. Escritura Concurrente en Nodos DB (N=3)
	for _, node := range s.dbNodes {
		wg.Add(1)
		go func(node Entity) {
			defer wg.Done()

			// Conexión al Nodo DB
			// Usamos el contexto pasado del productor, con un timeout max de 3 segundos
			dbCtx, dbCancel := context.WithTimeout(context.Background(), time.Second*3)
			defer dbCancel()

			conn, err := grpc.Dial(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				// Este error puede capturar el fallo del nodo DB si se cae totalmente (Fase 5)
				fmt.Printf("[Escritura] ❌ Error conectando con %s: %v\n", node.ID, err)
				return
			}
			defer conn.Close()

			dbClient := pb.NewDBNodeClient(conn)

			// Llamada a la función de escritura del Nodo DB
			resp, err := dbClient.StoreOffer(dbCtx, offer)

			if err != nil || !resp.GetSuccess() {
				// Imprime el error específico (incluyendo timeout si el nodo está en modo de fallo)
				fmt.Printf("[Escritura] ❌ %s falló en almacenar la oferta %s. Error: %v\n", node.ID, offer.GetOfertaId(), err)
				return
			}

			// Escritura exitosa
			fmt.Printf("[Escritura] ✅ %s confirma el almacenamiento de la oferta %s.\n", node.ID, offer.GetOfertaId())

			countMu.Lock()
			confirmedWrites++
			countMu.Unlock()
		}(node)
	}

	// Esperar a que terminen todas las llamadas
	wg.Wait()
	if confirmedWrites >= W {
		fmt.Printf("[Oferta] ✅ Oferta %s ACEPTADA. W=%d confirmación de escritura exitosa. (Fase 4: Notificación a Consumidores)\n", offer.GetOfertaId(), confirmedWrites)

		// FASE 4: Llamada asíncrona a la función de notificación.
		go s.notifyConsumers(offer)

		// --- Bloque de Conteo y Verificación ---
		terminacionMu.Lock()
		defer terminacionMu.Unlock()

		if offer.GetTienda() == "Parisio" {
			ofertas_parisio++
		} else if offer.GetTienda() == "Falabellox" {
			ofertas_falabellox++
		} else if offer.GetTienda() == "Riploy" {
			ofertas_riploy++
		}

		if ofertas_falabellox >= 20 && ofertas_parisio >= 20 && ofertas_riploy >= 20 {
			fmt.Println("\n=======================================================")
			fmt.Println("🛑 CONDICIÓN DE PARADA ALCANZADA: ¡3 ofertas por cada tienda!")
			fmt.Println("=======================================================")

			// ESTABLECER EL ESTADO GLOBAL DE TERMINACIÓN
			sistemaTerminado = true

			return &pb.OfferSubmissionResponse{
				Accepted: true,
				Message:  "Oferta aceptada y distribuida con éxito. Finalizado.",
				Termino:  true,
			}, nil
		}

		return &pb.OfferSubmissionResponse{
			Accepted: true,
			Message:  "Oferta aceptada y distribuida con éxito.\n",
			Termino:  false,
		}, nil
	}

	// --- Lógica de Falla (W < 2) ---
	terminacionMu.Lock()
	defer terminacionMu.Unlock()
	if sistemaTerminado {
		return &pb.OfferSubmissionResponse{
			Accepted: false,
			Message:  "El Cyberday ha finalizado. No se aceptan más ofertas.",
			Termino:  true,
		}, nil
	}

	// Falla si no se cumple W=2
	fmt.Printf("[Oferta] ❌ Oferta %s RECHAZADA. Sólo se confirmaron %d/%d escrituras. (W=%d requerido).\n", offer.GetOfertaId(), confirmedWrites, W, W)
	return &pb.OfferSubmissionResponse{
		Accepted: false,
		Message:  fmt.Sprintf("Escritura fallida: sólo se confirmaron %d escrituras (W=%d requerido).\n", confirmedWrites, W),
		Termino:  false,
	}, nil
}

// -------------------------------------------------------------------------
// Función Principal
// -------------------------------------------------------------------------

func main() {
	lis, err := net.Listen("tcp", brokerPort)
	if err != nil {
		fmt.Printf("Fallo al escuchar en: %v\n", err)
		return
	}

	s := grpc.NewServer()
	brokerServer := NewBrokerServer()

	// FASE 4: Cargar preferencias antes de empezar a servir
	if err := brokerServer.loadConsumerPreferences(); err != nil {
		fmt.Printf("🛑 Error fatal al cargar preferencias de consumidores: %v\n", err)
		return
	}

	// 1. Registrar el servicio EntityManagement (Fase 1)
	pb.RegisterEntityManagementServer(s, brokerServer)
	pb.RegisterConfirmarInicioServer(s, brokerServer)

	// 2. Registrar el servicio OfferSubmission (Fase 2)
	pb.RegisterOfferSubmissionServer(s, brokerServer)

	fmt.Printf("Broker central escuchando y esperando registros en %s...\n", brokerPort)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("Fallo al servir: %v\n", err)
		return
	}
}
