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

const (
	brokerPort = ":50095"
	N          = 3 // réplicas
	W          = 2 // escrituras requeridas
	R          = 2 // lecturas requeridas

	archivoPreferencias = "Broker/consumidores.csv"
	TOPE_OFERTAS        = 25 // Limite predefinido para maximo de ofertas y asi evitar sistema infinito
)

// Variables utiles para mantener registro del estado del sistema
var (
	contadorRegistrados int64
	ofertasParisio      int64
	ofertasRiploy       int64
	ofertasFalabellox   int64
	terminacionMu       sync.Mutex
	sistemaTerminado    bool

	ofertaIDs []string
	idsMu     sync.Mutex

	escriturasExitosas int64
	escriturasFallidas int64
	lecturasExitosas   int64
	lecturasFallidas   int64

	nodosCaidosAlFinalizar int64
	nodosCaidosArray       []string

	caidasMu            sync.Mutex
	caidasPorNodo       = map[string]int{}
	caidasPorConsumidor = map[string]int{}

	ofertasMu            sync.Mutex
	ofertasPorConsumidor = map[string]int{}

	resincronizacionMu           sync.Mutex
	resincronizacionConsumidores = map[string]int{}

	confirmacionMu              sync.Mutex
	confirmacionCSVConsumidores = map[string]bool{}
)

// Entidades (DB, Consumidores, Productores)
type Entidad struct {
	ID      string
	Type    string
	Address string
}

// Preferencia de un consumidor
type Preferencia struct {
	ID         string
	Categorias map[string]bool
	Tiendas    map[string]bool
	PrecioMax  int64
}

// Server Broker
type BrokerServer struct {
	pb.UnimplementedRegistroEntidadesServer
	pb.UnimplementedOfertasServer
	pb.UnimplementedConfirmarInicioServer
	pb.UnimplementedRecoveryServer
	pb.UnimplementedCaidaServer

	entidades         map[string]Entidad
	nodosDB           map[string]Entidad
	consumidores      map[string]Entidad
	prefsConsumidores map[string]Preferencia
	mu                sync.Mutex
	grpcServer        *grpc.Server
}

// Colores para mayor diferenciacion en prints
const (
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Reset  = "\033[0m"
)

/*
Nombre: NewBrokerServer
Parámetros: ninguno
Retorno: *BrokerServer
Descripción: Crea e inicializa el servidor del broker con mapas vacíos.
*/
func NewBrokerServer() *BrokerServer {
	return &BrokerServer{
		entidades:         make(map[string]Entidad),
		nodosDB:           make(map[string]Entidad),
		consumidores:      make(map[string]Entidad),
		prefsConsumidores: make(map[string]Preferencia),
	}
}

/*
Nombre: ForzarCierre
Parámetros: ninguno
Retorno: ninguno
Descripción: Detiene el servidor gRPC de manera ordenada tras un breve delay.
*/
func (s *BrokerServer) ForzarCierre() {
	go func() {
		time.Sleep(600 * time.Millisecond)
		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
		}
	}()
}

/*
Nombre: VerificarID
Parámetros: id string
Retorno: bool
Descripción: Revisa si un ID de oferta ya existe en la lista para evitar duplicados.
*/
func VerificarID(id string) bool {
	for _, v := range ofertaIDs {
		if v == id {
			return true
		}
	}
	return false
}

/*
Nombre: CargarPreferencias
Parámetros: ninguno
Retorno: error
Descripción: Lee el CSV de preferencias de consumidores y almacena el map asociado
*/
func (s *BrokerServer) CargarPreferencias() error {
	file, err := os.Open(archivoPreferencias)
	if err != nil {
		return fmt.Errorf("error al abrir %s: %w", archivoPreferencias, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	if _, err := reader.Read(); err != nil {
		if err == io.EOF {
			return fmt.Errorf("archivo %s vacío", archivoPreferencias)
		}
		return fmt.Errorf("error con cabecera %s: %w", archivoPreferencias, err)
	}

	prefsMap := make(map[string]Preferencia)
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

		categorias := make(map[string]bool)
		if strings.ToLower(record[1]) != "null" && record[1] != "" {
			for _, cat := range strings.Split(record[1], ";") {
				categorias[strings.TrimSpace(cat)] = true
			}
		}
		tiendas := make(map[string]bool)
		if strings.ToLower(record[2]) != "null" && record[2] != "" {
			for _, st := range strings.Split(record[2], ";") {
				tiendas[strings.TrimSpace(st)] = true
			}
		}
		var precioMax int64 = -1
		if strings.ToLower(record[3]) != "null" && record[3] != "" {
			if p, err := strconv.ParseInt(record[3], 10, 64); err == nil {
				precioMax = p
			}
		}

		prefsMap[id] = Preferencia{
			ID:         id,
			Categorias: categorias,
			Tiendas:    tiendas,
			PrecioMax:  precioMax,
		}
		recordsRead++
	}

	s.mu.Lock()
	s.prefsConsumidores = prefsMap
	s.mu.Unlock()

	fmt.Printf("Preferencias de %d consumidores cargadas.\n", recordsRead)
	return nil
}

/*
Nombre: Filtrado
Parámetros: offer *pb.Oferta, prefs Preferencia
Retorno: bool
Descripción: Determina si una oferta cumple las preferencias prefs (tiendas, categorías y precio).
*/
func (s *BrokerServer) Filtrado(offer *pb.Oferta, prefs Preferencia) bool {
	if len(prefs.Tiendas) > 0 {
		if _, ok := prefs.Tiendas[offer.GetTienda()]; !ok {
			return false
		}
	}
	if len(prefs.Categorias) > 0 {
		if _, ok := prefs.Categorias[offer.GetCategoria()]; !ok {
			return false
		}
	}
	if prefs.PrecioMax != -1 && offer.GetPrecio() > prefs.PrecioMax {
		return false
	}
	return true
}

/*
Nombre: RegistrarEntidad
Parámetros: ctx context.Context, req *pb.SolicitudRegistro
Retorno: (*pb.RespuestaRegistro, error)
Descripción: Registra DBs y consumidores; valida que el consumidor exista en el consumidores.csv.
*/
func (s *BrokerServer) RegistrarEntidad(ctx context.Context, req *pb.SolicitudRegistro) (*pb.RespuestaRegistro, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := req.GetIdEntidad()
	if _, exists := s.entidades[id]; exists {
		return &pb.RespuestaRegistro{Exito: false, Mensaje: "Entidad ya registrada\n"}, nil
	}

	ent := Entidad{
		ID:      id,
		Type:    req.GetTipoEntidad(),
		Address: req.GetDireccion(),
	}
	s.entidades[id] = ent

	switch ent.Type {
	case "DBNode":
		s.nodosDB[id] = ent
	case "Consumer":
		if _, ok := s.prefsConsumidores[id]; !ok {
			delete(s.entidades, id)
			fmt.Printf(Red+"[Registro]"+Reset+" Consumidor %s RECHAZADO. No encontrado en consumidores.csv.\n", id)
			return &pb.RespuestaRegistro{
				Exito:   false,
				Mensaje: "Registro fallido. Su ID de consumidor no está en la lista de preferencias.\n",
			}, nil
		}
		s.consumidores[id] = ent
	default:
	}

	fmt.Printf("[Registro] %s registrad@ correctamente (%s) en %s. Total de registrados: %d\n",
		id, ent.Type, ent.Address, len(s.entidades))

	atomic.AddInt64(&contadorRegistrados, 1)
	return &pb.RespuestaRegistro{
		Exito:   true,
		Mensaje: "Registro exitoso. Bienvenido al CyberDay!.\n",
	}, nil
}

/*
Nombre: Confirmacion
Parámetros: ctx context.Context, _ *pb.SolicitudInicio
Retorno: (*pb.RespuestaInicio, error)
Descripción: Indica si ya se registraron todas las entidades necesarias (18).
*/
func (s *BrokerServer) Confirmacion(ctx context.Context, _ *pb.SolicitudInicio) (*pb.RespuestaInicio, error) {
	if contadorRegistrados < 18 {
		return &pb.RespuestaInicio{Listo: false}, nil
	}
	return &pb.RespuestaInicio{Listo: true}, nil
}

/*
Nombre: NotificarConsumidores
Parámetros: offer *pb.Oferta
Retorno: ninguno
Descripción: Notifica la oferta a consumidores relevantes según las preferencias almacenada.
*/
func (s *BrokerServer) NotificarConsumidores(offer *pb.Oferta) {
	s.mu.Lock()
	cons := make(map[string]Entidad, len(s.consumidores))
	for k, v := range s.consumidores {
		cons[k] = v
	}
	prefsAux := make(map[string]Preferencia, len(s.prefsConsumidores))
	for k, v := range s.prefsConsumidores {
		prefsAux[k] = v
	}
	s.mu.Unlock()

	var wg sync.WaitGroup
	var mu sync.Mutex
	contadorEnviados := 0

	for _, c := range cons {
		prefs, ok := prefsAux[c.ID]
		if !ok || !s.Filtrado(offer, prefs) {
			continue
		}
		wg.Add(1)
		go func(consumer Entidad) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
			defer cancel()

			conn, err := grpc.Dial(consumer.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			defer conn.Close()

			cli := pb.NewConsumerClient(conn)
			resp, err := cli.RecibirOferta(ctx, offer)
			if err != nil || resp == nil || !resp.GetExito() {
				return
			}

			ofertasMu.Lock()
			ofertasPorConsumidor[consumer.ID]++
			ofertasMu.Unlock()

			mu.Lock()
			contadorEnviados++
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	fmt.Printf(Yellow+"[Oferta %s]"+Reset+" Oferta enviado a %d consumidores\n\n\n", offer.GetOfertaId(), contadorEnviados)
}

/*
Nombre: EnviarOferta
Parámetros: ctx context.Context, offer *pb.Oferta
Retorno: (*pb.RespuestaOferta, error)
Descripción: Realiza escritura, buscando asegurar W=2 en nodos DB y notifica a consumidores. Tambien se asegura de que las tiendas no sobrepasen el limite de ofertas enviadas.
*/
func (s *BrokerServer) EnviarOferta(ctx context.Context, offer *pb.Oferta) (*pb.RespuestaOferta, error) {
	terminacionMu.Lock()
	if sistemaTerminado {
		terminacionMu.Unlock()
		return &pb.RespuestaOferta{Aceptado: false, Mensaje: "Cyberday finalizado", Termino: true}, nil
	}
	terminacionMu.Unlock()

	id := strings.TrimSpace(offer.GetOfertaId())
	if id == "" {
		return &pb.RespuestaOferta{
			Aceptado: false,
			Mensaje:  "ID de oferta vacío o inválido",
			Termino:  false,
		}, nil
	}

	idsMu.Lock()
	if VerificarID(id) {
		idsMu.Unlock()
		fmt.Printf(Yellow+"[Oferta %s] Rechazada: ID duplicado\n\n"+Reset, id)
		return &pb.RespuestaOferta{
			Aceptado: false,
			Mensaje:  "ID de oferta duplicado",
			Termino:  false,
		}, nil
	}
	idsMu.Unlock()

	terminacionMu.Lock()
	switch offer.GetTienda() {
	case "Parisio":
		if ofertasParisio >= TOPE_OFERTAS {
			terminacionMu.Unlock()
			return &pb.RespuestaOferta{Aceptado: false, Mensaje: "Límite Parisio", Termino: false}, nil
		}
	case "Falabellox":
		if ofertasFalabellox >= TOPE_OFERTAS {
			terminacionMu.Unlock()
			return &pb.RespuestaOferta{Aceptado: false, Mensaje: "Límite Falabellox", Termino: false}, nil
		}
	case "Riploy":
		if ofertasRiploy >= TOPE_OFERTAS {
			terminacionMu.Unlock()
			return &pb.RespuestaOferta{Aceptado: false, Mensaje: "Límite Riploy", Termino: false}, nil
		}
	}
	terminacionMu.Unlock()

	fmt.Printf(Yellow+"[Oferta %s]"+Reset+" Iniciando escritura distribuida (N=%d, W=%d)\n", offer.GetOfertaId(), N, W)

	if len(s.nodosDB) < N {
		return &pb.RespuestaOferta{
			Aceptado: false,
			Mensaje:  fmt.Sprintf("No hay N=%d DBs activas\n", N),
			Termino:  false,
		}, nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	confirmed := 0

	s.mu.Lock()
	nodes := make([]Entidad, 0, len(s.nodosDB))
	for _, n := range s.nodosDB {
		nodes = append(nodes, n)
	}
	s.mu.Unlock()

	for _, node := range nodes {
		wg.Add(1)
		go func(n Entidad) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			conn, err := grpc.Dial(n.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			defer conn.Close()

			cli := pb.NewNodoDBClient(conn)
			resp, err := cli.AlmacenarOferta(ctx, offer)
			if err != nil || resp == nil || !resp.GetExito() {
				return
			}
			mu.Lock()
			confirmed++
			mu.Unlock()
		}(node)
	}

	wg.Wait()
	if confirmed >= W {
		escriturasExitosas++
		fmt.Printf(Yellow+"[Oferta %s]"+Reset+" Escritura exitosa, W cumplido\n", offer.GetOfertaId())
		go s.NotificarConsumidores(offer)

		terminacionMu.Lock()
		defer terminacionMu.Unlock()

		if !sistemaTerminado {
			switch offer.GetTienda() {
			case "Parisio":
				ofertasParisio++
			case "Falabellox":
				ofertasFalabellox++
			case "Riploy":
				ofertasRiploy++
			}
		}

		if ofertasFalabellox >= TOPE_OFERTAS && ofertasParisio >= TOPE_OFERTAS && ofertasRiploy >= TOPE_OFERTAS {
			time.Sleep(2 * time.Second)
			fmt.Println("\n=======================================================")
			fmt.Println("Límite de ofertas alcanzado! Finalizando CyberDay...")
			fmt.Printf("=======================================================\n\n")
			sistemaTerminado = true
			s.FinalizarSistema()
			return &pb.RespuestaOferta{Aceptado: true, Mensaje: "Finalizado", Termino: true}, nil
		}

		return &pb.RespuestaOferta{Aceptado: true, Mensaje: "OK", Termino: false}, nil
	} else {
		escriturasFallidas++
		fmt.Printf(Yellow+"[Oferta %s]"+Reset+" Escritura no exitosa, W no cumplido\n\n", offer.GetOfertaId())
	}

	return &pb.RespuestaOferta{
		Aceptado: false,
		Mensaje:  fmt.Sprintf("W no alcanzado: %d/%d", confirmed, W),
		Termino:  false,
	}, nil
}

/*
Nombre: offersEqual
Parámetros: a []*pb.Oferta, b []*pb.Oferta
Retorno: bool
Descripción: Verifica igualdad listas de ofertas por ID y campos clave para poder verificar R=2 en caso de historiales exactamente iguales.
*/
func offersEqual(a, b []*pb.Oferta) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]*pb.Oferta, len(a))
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

/*
Nombre: RecuperarHistorialDesdeDB
Parámetros: node Entidad, myID string, timeout time.Duration
Retorno: ([]*pb.Oferta, error)
Descripción: Solicita el historial de ofertas a una DB específica.
*/
func (s *BrokerServer) RecuperarHistorialDesdeDB(node Entidad, myID string, timeout time.Duration) ([]*pb.Oferta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.Dial(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewNodoDBClient(conn)
	resp, err := client.GetHistorialOfertas(ctx, &pb.SolicitudHistorialBD{IdNodo: myID})
	if err != nil {
		return nil, err
	}
	return resp.GetOfertas(), nil
}

type RecoveryServer struct {
	pb.UnimplementedRecoveryServer
	broker *BrokerServer
}

/*
Nombre: GetHistorialFiltrado
Parámetros: ctx context.Context, req *pb.SolicitudHistorialConsumer
Retorno: (*pb.RespuestaHistorialConsumer, error)
Descripción: Devuelve histórico consistente (R=2) filtrado por preferencias del consumidor.
*/
func (r *RecoveryServer) GetHistorialFiltrado(ctx context.Context, req *pb.SolicitudHistorialConsumer) (*pb.RespuestaHistorialConsumer, error) {

	consumerID := req.GetIdConsumidor()
	if consumerID == "" {
		return &pb.RespuestaHistorialConsumer{Ofertas: nil}, nil
	}

	r.broker.mu.Lock()
	nodes := make([]Entidad, 0, len(r.broker.nodosDB))
	for _, n := range r.broker.nodosDB {
		nodes = append(nodes, n)
	}
	prefs, havePrefs := r.broker.prefsConsumidores[consumerID]
	r.broker.mu.Unlock()

	if len(nodes) < 2 {
		return &pb.RespuestaHistorialConsumer{Ofertas: nil}, nil
	}

	type hs struct {
		arr []*pb.Oferta
		err error
	}
	results := make([]hs, len(nodes))

	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, node Entidad) {
			defer wg.Done()
			arr, err := r.broker.RecuperarHistorialDesdeDB(node, "BROKER", 3*time.Second)
			results[i] = hs{arr, err}
		}(i, n)
	}
	wg.Wait()

	var chosen []*pb.Oferta
	matchesFound := false
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

	resincronizacionMu.Lock()
	resincronizacionConsumidores[consumerID]++
	resincronizacionMu.Unlock()

	fmt.Printf("Solicitud de histórico por parte de %s...\n", consumerID)
	if matchesFound {
		lecturasExitosas++
		fmt.Printf("Lectura exitosa, se cumple R=2. Enviando histórico a %s...\n\n", consumerID)
	} else {
		lecturasFallidas++
		fmt.Printf("Lectura no exitosa, no se cumple R=2. No se le enviará nada a %s...\n\n", consumerID)
		return &pb.RespuestaHistorialConsumer{Ofertas: nil}, nil
	}

	if chosen == nil {
		return &pb.RespuestaHistorialConsumer{Ofertas: nil}, nil
	}

	if havePrefs {
		filtered := make([]*pb.Oferta, 0, len(chosen))
		for _, of := range chosen {
			if of != nil && r.broker.Filtrado(of, prefs) {
				filtered = append(filtered, of)
			}
		}
		return &pb.RespuestaHistorialConsumer{Ofertas: filtered}, nil
	}
	return &pb.RespuestaHistorialConsumer{Ofertas: chosen}, nil
}

/*
Nombre: informarFinAConsumer
Parámetros: consumer Entidad, timeout time.Duration
Retorno: ninguno
Descripción: Envía señal de fin a un consumidor y registra si generó CSV final.
*/
func (s *BrokerServer) informarFinAConsumer(consumer Entidad, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.Dial(consumer.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf(Blue+"[Fin]"+Reset+" No conecta con consumidor %s: %v\n", consumer.ID, err)
		confirmacionMu.Lock()
		confirmacionCSVConsumidores[consumer.ID] = false
		confirmacionMu.Unlock()
		return
	}
	defer conn.Close()

	c := pb.NewFinalizacionClient(conn)
	resp, err := c.InformarFinalizacion(ctx, &pb.NotificarFin{Fin: true})
	if err != nil {
		fmt.Printf(Red+"[Fin]"+Reset+" Consumidor %s CAÍDO: %v\n", consumer.ID, err)
		confirmacionMu.Lock()
		confirmacionCSVConsumidores[consumer.ID] = false
		confirmacionMu.Unlock()
		return
	}

	if resp != nil && resp.GetConfirmacionconsumidor() {
		confirmacionMu.Lock()
		confirmacionCSVConsumidores[consumer.ID] = true
		confirmacionMu.Unlock()
		fmt.Printf(Green+"[Fin]"+Reset+" Consumidor %s generó CSV final\n", consumer.ID)
	} else {
		confirmacionMu.Lock()
		confirmacionCSVConsumidores[consumer.ID] = false
		confirmacionMu.Unlock()
		fmt.Printf(Red+"[Fin]"+Reset+" Consumidor %s NO generó CSV final\n", consumer.ID)
	}
}

/*
Nombre: informarFinADB
Parámetros: node Entidad, timeout time.Duration
Retorno: ninguno
Descripción: Envía señal de fin a una DB y registra si confirmó o si estaba caída.
*/
func (s *BrokerServer) informarFinADB(node Entidad, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.Dial(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf(Blue+"[Fin]"+Reset+" %s CAÍDA: %v\n", node.ID, err)
		s.mu.Lock()
		nodosCaidosAlFinalizar++
		nodosCaidosArray = append(nodosCaidosArray, node.ID)
		s.mu.Unlock()
		return
	}
	defer conn.Close()

	c := pb.NewFinalizacionClient(conn)
	resp, err := c.InformarFinalizacion(ctx, &pb.NotificarFin{Fin: true})

	if err != nil {
		fmt.Printf(Red+"[Fin]"+Reset+" %s CAÍDA: %v\n", node.ID, err)
		s.mu.Lock()
		nodosCaidosAlFinalizar++
		nodosCaidosArray = append(nodosCaidosArray, node.ID)
		s.mu.Unlock()
		return
	}

	if resp == nil || !resp.GetConfirmacionbd() {
		fmt.Printf(Red+"[Fin]"+Reset+" %s CAÍDA.\n", node.ID)
		s.mu.Lock()
		nodosCaidosAlFinalizar++
		nodosCaidosArray = append(nodosCaidosArray, node.ID)
		s.mu.Unlock()
		return
	}

	fmt.Printf(Green+"[Fin]"+Reset+" %s confirmó finalización correctamente\n", node.ID)
}

type CaidaServer struct {
	pb.UnimplementedCaidaServer
	broker *BrokerServer
}

/*
Nombre: InformarCaida
Parámetros: ctx context.Context, req *pb.FailNotify
Retorno: (*pb.FailACK, error)
Descripción: Registra una caída reportada por DB o consumidor para poder incluirlas en reporte final.
*/
func (c *CaidaServer) InformarCaida(ctx context.Context, req *pb.FailNotify) (*pb.FailACK, error) {
	fmt.Printf(Red+"[Caída] %s (%s) reportó una caída\n"+Reset, req.GetId(), req.GetTipo())

	caidasMu.Lock()
	if req.GetTipo() == "DBNode" {
		caidasPorNodo[req.GetId()]++
	} else if req.GetTipo() == "Consumer" {
		caidasPorConsumidor[req.GetId()]++
	}
	caidasMu.Unlock()

	return &pb.FailACK{Ack: true}, nil
}

/*
Nombre: FinalizarSistema
Parámetros: ninguno
Retorno: ninguno
Descripción: Lanza avisos de finalización y luego genera el reporte final.
*/
func (s *BrokerServer) FinalizarSistema() {
	fmt.Println(Blue + "[Fin]" + Reset + " Notificando finalización...")

	s.mu.Lock()
	dbs := make([]Entidad, 0, len(s.nodosDB))
	for _, n := range s.nodosDB {
		dbs = append(dbs, n)
	}
	cons := make([]Entidad, 0, len(s.consumidores))
	for _, c := range s.consumidores {
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
		time.Sleep(5 * time.Second)
		s.GenerarReporteTXT()
		s.ForzarCierre()
	}()
}

/*
Nombre: GenerarReporteTXT
Parámetros: ninguno
Retorno: ninguno
Descripción: Escribe métricas finales y conclusión del sistema en un archivo TXT. Existen distintas posibles conclusiones basadas en combinaciones de metricas.
*/
func (s *BrokerServer) GenerarReporteTXT() {
	reportPath := "/app/Broker/Reporte.txt"

	file, err := os.Create(reportPath)
	if err != nil {
		fmt.Printf("Error creando archivo de reporte: %v\n", err)
		return
	}
	defer file.Close()

	fmt.Fprintln(file, "================= REPORTE FINAL =================")

	fmt.Fprintf(file, "\nOfertas por tienda (Recibidas por Broker):\n")
	fmt.Fprintf(file, "  Parisio: %d\n", ofertasParisio)
	fmt.Fprintf(file, "  Falabellox: %d\n", ofertasFalabellox)
	fmt.Fprintf(file, "  Riploy: %d\n\n", ofertasRiploy)
	escriturasExitosas = escriturasExitosas - escriturasFallidas
	fmt.Fprintf(file, "Escrituras exitosas (W=2): %d\n", escriturasExitosas)
	fmt.Fprintf(file, "Escrituras fallidas (W<2): %d\n\n", escriturasFallidas)

	fmt.Fprintf(file, "Lecturas exitosas (R=2): %d\n", lecturasExitosas)
	fmt.Fprintf(file, "Lecturas fallidas (R<2): %d\n\n", lecturasFallidas)

	fmt.Fprintf(file, "Nodos BD caídos al finalizar: %d\n", nodosCaidosAlFinalizar)
	for _, id := range nodosCaidosArray {
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
	for id, n := range resincronizacionConsumidores {
		fmt.Fprintf(file, "  %s: %d\n", id, n)
	}

	fmt.Fprintln(file, "\nResumen por Consumidor:")
	s.mu.Lock()
	consumerIDs := make([]string, 0, len(s.consumidores))
	for id := range s.consumidores {
		consumerIDs = append(consumerIDs, id)
	}
	s.mu.Unlock()

	for _, id := range consumerIDs {
		ofertasRecibidas := ofertasPorConsumidor[id]
		csvGenerado := confirmacionCSVConsumidores[id]
		estadoCSV := "generado"
		if !csvGenerado {
			estadoCSV = "no generado"
		}
		fmt.Fprintf(file, "  %s: %d ofertas recibidas (directas). Archivo CSV %s\n", id, ofertasRecibidas, estadoCSV)
	}

	conclusion := ""

	if len(caidasPorNodo) == 0 && escriturasFallidas == 0 && lecturasFallidas == 0 {
		conclusion = "NO EVIDENCIA DE TOLERANCIA A FALLOS: El sistema mantuvo la consistencia pero no se pudo evidenciar su tolerancia a fallos, ya que no se registró ninguna caída de nodos DB."
	} else if (nodosCaidosAlFinalizar > 0 || len(caidasPorNodo) > 0) && escriturasFallidas == 0 && lecturasFallidas == 0 {
		conclusion = "SISTEMA ROBUSTO Y CONSISTENTE: El sistema demostró exitosamente su Tolerancia a Fallos y Consistencia. A pesar de registrarse caída(s) temporal(es) de nodos DB, todas las operaciones de escritura y lectura fueron exitosas, lo que prueba la correcta aplicación de los quorums W=2 y R=2 para garantizar la disponibilidad y la integridad del dato."
	} else if escriturasFallidas > 0 || lecturasFallidas > 0 {
		fallos := ""
		if escriturasFallidas > 0 {
			fallos += fmt.Sprintf("%d escrituras fallidas (W<2)", escriturasFallidas)
		}
		if lecturasFallidas > 0 {
			if fallos != "" {
				fallos += " y "
			}
			fallos += fmt.Sprintf("%d lecturas fallidas (R<2)", lecturasFallidas)
		}
		conclusion = "SISTEMA CON DEGRADACIÓN: El sistema mantuvo disponibilidad pero con pérdida parcial de consistencia. " +
			fmt.Sprintf("Se presentaron %s debido a caídas simultáneas en múltiples nodos. ", fallos)
		if escriturasExitosas > escriturasFallidas || lecturasExitosas > lecturasFallidas {
			conclusion += "Sin embargo, el sistema logró permanecer operativo y la mayoría de operaciones se completaron exitosamente."
		}
	} else if nodosCaidosAlFinalizar >= 2 && (escriturasFallidas > 0 || lecturasFallidas > 0) {
		conclusion = "SISTEMA CRÍTICO: El sistema experimentó fallos significativos que afectaron la consistencia. " +
			"Con nodos DB caídos y operaciones fallidas, " +
			"la disponibilidad se mantuvo pero la consistencia bajo reglas específicadas no pudo garantizarse completamente."
	} else if len(resincronizacionConsumidores) > 0 && lecturasFallidas == 0 {
		totalResync := 0
		for _, n := range resincronizacionConsumidores {
			totalResync += n
		}
		conclusion = "SISTEMA RECUPERADO: El sistema superó fallos temporales mediante resincronización exitosa. " +
			fmt.Sprintf("Se recuperaron %d consumidores y se mantuvo la consistencia en lecturas. ", totalResync) +
			"La replicación eventual funcionó correctamente para restaurar el estado del sistema."
	} else {
		conclusion = "SISTEMA FUNCIONAL: El sistema operó dentro de parámetros aceptables. " +
			"Se presentaron algunos fallos pero el núcleo distributivo mantuvo operatividad. " +
			"La consistencia se preservó en la mayoría de operaciones críticas."
	}

	fmt.Fprintf(file, "\nConlusión final:\n%s\n", conclusion)
	fmt.Fprintln(file, "\n==============================================================")
	fmt.Printf(Blue + "[Reporte]" + Reset + " Reporte generado exitosamente\n")
}

// -------------------------------------------------------------------------
// main
// -------------------------------------------------------------------------

/*
Nombre: main
Parámetros: ninguno
Retorno: ninguno
Descripción: Inicializa el broker, registra servicios y sirve mediante gRPC.
*/
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

	if err := bs.CargarPreferencias(); err != nil {
		fmt.Printf("Preferencias error: %v\n", err)
		return
	}

	pb.RegisterRegistroEntidadesServer(s, bs)
	pb.RegisterConfirmarInicioServer(s, bs)
	pb.RegisterOfertasServer(s, bs)

	recoveryServer := &RecoveryServer{broker: bs}
	pb.RegisterRecoveryServer(s, recoveryServer)

	caidaServer := &CaidaServer{broker: bs}
	pb.RegisterCaidaServer(s, caidaServer)

	fmt.Printf("Broker escuchando en %s...\n", brokerPort)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("Serve error: %v\n", err)
	}
}
