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

const direccionBroker = "broker:50095"
const outputDir = "/app/output"
const archivoPreferencias = "Consumidores/consumidores.csv"

// Colores para mayor diferenciacion en prints
const (
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Reset  = "\033[0m"
)

// Util para detectar fallas
var (
	isFailing bool
	failMu    sync.Mutex
)

// Flags default (cambian con cada consumidor)
var (
	entityID   = flag.String("id", "C1-1", "ID único del consumidor.")
	entityPort = flag.String("port", ":50071", "Puerto local gRPC del consumidor.")
)

// Server Consumidor
type ConsumerServer struct {
	pb.UnimplementedConsumerServer
	pb.UnimplementedFinalizacionServer

	entityID   string
	stopCh     chan struct{}
	grpcServer *grpc.Server
}

type camposOfertas struct {
	id        string
	fecha     string
	producto  string
	precio    int64
	tienda    string
	categoria string
}

var RegistroOfertas []camposOfertas

// Preferencia de cada consumidor
type Preference struct {
	Categorias map[string]bool
	Tiendas    map[string]bool
	PrecioMax  int64
}

var (
	myPrefs *Preference
)

/*
Nombre: NewConsumerServer
Parámetros: id string
Retorno: *ConsumerServer
Descripción: Crea un servidor de consumidor con canales y estado inicial.
*/
func NewConsumerServer(id string) *ConsumerServer {
	return &ConsumerServer{entityID: id, stopCh: make(chan struct{})}
}

/*
Nombre: GenerarNombresCSV
Parámetros: entityID string
Retorno: string
Descripción: Devuelve la ruta del CSV de salida para un consumidor especifico.
*/
func GenerarNombresCSV(entityID string) string {
	return fmt.Sprintf("%s/ofertas_%s.csv", outputDir, entityID)
}

/*
Nombre: CargarPreferencias
Parámetros: path string, id string
Retorno: (*Preference, error)
Descripción: Carga las preferencias del consumidor desde el consumidores.csv.
*/
func CargarPreferencias(path, id string) (*Preference, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error al abrir %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	if _, err := r.Read(); err != nil {
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

		categorias := make(map[string]bool)
		if strings.ToLower(rec[1]) != "null" && rec[1] != "" {
			for _, cat := range strings.Split(rec[1], ";") {
				categorias[strings.TrimSpace(cat)] = true
			}
		}
		tiendas := make(map[string]bool)
		if strings.ToLower(rec[2]) != "null" && rec[2] != "" {
			for _, st := range strings.Split(rec[2], ";") {
				tiendas[strings.TrimSpace(st)] = true
			}
		}
		var precioMAx int64 = -1
		if strings.ToLower(rec[3]) != "null" && rec[3] != "" {
			if p, err := strconv.ParseInt(rec[3], 10, 64); err == nil {
				precioMAx = p
			}
		}

		return &Preference{
			Categorias: categorias,
			Tiendas:    tiendas,
			PrecioMax:  precioMAx,
		}, nil
	}
	return nil, nil
}

/*
Nombre: Filtrado
Parámetros: offer *pb.Oferta, prefs *Preference
Retorno: bool
Descripción: Verifica si una oferta es relevante para el consumidor.
*/
func Filtrado(offer *pb.Oferta, prefs *Preference) bool {
	if prefs == nil {
		return true
	}
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
Nombre: GenerarCSVInicial
Parámetros: fileName string
Retorno: error
Descripción: Crea un CSV vacio solo con encabezado (campos de las ofertas a almacenar).
*/
func GenerarCSVInicial(fileName string) error {
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

/*
Nombre: EscribirCSV
Parámetros: fileName string, ofertas []camposOfertas
Retorno: error
Descripción: Escribe todas las ofertas registradas en el CSV final.
*/
func EscribirCSV(fileName string, ofertas []camposOfertas) error {
	if err := GenerarCSVInicial(fileName); err != nil {
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

/*
Nombre: ForzarCierre
Parámetros: ninguno
Retorno: ninguno
Descripción: Detiene el servidor gRPC de manera ordenada tras un breve delay.
*/
func (s *ConsumerServer) ForzarCierre() {
	go func() {
		time.Sleep(600 * time.Millisecond)
		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
		}
	}()
}

/*
Nombre: RegistrarConBroker
Parámetros: client pb.RegistroEntidadesClient
Retorno: ninguno
Descripción: Registra el consumidor en el broker con su dirección correspondiente.
*/
func RegistrarConBroker(client pb.RegistroEntidadesClient) {
	fmt.Printf("[%s] Coordinando el registro con el Broker...\n", *entityID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dockerServiceName := os.Getenv("HOSTNAME")
	if dockerServiceName == "" {
		dockerServiceName = strings.ToLower(*entityID)
	}
	addressToRegister := dockerServiceName + *entityPort

	req := &pb.SolicitudRegistro{
		IdEntidad:   *entityID,
		TipoEntidad: "Consumer",
		Direccion:   addressToRegister,
	}
	resp, err := client.RegistrarEntidad(ctx, req)
	if err != nil || !resp.GetExito() {
		fmt.Printf("[%s] Registro fallido: %v\n", *entityID, err)
		os.Exit(1)
	}
	fmt.Printf("[%s] Respuesta del Broker: %s\n", *entityID, resp.GetMensaje())
}

/*
Nombre: RecibirOferta
Parámetros: ctx context.Context, offer *pb.Oferta
Retorno: (*pb.RespuestaConsumidor, error)
Descripción: Recibe ofertas, filtra por preferencias y las almacena.
*/
func (s *ConsumerServer) RecibirOferta(ctx context.Context, offer *pb.Oferta) (*pb.RespuestaConsumidor, error) {
	failMu.Lock()
	failing := isFailing
	failMu.Unlock()
	if failing {
		return &pb.RespuestaConsumidor{Exito: false, Mensaje: "Consumidor en fallo; descartada"}, nil
	}

	if !Filtrado(offer, myPrefs) {
		fmt.Printf(Red+"[%s] OFERTA NO RELEVANTE -> ignorada | [%s | %s | %d]\n"+Reset,
			s.entityID, offer.GetCategoria(), offer.GetTienda(), offer.GetPrecio())
		return &pb.RespuestaConsumidor{Exito: true, Mensaje: "Irrelevante (no almacenada)"}, nil
	}

	fmt.Printf("[%s] NUEVA OFERTA RECIBIDA: %s (Prod: %s, Precio: %d, Tienda: %s, Cat: %s)\n",
		s.entityID, offer.GetOfertaId(), offer.GetProducto(), offer.GetPrecio(), offer.GetTienda(), offer.GetCategoria())

	RegistroOfertas = append(RegistroOfertas, camposOfertas{
		id:        offer.GetOfertaId(),
		fecha:     offer.GetFecha(),
		producto:  offer.GetProducto(),
		precio:    offer.GetPrecio(),
		tienda:    offer.GetTienda(),
		categoria: offer.GetCategoria(),
	})

	return &pb.RespuestaConsumidor{Exito: true, Mensaje: "OK memoria"}, nil
}

/*
Nombre: Resincronizar
Parámetros: myID string, s *ConsumerServer
Retorno: ninguno
Descripción: Pide histórico al broker (solo si se cumple R=2 en broker) y agrega ofertas faltantes.
*/
func Resincronizar(myID string, s *ConsumerServer) {
	conn, err := grpc.Dial(direccionBroker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[%s] no conecta con broker para recovery: %v\n", myID, err)
		return
	}
	defer conn.Close()

	client := pb.NewRecoveryClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetHistorialFiltrado(ctx, &pb.SolicitudHistorialConsumer{IdConsumidor: myID})
	if err != nil || resp == nil {
		fmt.Printf("[%s] Resincronización falló: %v\n", myID, err)
		return
	}

	known := make(map[string]struct{}, len(RegistroOfertas))
	for _, o := range RegistroOfertas {
		known[o.id] = struct{}{}
	}

	added := 0
	for _, of := range resp.GetOfertas() {
		if of == nil || of.GetOfertaId() == "" {
			continue
		}
		if _, ok := known[of.GetOfertaId()]; ok {
			continue
		}
		if Filtrado(of, myPrefs) {
			RegistroOfertas = append(RegistroOfertas, camposOfertas{
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
	fmt.Printf(Green+"[%s] Resincronización completa: Ofertas recibidas=%d | nuevas=%d | total=%d\n"+Reset,
		myID, len(resp.GetOfertas()), added, len(RegistroOfertas))
}

/*
Nombre: reportarCaidaABroker
Parámetros: ninguno
Retorno: ninguno
Descripción: Informa al broker que el consumidor se encuentra caído para que este lo incluya en reporte final.
*/
func (s *ConsumerServer) reportarCaidaABroker() {
	conn, err := grpc.Dial(direccionBroker, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		Tipo: "Consumer",
	}

	_, _ = client.InformarCaida(ctx, req)
}

/*
Nombre: IniciarDaemonDeFallos
Parámetros: ninguno
Retorno: ninguno
Descripción: Simula fallos periódicos con cierta probabilidad y al volver solicita la resincronización.
*/
func (s *ConsumerServer) IniciarDaemonDeFallos() {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	fmt.Printf("[%s] Daemon de fallos: prob=%.0f%%, caída=%s, cada=%s\n",
		s.entityID, 0.15*100, 15*time.Second, 25*time.Second)

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
			if rand.Float64() < 0.15 {
				go func() {
					failMu.Lock()
					if isFailing {
						failMu.Unlock()
						return
					}
					isFailing = true
					failMu.Unlock()

					fmt.Printf(Red+"[%s] CAÍDA INESPERADA... (%s)\n"+Reset, s.entityID, 15*time.Second)
					s.reportarCaidaABroker()
					time.Sleep(15 * time.Second)

					failMu.Lock()
					isFailing = false
					select {
					case <-s.stopCh:
						fmt.Printf(Yellow+"[%s] Sistema finalizado - omitiendo resincronización\n"+Reset, s.entityID)
					default:
						fmt.Printf(Green+"[%s] LEVANTADO NUEVAMENTE, SOLICITANDO RESINCRONIZACIÓN...\n"+Reset, s.entityID)
						Resincronizar(s.entityID, s)
					}
					failMu.Unlock()
				}()
			}
		}
	}
}

/*
Nombre: InformarFinalizacion
Parámetros: ctx context.Context, req *pb.NotificarFin
Retorno: (*pb.ConfirmacionFin, error)
Descripción: Genera el CSV final y termina el consumidor si no está caído.
*/
func (s *ConsumerServer) InformarFinalizacion(ctx context.Context, req *pb.NotificarFin) (*pb.ConfirmacionFin, error) {
	failMu.Lock()
	failing := isFailing
	failMu.Unlock()

	if failing {
		fmt.Printf(Red+"[%s] CAÍDO. No genera CSV final\n"+Reset, s.entityID)
		s.ForzarCierre()
		return &pb.ConfirmacionFin{Confirmacionconsumidor: false}, nil
	}

	if !req.GetFin() {
		return &pb.ConfirmacionFin{Confirmacionconsumidor: false}, nil
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
		s.ForzarCierre()
		return &pb.ConfirmacionFin{Confirmacionconsumidor: false}, nil
	}
	fmt.Printf(Green+"[%s] CSV final generado con %d ofertas\n"+Reset, s.entityID, len(RegistroOfertas))

	s.ForzarCierre()
	return &pb.ConfirmacionFin{Confirmacionconsumidor: true}, nil
}

/*
Nombre: main
Parámetros: ninguno
Retorno: ninguno
Descripción: Inicia el consumidor, se registra, arranca gRPC y daemon de fallos.
*/
func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	if p, err := CargarPreferencias(archivoPreferencias, *entityID); err != nil {
		fmt.Printf(Red+"[%s] Error preferencias inicial: %v\n"+Reset, *entityID, err)
	} else {
		myPrefs = p
		fmt.Printf("Preferencias cargadas desde archivo csv.\n")
	}

	consumer := NewConsumerServer(*entityID)

	connReg, err := grpc.Dial(direccionBroker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[%s] No conecta broker registro: %v\n", *entityID, err)
		os.Exit(1)
	}
	defer connReg.Close()
	clientReg := pb.NewRegistroEntidadesClient(connReg)
	RegistrarConBroker(clientReg)

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
