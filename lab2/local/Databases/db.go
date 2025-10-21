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

const direccionBroker = "broker:50095"

// Flags default (cambian con cada bd)
var (
	dbNodeID   = flag.String("id", "DB1", "ID del nodo DB (DB1|DB2|DB3)")
	dbNodePort = flag.String("port", ":50061", "Puerto gRPC del nodo DB")
)

// Util para detectar falla
var (
	isFailing bool
	failMu    sync.Mutex
)

// Colores para mayor diferenciacion en prints
const (
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Reset  = "\033[0m"
)

// Server nodo BD
type DBNodeServer struct {
	pb.UnimplementedNodoDBServer
	pb.UnimplementedFinalizacionServer

	entityID   string
	mu         sync.RWMutex
	data       map[string]*pb.Oferta
	stopCh     chan struct{}
	grpcServer *grpc.Server
}

/*
Nombre: NewDBNodeServer
Parámetros: id string
Retorno: *DBNodeServer
Descripción: Crea un server nodo DB para almacenar ofertas.
*/
func NewDBNodeServer(id string) *DBNodeServer {
	return &DBNodeServer{
		entityID: id,
		data:     make(map[string]*pb.Oferta),
		stopCh:   make(chan struct{}),
	}
}

/*
Nombre: ForzarCierre
Parámetros: ninguno
Retorno: ninguno
Descripción: Detiene el servidor gRPC de manera ordenada tras un breve delay.
*/
func (s *DBNodeServer) ForzarCierre() {
	go func() {
		time.Sleep(600 * time.Millisecond)
		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
		}
	}()
}

/*
Nombre: Peers
Parámetros: id string
Retorno: []string
Descripción: Devuelve las direcciones de los nodos DB restantes.
*/
func Peers(id string) []string {
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

/*
Nombre: RegistrarConBroker
Parámetros: client pb.RegistroEntidadesClient, server *DBNodeServer
Retorno: ninguno
Descripción: Registra este nodo DB en el broker con su dirección correspondiente.
*/
func RegistrarConBroker(client pb.RegistroEntidadesClient, server *DBNodeServer) {
	fmt.Println("Registrando con Broker...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dockerServiceName := strings.ToLower(server.entityID)
	addressToRegister := dockerServiceName + *dbNodePort

	req := &pb.SolicitudRegistro{
		IdEntidad:   server.entityID,
		TipoEntidad: "DBNode",
		Direccion:   addressToRegister,
	}

	resp, err := client.RegistrarEntidad(ctx, req)
	if err != nil || !resp.GetExito() {
		fmt.Printf("Registro con broker falló: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Broker respondió: %s\n", resp.GetMensaje())
}

/*
Nombre: Resincronizar
Parámetros: myID string, s *DBNodeServer
Retorno: ninguno
Descripción: Copia ofertas desde nodos hasta lograr el estado más reciente (consistencia). Indica desde quien se resincroniza y cuanta data nueva recibio.
*/
func Resincronizar(myID string, s *DBNodeServer) {
	addrs := Peers(myID)
	for _, addr := range addrs {
		fmt.Printf("[%s] Resincronizando desde %s ...\n", myID, addr)
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		client := pb.NewNodoDBClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		resp, err := client.GetHistorialOfertas(ctx, &pb.SolicitudHistorialBD{IdNodo: myID})
		cancel()
		conn.Close()
		if err != nil || resp == nil {
			continue
		}

		added := 0
		s.mu.Lock()
		for _, of := range resp.GetOfertas() {
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
		fmt.Printf(Green+"[%s] Resincronización desde %s | nuevas=%d | total=%d\n"+Reset, myID, addr, added, total)
		return
	}
	fmt.Printf("[%s] No se pudo resincronizar con ningún nodo\n", myID)
}

/*
Nombre: reportarCaidaABroker
Parámetros: ninguno
Retorno: ninguno
Descripción: Informa al broker que el nodo DB se encuentra caído para que este lo pueda incluir en reporte final.
*/
func (s *DBNodeServer) reportarCaidaABroker() {
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
		Tipo: "DBNode",
	}

	_, _ = client.InformarCaida(ctx, req)
}

/*
Nombre: IniciarDaemonDeFallos
Parámetros: ninguno
Retorno: ninguno
Descripción: Simula fallos periódicos con cierta probabilidad y ejecuta resincronización al volver.
*/
func (s *DBNodeServer) IniciarDaemonDeFallos() {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	fmt.Printf("[%s] Daemon de fallos: prob=%.0f%%, caída=%s, cada=%s\n",
		s.entityID, 0.20*100, 15*time.Second, 25*time.Second)

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
						fmt.Printf(Yellow+"[%s] Sistema finalizado durante la caída - omitiendo resincronización\n"+Reset, s.entityID)
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
Nombre: AlmacenarOferta
Parámetros: ctx context.Context, offer *pb.Oferta
Retorno: (*pb.RespuestaAlmacenarOferta, error)
Descripción: Guarda una oferta si no existe y responde éxito o fallo.
*/
func (s *DBNodeServer) AlmacenarOferta(ctx context.Context, offer *pb.Oferta) (*pb.RespuestaAlmacenarOferta, error) {
	failMu.Lock()
	failing := isFailing
	failMu.Unlock()
	if failing {
		return &pb.RespuestaAlmacenarOferta{Exito: false, Mensaje: s.entityID + " en fallo"}, nil
	}
	if offer.GetOfertaId() == "" {
		return &pb.RespuestaAlmacenarOferta{Exito: false, Mensaje: "oferta_id vacío"}, nil
	}

	s.mu.Lock()
	if _, exists := s.data[offer.GetOfertaId()]; !exists {
		s.data[offer.GetOfertaId()] = offer
	}
	total := len(s.data)
	s.mu.Unlock()

	fmt.Printf("[%s] Oferta %s almacenada | total=%d\n", s.entityID, offer.GetOfertaId(), total)
	return &pb.RespuestaAlmacenarOferta{Exito: true, Mensaje: "ok"}, nil
}

/*
Nombre: GetHistorialOfertas
Parámetros: ctx context.Context, req *pb.SolicitudHistorialBD
Retorno: (*pb.RespuestaHistorialBD, error)
Descripción: Retorna todas las ofertas almacenadas en este nodo.
*/
func (s *DBNodeServer) GetHistorialOfertas(ctx context.Context, req *pb.SolicitudHistorialBD) (*pb.RespuestaHistorialBD, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*pb.Oferta, 0, len(s.data))
	for _, of := range s.data {
		out = append(out, of)
	}
	fmt.Printf("[%s] Enviando histórico a %s (%d ofertas)\n", s.entityID, req.GetIdNodo(), len(out))
	return &pb.RespuestaHistorialBD{Ofertas: out}, nil
}

/*
Nombre: InformarFinalizacion
Parámetros: ctx context.Context, req *pb.NotificarFin
Retorno: (*pb.ConfirmacionFin, error)
Descripción: Confirma finalización si el nodo no está fallando y termina al nodo.
*/
func (s *DBNodeServer) InformarFinalizacion(ctx context.Context, req *pb.NotificarFin) (*pb.ConfirmacionFin, error) {
	failMu.Lock()
	failing := isFailing
	failMu.Unlock()

	if failing {
		fmt.Printf(Red+"[%s] DB CAÍDA. No puede confirmar finalización\n"+Reset, s.entityID)
		go s.ForzarCierre()
		return &pb.ConfirmacionFin{Confirmacionbd: false}, nil
	}

	if !req.GetFin() {
		go s.ForzarCierre()
		return &pb.ConfirmacionFin{Confirmacionbd: false}, nil
	}

	select {
	case <-s.stopCh:
		fmt.Printf(Red+"[%s] DB CAÍDA. No puede confirmar finalización\n"+Reset, s.entityID)
		go s.ForzarCierre()
		return &pb.ConfirmacionFin{Confirmacionbd: false}, nil
	default:
	}

	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
	failMu.Lock()
	isFailing = false
	failMu.Unlock()

	fmt.Printf(Green+"[%s] Finalización confirmada.\n"+Reset, s.entityID)
	go s.ForzarCierre()

	return &pb.ConfirmacionFin{Confirmacionbd: true}, nil
}

/*
Nombre: main
Parámetros: ninguno
Retorno: ninguno
Descripción: Inicia el nodo DB, se registra en el broker y sirve mediante gRPC.
*/
func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	dbServer := NewDBNodeServer(*dbNodeID)

	conn, err := grpc.Dial(direccionBroker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("No se logró conexión con broker: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	emClient := pb.NewRegistroEntidadesClient(conn)
	RegistrarConBroker(emClient, dbServer)

	fmt.Printf("[%s] Escuchando en %s...\n", *dbNodeID, *dbNodePort)
	lis, err := net.Listen("tcp", *dbNodePort)
	if err != nil {
		fmt.Printf("Listen %s falló: %v\n", *dbNodePort, err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	dbServer.grpcServer = s
	pb.RegisterNodoDBServer(s, dbServer)
	pb.RegisterFinalizacionServer(s, dbServer)
	go dbServer.IniciarDaemonDeFallos()

	if err := s.Serve(lis); err != nil {
		fmt.Printf("Serve falló: %v\n", err)
		os.Exit(1)
	}
}
