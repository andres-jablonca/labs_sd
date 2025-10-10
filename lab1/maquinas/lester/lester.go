package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	pb "lester/proto"

	"github.com/streadway/amqp"
	"google.golang.org/grpc"
)

// Struct para almacenar el formato de las ofertas
type OfertaCSV struct {
	BotinInicial      int32
	ProbExitoFranklin int32
	ProbExitoTrevor   int32
	RiesgoPolicial    int32
}

/*
OfertaAleatoria()
***
Parametros:

	nombre_archivo : string

***
Retorno:

	(*OfertaCSV, error)

***
Lee un CSV de ofertas, filtra las filas válidas y retorna una oferta aleatoria con el formato del struct OfertaCSV.
***
*/
func OfertaAleatoria(nombre_archivo string) (*OfertaCSV, error) {
	file, err := os.Open(nombre_archivo)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var ofertas []OfertaCSV

	if scanner.Scan() {
	}

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ",")

		filas_validas := false
		for i := range 4 {
			if strings.TrimSpace(fields[i]) != "" {
				filas_validas = true
				break
			}
		}

		if filas_validas {
			botin, _ := strconv.Atoi(strings.TrimSpace(fields[0]))
			franklin, _ := strconv.Atoi(strings.TrimSpace(fields[1]))
			trevor, _ := strconv.Atoi(strings.TrimSpace(fields[2]))
			riesgo, _ := strconv.Atoi(strings.TrimSpace(fields[3]))

			oferta := OfertaCSV{
				BotinInicial:      int32(botin),
				ProbExitoFranklin: int32(franklin),
				ProbExitoTrevor:   int32(trevor),
				RiesgoPolicial:    int32(riesgo),
			}
			ofertas = append(ofertas, oferta)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if len(ofertas) == 0 {
		return nil, fmt.Errorf("no se encontraron filas válidas")
	}

	rand.Seed(time.Now().UnixNano())
	random_index := rand.Intn(len(ofertas))
	return &ofertas[random_index], nil
}

// Globales
var (
	rechazos           int = 0
	enviandoNotifs         = false
	stopNotificaciones     = make(chan bool)
	connRabbitMQ       *amqp.Connection
	chRabbitMQ         *amqp.Channel
)

type server struct {
	pb.UnimplementedMichaelLesterServer
}

/*
isEnviandoNotifs()
***
Parametros:

	(none)

***
Retorno:

	bool

***
Indica si actualmente se están enviando notificaciones de estrellas. Devuelve true si están activas, false en caso contrario.
***
*/
func isEnviandoNotifs() bool {
	return enviandoNotifs
}

/*
EntregarOferta()
***
Parametros:

	ctx : context.Context
	req : *pb.SolicitudOferta (mensaje enviado por Michael)

***
Retorno:

	(*pb.OfertaDisponible, error) (respuesta a ser enviada a Michael)

***
Recibe la solicitud de oferta de Michael. Con probabilidad del 10% responde que
no hay oferta y si hay, lee una oferta aleatoria desde el CSV y la retorna
normalizada (probabilidades y riesgo como floats).
***
*/
func (s *server) EntregarOferta(ctx context.Context, req *pb.SolicitudOferta) (*pb.OfertaDisponible, error) {
	fmt.Printf("------\nSolicitud recibida!\n\n")
	time.Sleep(2 * time.Second)

	rand.Seed(time.Now().UnixNano())

	prob_aceptar := rand.Float32()

	if prob_aceptar < 0.1 {
		fmt.Printf("No tengo ofertas disponibles en este momento...\n")
		time.Sleep(2 * time.Second)
		return &pb.OfertaDisponible{Disponible: false}, nil
	} else {

		oferta, err := OfertaAleatoria("ofertas_grande.csv")
		if err != nil {
			log.Fatal(err)
		}
		botin := oferta.BotinInicial
		prob_franklin := oferta.ProbExitoFranklin
		prob_trevor := oferta.ProbExitoTrevor
		riesgo := oferta.RiesgoPolicial
		fmt.Printf("Oferta enviada!\n\n")
		time.Sleep(2 * time.Second)
		return &pb.OfertaDisponible{Disponible: true, BotinInicial: botin, ProbabilidadFranklin: float32(prob_franklin) / 100.0, ProbabilidadTrevor: float32(prob_trevor) / 100.0, RiesgoPolicial: float32(riesgo) / 100.0}, nil
	}
}

/*
ConfirmarOferta()
***
Parametros:

	ctx : context.Context
	req : *pb.ConfirmacionOferta (Mensaje enviado por Michael)

***
Retorno:

	(*pb.AckConfirmacionOferta, error) (Ack para ser enviado a Michael)

***
Confirma aceptación o rechazo de la oferta. Si se rechaza, incrementa el
contador y cada 3 rechazos hace una espera 10 segundos. Retorna Ack.
***
*/
func (s *server) ConfirmarOferta(ctx context.Context, req *pb.ConfirmacionOferta) (*pb.AckConfirmacionOferta, error) {
	confir := req.GetAceptada()
	if !confir {
		fmt.Printf(">:(\n")
		time.Sleep(2 * time.Second)
		rechazos++
	} else {
		fmt.Printf("Enterado de la aceptación de la oferta!\n")
		fmt.Printf("------\n")
		time.Sleep(2 * time.Second)
		return &pb.AckConfirmacionOferta{}, nil
	}
	if rechazos%3 == 0 && rechazos != 0 {
		fmt.Printf("Deja de rechazar loco, ahora espera 10 segundos..\n")
		time.Sleep(10 * time.Second)
	}

	return &pb.AckConfirmacionOferta{}, nil
}

/*
IniciarNotificacionesEstrellas()
***
Parametros:

	ctx : context.Context
	req : *pb.InicioNotifEstrellas (Mensaje enviado por Michael)

***
Retorno:

	(*pb.AckInicioNotif, error) (Ack para ser enviado a Michael)

***
Inicia el envío periódico de notificaciones de estrellas para el personaje
indicado con frecuencia en función del riesgo policial. Retorna Ack.
***
*/
func (s *server) IniciarNotificacionesEstrellas(ctx context.Context, req *pb.InicioNotifEstrellas) (*pb.AckInicioNotif, error) {
	fmt.Printf("Entendido Michael..\n")
	if !isEnviandoNotifs() {
		enviandoNotifs = false
	}
	// Go routine para el envio de notificaciones periodicas y no mantener en espera a Michael
	go enviarNotificacionesEstrellas(req.GetPersonaje(), req.GetRiesgoPolicial())
	return &pb.AckInicioNotif{}, nil
}

/*
DetenerNotificacionesEstrellas
***
Parametros:

	ctx : context.Context
	req : *pb.DetenerNotifEstrellas (mensaje enviado por Michael)

***
Retorno:

	(*pb.AckDetenerNotif, error) (respuesta a enviar a Michael)

***
Michael informa para detener el envío de notificaciones para el personaje indicado. Registra si la misión tuvo éxito o no y detiene el canal mediante una señal. Retorna un Ack.
***
*/
func (s *server) DetenerNotificacionesEstrellas(ctx context.Context, req *pb.DetenerNotifEstrellas) (*pb.AckDetenerNotif, error) {
	if !req.GetExito() {
		fmt.Printf("\nYa veo...\n\n")
	} else {
		fmt.Printf("\nExcelente!\n\n")
	}
	if isEnviandoNotifs() {
		enviandoNotifs = false
	}

	stopNotificaciones <- true
	return &pb.AckDetenerNotif{}, nil
}

/*
EnviarNotificacionesEstrella()
***
Parametros:

	personaje : string (Franklin o Trevor)
	riesgoPolicial : float32

***
Retorno:

	None

***
Simula los turnos trabajados por el personaje y envía el número de estrellas por RabbitMQ a la cola "estrellas_{Personaje}" cada cierto número de turnos. Se detiene al recibir señal por canal.
***
*/
func enviarNotificacionesEstrellas(personaje string, riesgoPolicial float32) {
	frecuenciaTurnos := 100 - int(riesgoPolicial*100)
	if frecuenciaTurnos <= 0 {
		frecuenciaTurnos = 1
	}

	estrellas := 0
	turno := 0
	time.Sleep(4*time.Second + 50*time.Millisecond)
	fmt.Printf("\nEstaré notificando las estrellas a %s cada %d turnos\n\n", personaje, frecuenciaTurnos)
	estre := -1
	for {
		select {
		case <-stopNotificaciones:
			fmt.Printf("Deteniendo envio de notificaciones...\n")
			return
		default:
			turno++
			time.Sleep(500 * time.Millisecond)

			if turno%frecuenciaTurnos == 0 {
				estrellas++
				estre++
				if estrellas >= 5 && personaje == "Franklin" {
					fmt.Printf("Turno %d: Estrellas: 5, ten cuidado!\n", turno)
				} else if estrellas >= 7 && personaje == "Trevor" {
					fmt.Printf("Turno %d: Estrellas: 7, ten cuidado!\n", turno)
				}
				fmt.Printf("Turno %d: Aumento de estrellas: %d -> %d, ten cuidado!\n", turno, estre, estrellas)

				err := enviarMensajeRabbitMQ(personaje, estrellas)
				if err != nil {
					fmt.Printf("Error enviando mensaje a RabbitMQ: %v\n", err)
				}
			}

		}
	}
}

/*
enviarMensajeRabbitMQ()
***
Parametros:

	personaje : string
	estrellas : int

***
Retorno:

	error

***
Publica mensaje a la cola de RabbitMQ llamada "estrellas_{Personaje}". Retorna error si el canal no está inicializado o si falla la publicación.
***
*/
func enviarMensajeRabbitMQ(personaje string, estrellas int) error {
	if chRabbitMQ == nil {
		return fmt.Errorf("canal de RabbitMQ no inicializado")
	}

	mensaje := fmt.Sprintf("%s:%d", personaje, estrellas)
	err := chRabbitMQ.Publish(
		"",
		"estrellas_"+personaje,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(mensaje),
		})
	return err
}

/*
PagarLester()
***
Parametros:

	ctx : context.Context
	req : *pb.MontoPago (Mensaje enviado por Michael)

***
Retorno:

	(*pb.ConfirmarPagoLester, error) (Respuesta a ser enviada a Michael)

***
Valida el pago entregado por Michael. Comprueba que lo recibido sea una cuarta parte del total + el dinero sobrante. Retorna si el pago es correcto y un mensaje a ser incluido en el reporte final.
***
*/
func (s *server) PagarLester(ctx context.Context, req *pb.MontoPago) (*pb.ConfirmarPagoLester, error) {
	total := req.GetTotal()
	pagoRecibido := req.GetCorrespondencia()

	pagoEsperadoPorPersona := total / 4
	resto := total % 4

	pagoEsperadoLester := pagoEsperadoPorPersona
	if resto > 0 {
		pagoEsperadoLester += resto
	}

	check := false
	msj := "Esto no es lo que acordamos.."

	if pagoRecibido == pagoEsperadoLester {
		check = true
		msj = "Un placer hacer negocios."
	}
	time.Sleep(2 * time.Second)
	fmt.Printf("------\nTotal: %d\nEsperado: %d\nRecibido: %d\n\n", total, pagoEsperadoLester, pagoRecibido)
	time.Sleep(2 * time.Second)
	fmt.Println(msj)
	if check {
		fmt.Printf("Llamenme si necesitan mi ayuda en otra ocasión...\n------")
	}

	return &pb.ConfirmarPagoLester{Correcto: check, Mensaje: msj}, nil
}

/*
conectarRabbitMQ()
***
Parametros:

	(none)

***
Retorno:

	None

***
Intenta conectar a RabbitMQ con reintentos (hasta 15). Si logra conexión,
abre un canal global para publicaciones. Finaliza el programa si no logra
conectar tras los intentos.
***
*/
func conectarRabbitMQ() {
	var err error
	for i := range 15 {
		connRabbitMQ, err = amqp.Dial("amqp://gta:gta123@10.35.168.43:5672/")
		if err == nil {
			chRabbitMQ, err = connRabbitMQ.Channel()
			if err == nil {
				fmt.Println("Conectado a RabbitMQ")
				return
			}
		}
		fmt.Printf("Intentando conectar con RabbitMQ... (%d/15)\n", i+1)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("No se pudo conectar a RabbitMQ: %v", err)
}

/*
***
(none)
***
None
***
Punto de entrada del servicio de Lester. Conecta a RabbitMQ, abre el
listener TCP en :50051, registra el servicio gRPC y queda atendiendo
solicitudes entrantes.
***
*/
func main() {

	// Conectar a RabbitMQ primero
	conectarRabbitMQ()
	defer connRabbitMQ.Close()
	defer chRabbitMQ.Close()

	// Inicializar servidor para escuchar en el puerto 50051
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	// Conexion gRPC para la comunicacion con Michael
	s := grpc.NewServer()
	pb.RegisterMichaelLesterServer(s, &server{})

	fmt.Printf("\nLester en linea!\n\n")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
