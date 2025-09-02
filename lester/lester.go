package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	pb "lester/proto"

	"github.com/streadway/amqp"
	"google.golang.org/grpc"
)

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

func (s *server) EntregarOferta(ctx context.Context, req *pb.SolicitudOferta) (*pb.OfertaDisponible, error) {
	fmt.Printf("Solicitud recibida!\n")
	time.Sleep(time.Second)

	rand.Seed(time.Now().UnixNano())

	prob_aceptar := rand.Float32()

	if prob_aceptar < 0.1 {
		fmt.Printf("No hay ofertas disponibles\n")
		time.Sleep(time.Second)
		return &pb.OfertaDisponible{Disponible: false}, nil
	} else {
		botin := rand.Intn(20000) + 10000
		prob_franklin := rand.Float32()
		prob_trevor := rand.Float32()
		riesgo := rand.Float32()
		fmt.Printf("Oferta enviada!\n")
		time.Sleep(time.Second)
		return &pb.OfertaDisponible{Disponible: true, BotinInicial: int32(botin), ProbabilidadFranklin: prob_franklin, ProbabilidadTrevor: prob_trevor, RiesgoPolicial: riesgo}, nil
	}
}

func (s *server) ConfirmarOferta(ctx context.Context, req *pb.ConfirmacionOferta) (*pb.AckConfirmacionOferta, error) {
	confir := req.GetAceptada()
	if !confir {
		fmt.Printf(">:(\n")
		time.Sleep(time.Second)
		rechazos++
	} else {
		fmt.Printf("Enterado de la aceptación de la oferta!\n")
		time.Sleep(time.Second)
		return &pb.AckConfirmacionOferta{}, nil
	}
	if rechazos%3 == 0 && rechazos != 0 {
		fmt.Printf("Deja de rechazar loco, ahora espera 10 segundos..\n")
		time.Sleep(10 * time.Second)
	}
	return &pb.AckConfirmacionOferta{}, nil
}

func (s *server) IniciarNotificacionesEstrellas(ctx context.Context, req *pb.InicioNotifEstrellas) (*pb.AckInicioNotif, error) {
	fmt.Printf("Iniciando notificaciones de estrellas para %s\n", req.GetPersonaje())
	enviandoNotifs = true
	go enviarNotificacionesEstrellas(req.GetPersonaje(), req.GetRiesgoPolicial())
	return &pb.AckInicioNotif{}, nil
}

func (s *server) DetenerNotificacionesEstrellas(ctx context.Context, req *pb.DetenerNotifEstrellas) (*pb.AckDetenerNotif, error) {
	fmt.Printf("Deteniendo notificaciones de estrellas para %s\n", req.GetPersonaje())
	enviandoNotifs = false
	stopNotificaciones <- true
	return &pb.AckDetenerNotif{}, nil
}

func enviarNotificacionesEstrellas(personaje string, riesgoPolicial float32) {
	frecuenciaTurnos := 100 - int(riesgoPolicial*100)
	if frecuenciaTurnos <= 0 {
		frecuenciaTurnos = 1
	}

	estrellas := 0
	turno := 0
	time.Sleep(time.Second)
	fmt.Printf("Comenzando a enviar notificaciones para %s cada %d turnos\n", personaje, frecuenciaTurnos)
	estre := -1
	for {
		select {
		case <-stopNotificaciones:
			fmt.Printf("Deteniendo el envio de notificaciones\n")
			return
		default:
			turno++
			time.Sleep(500 * time.Millisecond)

			if turno%frecuenciaTurnos == 0 {
				estrellas++
				estre++
				if estrellas >= 5 {
					log.Printf("Turno %d: Estrellas: 5, ten cuidado!\n", turno)
				}
				log.Printf("Turno %d: Aumento de estrellas: %d -> %d, ten cuidado!\n", turno, estre, estrellas)

				err := enviarMensajeRabbitMQ(personaje, estrellas)
				if err != nil {
					fmt.Printf("Error enviando mensaje a RabbitMQ: %v\n", err)
				}
			}

		}
	}
}

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

	fmt.Printf("Total: %d, Recibido: %d, Esperado: %d \n", total, pagoRecibido, pagoEsperadoLester)
	fmt.Println(msj)

	return &pb.ConfirmarPagoLester{Correcto: check, Mensaje: msj}, nil
}

func conectarRabbitMQ() {
	var err error
	for i := 0; i < 15; i++ {
		connRabbitMQ, err = amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
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

func main() {
	// Conectar a RabbitMQ primero
	conectarRabbitMQ()
	defer connRabbitMQ.Close()
	defer chRabbitMQ.Close()

	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMichaelLesterServer(s, &server{})

	fmt.Printf("\nLester en linea!\n")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
