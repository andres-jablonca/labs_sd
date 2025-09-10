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

type OfertaCSV struct {
	BotinInicial      int32
	ProbExitoFranklin int32
	ProbExitoTrevor   int32
	RiesgoPolicial    int32
}

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

func isEnviandoNotifs() bool {
	return enviandoNotifs
}

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

func (s *server) IniciarNotificacionesEstrellas(ctx context.Context, req *pb.InicioNotifEstrellas) (*pb.AckInicioNotif, error) {
	fmt.Printf("Entendido Michael..\n")
	if !isEnviandoNotifs() {
		enviandoNotifs = false
	}
	go enviarNotificacionesEstrellas(req.GetPersonaje(), req.GetRiesgoPolicial())
	return &pb.AckInicioNotif{}, nil
}

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
	time.Sleep(2 * time.Second)
	fmt.Printf("------\nTotal: %d\nEsperado: %d\nRecibido: %d\n\n", total, pagoEsperadoLester, pagoRecibido)
	time.Sleep(2 * time.Second)
	fmt.Println(msj)
	if check {
		fmt.Printf("Llamenme si necesitan mi ayuda en otra ocasión...\n------")
	}

	return &pb.ConfirmarPagoLester{Correcto: check, Mensaje: msj}, nil
}

func conectarRabbitMQ() {
	var err error
	for i := range 15 {
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

	fmt.Printf("\nLester en linea!\n\n")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
