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

func OfertaAleatoria(filePath string) (*OfertaCSV, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var validRows []OfertaCSV

	// Saltar la primera línea
	if scanner.Scan() {
	}

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ",")

		// Asegurar que tenga al menos 4 columnas
		if len(fields) < 4 {
			continue
		}

		// Verificar que al menos uno de los primeros 4 no esté vacío
		valid := false
		for i := 0; i < 4; i++ {
			if strings.TrimSpace(fields[i]) != "" {
				valid = true
				break
			}
		}

		if valid {
			// Convertir a int32
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
			validRows = append(validRows, oferta)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if len(validRows) == 0 {
		return nil, fmt.Errorf("no se encontraron filas válidas")
	}

	// Selección aleatoria
	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(validRows))
	return &validRows[randomIndex], nil
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
	fmt.Printf("Solicitud recibida!\n")
	time.Sleep(time.Second)

	rand.Seed(time.Now().UnixNano())

	prob_aceptar := rand.Float32()

	if prob_aceptar < 0.1 {
		fmt.Printf("No hay ofertas disponibles\n")
		time.Sleep(time.Second)
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
		fmt.Printf("Oferta enviada!\n")
		time.Sleep(time.Second)
		return &pb.OfertaDisponible{Disponible: true, BotinInicial: botin, ProbabilidadFranklin: float32(prob_franklin) / 100.0, ProbabilidadTrevor: float32(prob_trevor) / 100.0, RiesgoPolicial: float32(riesgo) / 100.0}, nil
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
	if !isEnviandoNotifs() {
		enviandoNotifs = false
	}
	go enviarNotificacionesEstrellas(req.GetPersonaje(), req.GetRiesgoPolicial())
	return &pb.AckInicioNotif{}, nil
}

func (s *server) DetenerNotificacionesEstrellas(ctx context.Context, req *pb.DetenerNotifEstrellas) (*pb.AckDetenerNotif, error) {
	fmt.Printf("Deteniendo notificaciones de estrellas para %s\n", req.GetPersonaje())
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
			time.Sleep(520 * time.Millisecond)

			if turno%frecuenciaTurnos == 0 {
				estrellas++
				estre++
				if estrellas >= 5 {
					fmt.Printf("Turno %d: Estrellas: 5, ten cuidado!\n", turno)
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
