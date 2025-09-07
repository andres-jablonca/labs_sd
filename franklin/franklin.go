package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	pb "franklin/proto"

	"github.com/streadway/amqp"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedMichaelTrevorFranklinServer
	estrellasActuales int
}

func (s *server) InformarDistraccion(ctx context.Context, inf *pb.InfoDistraccion) (*pb.ResultadoDistraccion, error) {
	fmt.Printf("Confirmo inicio de fase 2!\n")

	rand.Seed(time.Now().UnixNano())
	probabilidad_exito := inf.GetProbabilidadExito()
	var probabilidad_fallar float32

	turnos_necesarios := 200 - int(probabilidad_exito*100)
	mitad_turnos := turnos_necesarios / 2
	exito := true
	time.Sleep(80 * time.Millisecond)

	for i := 0; i < turnos_necesarios; i++ {
		if i != mitad_turnos {
			fmt.Printf("Trabajando... (%d turnos restantes)\n", turnos_necesarios-i)
			time.Sleep(500 * time.Millisecond)
		} else {
			probabilidad_fallar = rand.Float32()
			if probabilidad_fallar < 0.1 {
				fmt.Println("La mision fracasó debido a que Chop se puso a ladrar y Franklin se distrajo")
				exito = false
				break
			}
		}
	}
	if exito {
		fmt.Println("Distracción completada, procedan!")
		return &pb.ResultadoDistraccion{Exito: exito}, nil
	} else {
		return &pb.ResultadoDistraccion{Exito: exito, Motivo: "Chop se puso a ladrar y Franklin se distrajo"}, nil
	}
}

func (s *server) InformarGolpe(ctx context.Context, inf *pb.InfoGolpe) (*pb.ResultadoGolpe, error) {
	fmt.Printf("Confirmo inicio de fase 3!\n")
	time.Sleep(time.Second)

	go s.consumirEstrellas()

	rand.Seed(time.Now().UnixNano())
	probabilidad_exito := inf.GetProbabilidadExito()
	turnos_necesarios := 200 - int(probabilidad_exito*100)
	exito := true
	motivo := ""
	extra := 0
	chop := false
	for i := range turnos_necesarios {
		if i%5 == 0 {
			fmt.Printf("[*] CONSULTANDO ESTRELLAS... %d ESTRELLAS DETECTADAS!\n", s.estrellasActuales)

			if s.estrellasActuales >= 3 && !chop { // Verificar si se puede activar la habilidad
				fmt.Printf("================\nHabilidad activada: Chop recolectando botin extra!\n================\n")
				chop = true
			} else if s.estrellasActuales >= 5 { // Verificar si se perdió
				exito = false
				fmt.Printf("%d estrellas?!?!\n", s.estrellasActuales)
				motivo = "Limite de estrellas alcanzado."
				break
			}
		}

		// Trabajar turnos y recolectar botin extra si habilidad está activa
		fmt.Printf("Trabajando... (%d turnos restantes)\n", turnos_necesarios-i)
		if chop {
			fmt.Printf("+ $1000 obtenidos por Chop\n")
			extra += 1000
		}

		// Asegurarse de revisar las estrellas un turno antes de terminar la misión
		if s.estrellasActuales >= 5 && i == turnos_necesarios-1 {
			exito = false
			fmt.Printf("%d estrellas?!?!\n", s.estrellasActuales)
			motivo = "Limite de estrellas alcanzado."
			break
		}

		time.Sleep(500 * time.Millisecond)

	}

	// Verificar que no se hubieran llegado a las 5 estrellas una vez terminados los turnos
	// if s.estrellasActuales >= 5 {
	// 	fmt.Printf("Límite de estrellas alcanzado\n")
	//	exito = false
	// }

	if exito {
		fmt.Println("La fase 3 fue todo un éxito Michael!")
		return &pb.ResultadoGolpe{
			Exito:          exito,
			Botin:          inf.GetBotin(),
			BotinExtra:     int32(extra),
			EstrellasFinal: int32(s.estrellasActuales), // Borrable igual
		}, nil
	} else {
		fmt.Printf("Fase 3 fracasó, lo siento...\nMotivo: %s\n", motivo)
		return &pb.ResultadoGolpe{
			Exito:          exito,
			Botin:          inf.GetBotin(),
			Motivo:         motivo,
			BotinExtra:     int32(extra), // Botin extra perdido
			EstrellasFinal: int32(s.estrellasActuales),
		}, nil
	}
}

func (s *server) consumirEstrellas() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		fmt.Printf("No se pudo conectar a RabbitMQ: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("No se pudo abrir canal RabbitMQ: %v", err)
		return
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"estrellas_Franklin",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Error declarando cola: %v", err)
		return
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Error consumiendo mensajes: %v", err)
		return
	}

	fmt.Println("Estaré atento a tus notificaciones, Lester")

	for msg := range msgs {
		parts := strings.Split(string(msg.Body), ":")
		if len(parts) == 2 && parts[0] == "Franklin" {
			estrellas, err := strconv.Atoi(parts[1])
			if err == nil {
				s.estrellasActuales = estrellas
			}
		}
	}
}

func (s *server) PagarMiembro(ctx context.Context, req *pb.MontoPagoMiembro) (*pb.ConfirmarPagoMiembro, error) {
	check := false
	msj := "Esto no es lo que acordamos.."
	correspondencias := int32(req.GetTotal() / 4)
	if correspondencias == req.GetCorrespondencia() {
		check = true
		msj = "Excelente Michael! El pago es correcto."
	}

	fmt.Printf("Total: %d, Recibido: %d, Esperado: %d \n", req.Total, req.Correspondencia, correspondencias)
	fmt.Println(msj)
	return &pb.ConfirmarPagoMiembro{Correcto: check, Mensaje: msj}, nil
}

func main() {

	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMichaelTrevorFranklinServer(s, &server{})

	fmt.Printf("\nFranklin en linea!\n")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
