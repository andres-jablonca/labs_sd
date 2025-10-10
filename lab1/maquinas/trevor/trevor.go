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

	pb "trevor/proto"

	"github.com/streadway/amqp"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedMichaelTrevorFranklinServer
	estrellasActuales int
}

/*
InformarDistraccion()
***
Parametros:

	ctx : context.Context
	inf : *pb.InfoDistraccion (mensaje enviado por Michael)

***
Retorno:

	(*pb.ResultadoDistraccion, error) (respuesta a ser enviada a Michael)

***
Confirma e inicia la fase 2 (distracción) y simula el avance por turnos.
En la mitad de los turnos puede fallar con probabilidad del 10%. Devuelve si la
distracción fue exitosa o no, y en caso de fracaso se incluye el motivo.
*/
func (s *server) InformarDistraccion(ctx context.Context, inf *pb.InfoDistraccion) (*pb.ResultadoDistraccion, error) {
	fmt.Printf("\nConfirmo inicio de fase 2!\n\n")

	rand.Seed(time.Now().UnixNano())
	probabilidad_exito := inf.GetProbabilidadExito()
	var probabilidad_fallar float32

	turnos_necesarios := 200 - int(probabilidad_exito*100)
	mitad_turnos := turnos_necesarios / 2
	exito := true
	time.Sleep(80 * time.Millisecond)

	for i := range turnos_necesarios {

		fmt.Printf("Trabajando... (%d turnos restantes)\n", turnos_necesarios-i)
		time.Sleep(500 * time.Millisecond)

		if i == mitad_turnos {

			probabilidad_fallar = rand.Float32()
			if probabilidad_fallar < 0.1 {
				fmt.Println("La mision fracasó debido a que Trevor se emborrachó...")
				exito = false
				break
			} else {
				fmt.Printf("Informando a Michael que la misión va de maravilla...\n")
			}
		}
	}
	if exito {
		fmt.Println("\nDistracción completada, procedan!\n----------")
		return &pb.ResultadoDistraccion{Exito: exito}, nil
	} else {
		return &pb.ResultadoDistraccion{Exito: exito, Motivo: "Trevor se emborrachó"}, nil
	}
}

/*
InformarGolpe()
***
Parametros:

	ctx : context.Context
	inf : *pb.InfoGolpe (Mensaje por parte de Michael)

***
Retorno:

	(*pb.ResultadoGolpe, error) (respuesta a ser enviada a Michael)

***
Confirma e inicia la fase 3 (golpe). Lanza un consumidor de RabbitMQ para
recibir el número de estrellas en tiempo real las cuales son notificadas por Lester. En caso de 5 estrellas se activa su habilidad en donde aumenta su limite hasta 7 estrellas y en caso de 7 estrellas la mision fracasa (casos mas especificos explicados en readme). En caso de éxito retorna el botín base y las estrellas al finalizar y en caso de fracaso se incluye también el motivo.
***
*/
func (s *server) InformarGolpe(ctx context.Context, inf *pb.InfoGolpe) (*pb.ResultadoGolpe, error) {
	fmt.Printf("\nConfirmo inicio de fase 3!\n\n")
	time.Sleep(2 * time.Second)

	// Go routine para verificar estrellas a la vez que se trabaja
	go s.consumirEstrellas()

	rand.Seed(time.Now().UnixNano())
	probabilidad_exito := inf.GetProbabilidadExito()
	turnos_necesarios := 200 - int(probabilidad_exito*100)
	exito := true
	motivo := ""
	trevor := false
	estr := 0
	for i := range turnos_necesarios {

		if i%5 == 0 {
			fmt.Printf("[*] CONSULTANDO ESTRELLAS... %d ESTRELLAS DETECTADAS!\n", s.estrellasActuales)

			if s.estrellasActuales >= 5 && !trevor { // Verificar si se puede activar habilidad
				fmt.Printf("================\nHabilidad activada: Limite de estrellas aumentado!\n================\n")
				trevor = true
			} else if s.estrellasActuales >= 7 { // Verificar si se perdió
				exito = false
				fmt.Printf("\n%d ESTRELLAS?!?!\n\n", s.estrellasActuales)
				motivo = "Limite de estrellas alcanzado."
				break
			}
		}

		// Fijar estrellas en el ultimo turno para evitar caso en que esta cambie justo al instante luego de ser chequeada
		if i == turnos_necesarios-1 {
			estr = s.estrellasActuales
		}

		// Trabajar turnos
		if !trevor {
			fmt.Printf("Trabajando... (%d turnos restantes)\n", turnos_necesarios-i)
		} else {
			fmt.Printf("Trabajando con furia! (%d turnos restantes)\n", turnos_necesarios-i)
		}

		time.Sleep(500 * time.Millisecond)

	}

	// Asegurarse de revisar las estrellas y la habilidad al terminar la misión

	if estr >= 5 && !trevor { // Se terminaron los turnos, pero Trevor no se dio cuenta de que llego a 5 estrellas, por lo que no activo su habilidad -> fracaso
		exito = false
		fmt.Printf("\n%d ESTRELLAS?!?!\n\n", estr)
		motivo = "Limite de estrellas alcanzado (5), no alcanzó a activar su habilidad."
	} else if estr >= 7 { // Se activo habilidad, pero se llego a 7 estrellas -> fracaso
		exito = false
		fmt.Printf("\n%d ESTRELLAS?!?!\n\n", estr)
		motivo = "Limite de estrellas alcanzado (7)."
	}

	if exito {
		fmt.Printf("\nLa fase 3 fue todo un éxito Michael!\nEste fue el botín que conseguí: %d\n----------\n", inf.GetBotin())
		return &pb.ResultadoGolpe{
			Exito:          exito,
			Botin:          inf.GetBotin(),
			EstrellasFinal: int32(estr), // Borrable
		}, nil
	} else {
		fmt.Printf("Fase 3 fracasó, lo siento...\nMotivo: %s\n", motivo)
		return &pb.ResultadoGolpe{
			Exito:          exito,
			Botin:          inf.GetBotin(),
			Motivo:         motivo,
			EstrellasFinal: int32(estr),
		}, nil
	}
}

/*
ConsumirEstrellas()
***
Parametros:

	(none)

***
Retorno:

	None

***
Consume mensajes desde la cola de RabbitMQ "estrellas_Trevor" y actualiza
s.estrellasActuales cuando llega un mensaje. No retorna nada y mantiene el estado para ser consultado mientras dure la fase 3.
***
*/
func (s *server) consumirEstrellas() {

	// Conexion a la IP de Lester y puerto de RabbitMQ
	conn, err := amqp.Dial("amqp://gta:gta123@10.35.168.43:5672/")
	if err != nil {
		fmt.Printf("No se pudo conectar a RabbitMQ: %v", err)
		return
	}
	defer conn.Close()

	// Declarar canal
	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("No se pudo abrir canal RabbitMQ: %v", err)
		return
	}
	defer ch.Close()

	// Declarar cola
	q, err := ch.QueueDeclare(
		"estrellas_Trevor",
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

	// Consumir mensajes
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

	// Leer mensajes y actualizar estrellas
	for msg := range msgs {
		parts := strings.Split(string(msg.Body), ":")
		if len(parts) == 2 && parts[0] == "Trevor" {
			estrellas, err := strconv.Atoi(parts[1])
			if err == nil {
				s.estrellasActuales = estrellas
			}
		}
	}
}

/*
PagarMiembro()
***
Parametros:

	ctx : context.Context
	req : *pb.MontoPagoMiembro (mensaje enviado por Michael)

***
Retorno:

	(*pb.ConfirmarPagoMiembro, error) (respuesta a ser enviada a Michael)

***
Valida el pago entregado por Michael. Comprueba que lo recibido sea una cuarta parte del total. Retorna si el pago es correcto y un mensaje a ser incluido en el reporte final.
***
*/
func (s *server) PagarMiembro(ctx context.Context, req *pb.MontoPagoMiembro) (*pb.ConfirmarPagoMiembro, error) {
	check := false
	msj := "Esto no es lo que acordamos.."
	correspondencias := int32(req.GetTotal() / 4)
	if correspondencias == req.GetCorrespondencia() {
		check = true
		msj = "Justo lo que esperaba!"
	}
	time.Sleep(2 * time.Second)
	fmt.Printf("Total: %d\nEsperado: %d\nRecibido: %d\n\n", req.Total, correspondencias, req.Correspondencia)
	time.Sleep(2 * time.Second)
	fmt.Println(msj)
	if check {
		fmt.Printf("Un placer trabajar con ustedes muchachos!\n------")
	}
	return &pb.ConfirmarPagoMiembro{Correcto: check, Mensaje: msj}, nil
}

func main() {

	// Inicializar servidor para escuchar en el puerto 50051
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	// Conexion gRPC para la comunicacion con Michael
	s := grpc.NewServer()
	pb.RegisterMichaelTrevorFranklinServer(s, &server{})

	fmt.Printf("\nTrevor en linea\n\n")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
