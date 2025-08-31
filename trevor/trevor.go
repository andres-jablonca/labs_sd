package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	pb "lab1/proto"

	"github.com/streadway/amqp"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedSegundaFaseServer
	pb.UnimplementedTerceraFaseServer
}

func (s *server) IniciarGolpe(ctx context.Context, req *pb.OrdenGolpe) (*pb.AckInicio, error) {
	fmt.Printf("[Trevor] 🔥 IniciarGolpe() recibido: p=%.2f, riesgo=%.2f, botin=%d\n",
		req.GetProbabilidadExito(), req.GetRiesgoPolicial(), req.GetBotinInicial())

	go runGolpe("Trevor", int(req.GetBotinInicial()), req.GetProbabilidadExito())
	return &pb.AckInicio{Ok: true, Detalle: "Trevor inició el golpe"}, nil
}

func (s *server) InformarEstadoSegundaFase(ctx context.Context, inf *pb.InformarTrabajo) (*pb.Resultado, error) {
	fmt.Printf("Solicitud recibida!\n")

	rand.Seed(time.Now().UnixNano())
	probabilidad_exito := inf.GetProbabilidadExito()
	var probabilidad_fallar float32

	turnos_necesarios := 200 - int(probabilidad_exito*100)
	mitad_turnos := turnos_necesarios / 2
	exito := true
	time.Sleep(80 * time.Millisecond)
	for i := range turnos_necesarios {
		if i != mitad_turnos {
			fmt.Printf("Trevor trabajando... (%d turnos restantes)\n", turnos_necesarios-i)
			time.Sleep(500 * time.Millisecond)
		} else {
			probabilidad_fallar = rand.Float32()
			if probabilidad_fallar < 0.1 {
				fmt.Println("La mision fracasó debido a que Trevor se emborrachó y fue descubierto")
				exito = false
				break
			}
		}
	}
	if exito {
		fmt.Println("Distraccion completada!")
	}
	return &pb.Resultado{Exito: exito}, nil
}

type estrellasMsg struct{ Estrellas int `json:"estrellas"` }

func runGolpe(nombre string, botin int, pex float32) {
	// 1) Conectar Rabbit y consumir cola propia
	rabbitURL := os.Getenv("RABBIT_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@rabbitmq:5672/"
	}
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Println(err)
		return
	}
	defer ch.Close()

	qName := "estrellas.trevor"
	_, _ = ch.QueueDeclare(qName, false, false, false, false, nil)
	msgs, err := ch.Consume(qName, "", true, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	estrellas := 0
	turnos := 200 - int(pex*100) // del enunciado
	if turnos < 1 {
		turnos = 1
	}

	fmt.Printf("[%s] 🎬 IniciarTrabajoDeGolpe(): turnos=%d, botin=%d\n", nombre, turnos, botin)
	limiteFracaso := 5 // Trevor sube a 7 cuando llegue a 5 (furia)

	turnoTicker := time.NewTicker(500 * time.Millisecond)
	defer turnoTicker.Stop()

	// lector asíncrono
	estrellaCh := make(chan int, 1)
	go func() {
		for d := range msgs {
			var m estrellasMsg
			if json.Unmarshal(d.Body, &m) == nil {
				select {
				case estrellaCh <- m.Estrellas:
				default:
					<-estrellaCh
					estrellaCh <- m.Estrellas
				}
				fmt.Printf("[%s] ⭐ Lester notifica: estrellas=%d\n", nombre, m.Estrellas)
			}
		}
	}()

	for t := 1; t <= turnos; t++ {
		select {
		case e := <-estrellaCh:
			estrellas = e
			// Habilidad Trevor: al llegar a 5 activa furia → límite pasa a 7
			if nombre == "Trevor" && estrellas >= 5 && limiteFracaso == 5 {
				limiteFracaso = 7
				fmt.Printf("[Trevor] 💢 FURIA ACTIVADA: nuevo límite de fracaso = 7\n")
			}
		case <-turnoTicker.C:
			// avanza el turno
		}
		fmt.Printf("[%s] 🕒 Turno %d/%d | estrellas=%d (límite=%d)\n", nombre, t, turnos, estrellas, limiteFracaso)

		if estrellas >= limiteFracaso {
			fmt.Printf("[%s] ❌ FRACASO POR ESTRELLAS (=%d)\n", nombre, estrellas)
			return
		}
	}
	fmt.Printf("[%s] ✅ GOLPE COMPLETADO (botín base=%d)\n", nombre, botin)
}

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterSegundaFaseServer(s, &server{})
	pb.RegisterTerceraFaseServer(s, &server{})

	log.Printf("\nTrevor en linea\n")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
