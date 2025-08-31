package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	pb "lab1/proto"

	"github.com/streadway/amqp"
	"google.golang.org/grpc"
)

var rechazos int = 0

type server struct {
	pb.UnimplementedOfertaServer
	pb.UnimplementedMonitoreoServer

	mu      sync.Mutex
	running map[string]chan struct{} // personaje -> stopChan
}

// ------------------ FASE 1 ------------------

func (s *server) EntregarOferta(ctx context.Context, req *pb.SolicitudOferta) (*pb.OfertaDisponible, error) {
	fmt.Println("Solicitud recibida!")
	time.Sleep(time.Second)

	rand.Seed(time.Now().UnixNano())

	if rand.Float32() < 0.1 {
		fmt.Println("No hay ofertas disponibles")
		time.Sleep(time.Second)
		return &pb.OfertaDisponible{Disponible: false}, nil
	}

	botin := rand.Intn(20000)
	prob_franklin := rand.Float32()
	prob_trevor := rand.Float32()
	riesgo := rand.Float32()
	fmt.Println("Oferta enviada!")
	time.Sleep(time.Second)
	return &pb.OfertaDisponible{
		Disponible:           true,
		BotinInicial:         int32(botin),
		ProbabilidadFranklin: prob_franklin,
		ProbabilidadTrevor:   prob_trevor,
		RiesgoPolicial:       riesgo,
	}, nil
}

func (s *server) ConfirmarOferta(ctx context.Context, req *pb.Confirmacion) (*pb.AckConfirmacion, error) {
	if !req.GetAceptada() {
		fmt.Println("> :(")
		time.Sleep(time.Second)
		rechazos++
	} else {
		fmt.Println("Enterado de la aceptación de la oferta!")
		time.Sleep(time.Second)
		return &pb.AckConfirmacion{}, nil
	}
	if rechazos%3 == 0 || rechazos == 0 {
		fmt.Println("Deja de rechazar loco, ahora espera 10 segundos..")
		time.Sleep(10 * time.Second)
	}
	return &pb.AckConfirmacion{}, nil
}

// ------------------ MONITOREO (FASE 3) ------------------

type estrellasMsg struct{ Estrellas int `json:"estrellas"` }

func (s *server) IniciarNotificaciones(ctx context.Context, req *pb.ObjetivoNotificacion) (*pb.AckMonitoreo, error) {
	personaje := normalize(req.GetPersonaje())
	if personaje != "franklin" && personaje != "trevor" {
		return &pb.AckMonitoreo{Ok: false, Detalle: "personaje inválido"}, nil
	}

	s.mu.Lock()
	if _, ok := s.running[personaje]; ok {
		s.mu.Unlock()
		return &pb.AckMonitoreo{Ok: true, Detalle: "ya estaba notificando"}, nil
	}
	stop := make(chan struct{})
	if s.running == nil {
		s.running = make(map[string]chan struct{})
	}
	s.running[personaje] = stop
	s.mu.Unlock()

	go s.publishStars(personaje, req.GetRiesgoPolicial(), stop)

	fmt.Printf("[Lester] ▶️ IniciarNotificaciones -> %s (riesgo=%.2f)\n", personaje, req.GetRiesgoPolicial())
	return &pb.AckMonitoreo{Ok: true, Detalle: "notificaciones iniciadas"}, nil
}

func (s *server) DetenerNotificaciones(ctx context.Context, req *pb.ObjetivoNotificacion) (*pb.AckMonitoreo, error) {
	personaje := normalize(req.GetPersonaje())
	s.mu.Lock()
	stop, ok := s.running[personaje]
	if ok {
		close(stop)
		delete(s.running, personaje)
	}
	s.mu.Unlock()
	if ok {
		fmt.Printf("[Lester] ⏹️ DetenerNotificaciones -> %s\n", personaje)
	}
	return &pb.AckMonitoreo{Ok: ok, Detalle: "detenido"}, nil
}

func (s *server) publishStars(personaje string, riesgo float32, stop <-chan struct{}) {
	rabbitURL := os.Getenv("RABBIT_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@rabbitmq:5672/"
	}

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Printf("[Lester] RabbitMQ error: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("[Lester] Channel error: %v", err)
		return
	}
	defer ch.Close()

	queueName := "estrellas." + personaje
	if _, err := ch.QueueDeclare(queueName, false, false, false, false, nil); err != nil {
		log.Printf("[Lester] QueueDeclare(%s): %v", queueName, err)
		return
	}

	estrellas := 0
	// más riesgo => notifica más rápido (2s..10s aprox)
	paso := int(math.Max(20, float64(100 - int(riesgo*100))))
	intervalo := time.Duration(paso) * 100 * time.Millisecond


	ticker := time.NewTicker(intervalo)
	defer ticker.Stop()

	fmt.Printf("[Lester] 📡 Publicando estrellas para %s cada %v (inicio en 0)\n", personaje, intervalo)
	s.publishOne(ch, queueName, estrellas) // envía 0 de inmediato
	for {
		select {
		case <-stop:
			fmt.Printf("[Lester] 📴 Fin publicaciones para %s\n", personaje)
			return
		case <-ticker.C:
			if estrellas < 7 {
				estrellas++
			}
			s.publishOne(ch, queueName, estrellas)
		}
	}
}

func (s *server) publishOne(ch *amqp.Channel, q string, k int) {
	body, _ := json.Marshal(estrellasMsg{Estrellas: k})
	_ = ch.Publish("", q, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	fmt.Printf("[Lester] ⭐ Notificación -> %s: estrellas=%d\n", q, k)
}

func normalize(s string) string {
	if len(s) == 0 {
		return s
	}
	if s[0] >= 'A' && s[0] <= 'Z' {
		s = string(s[0]+'a'-'A') + s[1:]
	}
	return s
}

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	s := grpc.NewServer()
	srv := &server{running: make(map[string]chan struct{})}

	// 👉 registra el MISMO objeto en ambos servicios
	pb.RegisterOfertaServer(s, srv)
	pb.RegisterMonitoreoServer(s, srv)

	fmt.Println("\nLester en linea")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
