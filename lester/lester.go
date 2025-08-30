package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	pb "lab1/proto"

	"github.com/streadway/amqp"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedOfertaServer
	ch *amqp.Channel
	q  amqp.Queue
}

func (s *server) EntregarOferta(ctx context.Context, req *pb.SolicitudOferta) (*pb.OfertaDisponible, error) {

	fmt.Printf("Solicitud recibida!\n")
	time.Sleep(30 * time.Millisecond)

	rand.Seed(time.Now().UnixNano())

	prob_aceptar := rand.Float32()

	if req.GetRechazos()%3 == 0 && req.GetRechazos() != 0 {
		fmt.Println("Deja de rechazar loco\nEspera 10 segundos...")
		for range 10 {
			fmt.Println("Esperando...")
		}
	}

	if prob_aceptar < 0.1 {
		fmt.Printf("No hay ofertas disponibles\n")

		return &pb.OfertaDisponible{Disponible: false}, nil
	} else {
		botin := rand.Intn(20000)
		prob_franklin := rand.Float32()
		prob_trevor := rand.Float32()
		riesgo := rand.Float32()
		fmt.Println("Oferta enviada!")
		time.Sleep(30 * time.Millisecond)
		return &pb.OfertaDisponible{Disponible: true, BotinInicial: int32(botin), ProbabilidadFranklin: prob_franklin, ProbabilidadTrevor: prob_trevor, RiesgoPolicial: riesgo}, nil
	}

}

func main() {
	//port := ":50051"
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterOfertaServer(s, &server{})

	log.Printf("\nLester en linea\n")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
