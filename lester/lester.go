package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	pb "lester/proto"

	//"github.com/streadway/amqp"
	"google.golang.org/grpc"
)

var rechazos int = 0

type server struct {
	pb.UnimplementedMichaelLesterServer
	//ch *amqp.Channel
	//q  amqp.Queue
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
		botin := rand.Intn(20000)
		prob_franklin := rand.Float32()
		prob_trevor := rand.Float32()
		riesgo := rand.Float32()
		fmt.Printf("Oferta enviada!\n")
		time.Sleep(time.Second)
		return &pb.OfertaDisponible{Disponible: true, BotinInicial: int32(botin), ProbabilidadFranklin: prob_franklin, ProbabilidadTrevor: prob_trevor, RiesgoPolicial: riesgo}, nil
	}

}

func (s *server) ConfirmarOferta(ctx context.Context, req *pb.Confirmacion) (*pb.AckConfirmacion, error) {
	confir := req.GetAceptada()
	if !confir {
		fmt.Printf(">:(\n")
		time.Sleep(time.Second)
		rechazos++
	} else {
		fmt.Printf("Enterado de la aceptación de la oferta!\n")
		time.Sleep(time.Second)
		return &pb.AckConfirmacion{}, nil
	}
	if rechazos%3 == 0 || rechazos == 0 {
		fmt.Printf("Deja de rechazar loco, ahora espera 10 segundos..\n")
		time.Sleep(10 * time.Second)
	}
	return &pb.AckConfirmacion{}, nil
}

func (s *server) Pagar(ctx context.Context, req *pb.Monto) (*pb.ConfirmarPago, error) {
	total := req.GetTotal()
	pagoRecibido := req.GetCorrespondencia()

	pagoEsperadoPorPersona := total / 4
	resto := total % 4

	pagoEsperadoLester := pagoEsperadoPorPersona + resto

	check := false
	msj := "Esto no es lo que acordamos.."

	if pagoRecibido == pagoEsperadoLester {
		check = true
		msj = "Un placer hacer negocios."
	} else if pagoRecibido == pagoEsperadoPorPersona && resto == 0 {
		check = true
		msj = "Un placer hacer negocios."
	}

	fmt.Printf("Total: %d, Recibido: %d, Esperado: %d \n", total, pagoRecibido, pagoEsperadoLester)
	fmt.Println(msj)

	return &pb.ConfirmarPago{Correcto: check, Mensaje: msj}, nil
}

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMichaelLesterServer(s, &server{})

	fmt.Printf("\nLester en linea\n")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
