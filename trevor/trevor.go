package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	pb "lab1/proto"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedMichaelTrevorFranklinServer
}

func (s *server) InformarEstadoSegundaFase(ctx context.Context, inf *pb.InformarDistraccion) (*pb.ResultadoDistraccion, error) {

	fmt.Printf("Confirmo inicio de fase 2!\n")

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
			time.Sleep(200 * time.Millisecond)
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
	return &pb.ResultadoDistraccion{Exito: exito}, nil

}

func (s *server) InformarEstadoTerceraFase(ctx context.Context, inf *pb.InformarGolpe) (*pb.ResultadoGolpe, error) {

	fmt.Printf("Confirmo inicio de fase 3!\n")

	rand.Seed(time.Now().UnixNano())
	probabilidad_exito := inf.GetProbabilidadExito()

	turnos_necesarios := 200 - int(probabilidad_exito*100)
	exito := true
	time.Sleep(80 * time.Millisecond)
	for i := range turnos_necesarios {

		fmt.Printf("Trevor trabajando... (%d turnos restantes)\n", turnos_necesarios-i)
		time.Sleep(200 * time.Millisecond)

	}
	if exito {
		fmt.Println("Golpe completado!")
	}
	return &pb.ResultadoGolpe{Exito: exito, Botin: 10000}, nil

}

func (s *server) Pagar(ctx context.Context, req *pb.Monto) (*pb.ConfirmarPago, error) {
	check := false
	msj := "Esto no es lo que acordamos.."
	correspondencias := int32(req.GetTotal() / 4)
	if correspondencias == req.GetCorrespondencia() {
		check = true
		msj = "Justo lo que esperaba!"
	}

	fmt.Printf("Total: %d, Recibido: %d, Esperado: %d \n", req.Total, req.Correspondencia, correspondencias)
	fmt.Println(msj)
	return &pb.ConfirmarPago{Correcto: check, Mensaje: msj}, nil
}

func main() {
	//port := ":50051"
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMichaelTrevorFranklinServer(s, &server{})

	log.Printf("\nTrevor en linea\n")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
