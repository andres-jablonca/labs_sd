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
	pb.UnimplementedSegundaFaseServer
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
			time.Sleep(5 * time.Millisecond)
		} else {
			probabilidad_fallar = rand.Float32()
			if probabilidad_fallar < 0.1 {
				fmt.Println("La mision fracasó debido a que Trevor se emborrachó y fue descubierto")
				exito = false
				break
			}
		}
	}
	fmt.Println("Distraccion completada!")
	return &pb.Resultado{Exito: exito}, nil

}

func main() {
	//port := ":50051"
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterSegundaFaseServer(s, &server{})

	log.Printf("\nTrevor en linea\n")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
