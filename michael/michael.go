package main

import (
	"context"
	"fmt"

	"time"

	pb "lab1/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	var botin int32
	var prob_franklin float32
	var prob_trevor float32
	var riesgo float32

	rechazos := 0

	conn_lester, err_lester := grpc.Dial("lester:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err_lester != nil {
		fmt.Printf("No se pudo conectar: %v\n", err_lester)
		return
	}

	defer conn_lester.Close()

	fmt.Println("---\nIniciar fase 1!")

	client_lester := pb.NewOfertaClient(conn_lester)

	for {

		request_lester := &pb.SolicitudOferta{Rechazos: int32(rechazos)}

		fmt.Println("---\nSolicitud enviada!")
		time.Sleep(30 * time.Millisecond)

		response, err_lester := client_lester.EntregarOferta(context.Background(), request_lester)

		if err_lester != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err_lester)
			return
		}

		if response.GetDisponible() {

			fmt.Printf("Oferta recibida:\n\n")
			time.Sleep(30 * time.Millisecond)
			fmt.Printf("Botin inicial: %d\n", response.GetBotinInicial())
			fmt.Printf("Probabilidad de exito Franklin: %f\n", response.GetProbabilidadFranklin())
			fmt.Printf("Probabilidad de exito Trevor: %f\n", response.GetProbabilidadTrevor())
			fmt.Printf("Riesgo Policial: %f\n", response.GetRiesgoPolicial())

			if (response.GetProbabilidadFranklin() > 0.5 || response.GetProbabilidadTrevor() > 0.5) && response.GetRiesgoPolicial() < 0.8 {
				botin = response.GetBotinInicial()
				prob_franklin = response.GetProbabilidadFranklin()
				prob_trevor = response.GetProbabilidadTrevor()
				riesgo = response.GetRiesgoPolicial()
				fmt.Println("\nOferta ACEPTADA")
				break
			} else {
				fmt.Printf("\nOferta RECHAZADA, se enviara otra solicitud\n")
				rechazos++
			}

		} else {
			fmt.Print("Se enviara otra solicitud\n")
		}
	}

	fmt.Printf("\n--- OFERTA FINAL ---\n\n")
	fmt.Printf("BOTIN INICIAL: %d\n", botin)
	fmt.Printf("PROBABILIDAD DE EXITO DE FRANKLIN: %f\n", prob_franklin)
	fmt.Printf("PROBABILIDAD DE EXITO DE TREVOR: %f\n", prob_trevor)
	fmt.Printf("RIESGO POLICIAL: %f\n", riesgo)
	fmt.Println("\n--- OFERTA FINAL ---")
	fmt.Printf("\nProceder a fase 2!\n\n")

	franklin := prob_franklin > prob_trevor

	// ------------------ FASE 2 -------------------------

	if franklin {

		conn_franklin, err_franklin := grpc.Dial("franklin:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err_franklin != nil {
			fmt.Printf("No se pudo conectar: %v\n", err_franklin)
			return
		}

		defer conn_franklin.Close()

		client_franklin := pb.NewSegundaFaseClient(conn_franklin)
		request_franklin := &pb.InformarTrabajo{ProbabilidadExito: prob_franklin}

		fmt.Println("----------\nInformando a Franklin para proceder a fase 2!")
		time.Sleep(30 * time.Millisecond)
		exito_primera_mision, err_franklin := client_franklin.InformarEstadoSegundaFase(context.Background(), request_franklin)

		if err_franklin != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err_franklin)
			return
		}
		time.Sleep(80 * time.Millisecond)
		if exito_primera_mision.GetExito() {
			fmt.Printf("\nFase 2 finalizada con exito!!!\n")
		} else {
			fmt.Printf("\nFase 2 fracasó D:\n")
		}

	} else {

		conn_trevor, err_trevor := grpc.Dial("trevor:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err_trevor != nil {
			fmt.Printf("No se pudo conectar: %v\n", err_trevor)
			return
		}

		defer conn_trevor.Close()

		client_trevor := pb.NewSegundaFaseClient(conn_trevor)
		request_trevor := &pb.InformarTrabajo{ProbabilidadExito: prob_trevor}

		fmt.Println("----------\nInformando a Trevor para proceder con fase 2!")
		time.Sleep(30 * time.Millisecond)

		exito_primera_mision, err_trevor := client_trevor.InformarEstadoSegundaFase(context.Background(), request_trevor)

		if err_trevor != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err_trevor)
			return
		}

		if exito_primera_mision.GetExito() {
			fmt.Printf("\nFase 2 finalizada con exito!!!\n")
		} else {
			fmt.Printf("\nFase 2 fracasó D:\n")
		}
	}

	// ------------------ FASE 3 ------------------------

	if !franklin {

		// Informar a Lester y a Franklin para proceder a fase 3

	} else {

		// Informar a Lester y a Trevor para proceder a fase 3
	}
}
