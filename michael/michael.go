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

	conn_lester, err_lester := grpc.Dial("lester:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err_lester != nil {
		fmt.Printf("No se pudo conectar: %v\n", err_lester)
		return
	}

	defer conn_lester.Close()

	fmt.Println("---\nIniciar fase 1!")
	time.Sleep(time.Second)
	client_lester := pb.NewOfertaClient(conn_lester)

	for {

		request_lester := &pb.SolicitudOferta{}

		fmt.Println("---\nSolicitud enviada!")
		time.Sleep(2 * time.Second)

		response, err_lester := client_lester.EntregarOferta(context.Background(), request_lester)

		if err_lester != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err_lester)
			return
		}

		if response.GetDisponible() {

			fmt.Printf("Oferta recibida:\n\n")
			time.Sleep(time.Second)
			fmt.Printf("Botin inicial: %d\n", response.GetBotinInicial())
			fmt.Printf("Probabilidad de exito Franklin: %f\n", response.GetProbabilidadFranklin())
			fmt.Printf("Probabilidad de exito Trevor: %f\n", response.GetProbabilidadTrevor())
			fmt.Printf("Riesgo Policial: %f\n", response.GetRiesgoPolicial())
			time.Sleep(time.Second)
			if (response.GetProbabilidadFranklin() > 0.5 || response.GetProbabilidadTrevor() > 0.5) && response.GetRiesgoPolicial() < 0.8 {
				botin = response.GetBotinInicial()
				prob_franklin = response.GetProbabilidadFranklin()
				prob_trevor = response.GetProbabilidadTrevor()
				riesgo = response.GetRiesgoPolicial()
				fmt.Println("\nOferta ACEPTADA")
				time.Sleep(time.Second)
				// --- INFORMAR A LESTER QUE SE ACEPTO
				request_lester := &pb.Confirmacion{Aceptada: true}
				fmt.Println("---\nInformando aceptación de oferta")
				time.Sleep(2 * time.Second)
				_, err_lester := client_lester.ConfirmarOferta(context.Background(), request_lester)
				if err_lester != nil {
					fmt.Printf("Error al llamar al servicio: %v\n", err_lester)
					return
				}
				break
			} else {
				//---INFORMAR A LESTER QUE SE RECHAZO
				fmt.Printf("\nOferta RECHAZADA, se enviara otra solicitud\n")
				time.Sleep(time.Second)
				request_lester := &pb.Confirmacion{Aceptada: false}
				fmt.Println("---\nInformando rechazo de oferta")
				time.Sleep(2 * time.Second)
				_, err_lester := client_lester.ConfirmarOferta(context.Background(), request_lester)
				if err_lester != nil {
					fmt.Printf("Error al llamar al servicio: %v\n", err_lester)
					return
				}
			}

		} else {
			fmt.Print("Se enviara otra solicitud en 3 segundos\n")
			time.Sleep(3 * time.Second)
		}
	}

	fmt.Printf("\n--- OFERTA FINAL ---\n\n")
	fmt.Printf("BOTIN INICIAL: %d\n", botin)
	fmt.Printf("PROBABILIDAD DE EXITO DE FRANKLIN: %f\n", prob_franklin)
	fmt.Printf("PROBABILIDAD DE EXITO DE TREVOR: %f\n", prob_trevor)
	fmt.Printf("RIESGO POLICIAL: %f\n", riesgo)
	fmt.Println("\n--- OFERTA FINAL ---")
	time.Sleep(time.Second)
	fmt.Printf("\nProceder a fase 2!\n\n")
	time.Sleep(time.Second)
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
		time.Sleep(2 * time.Second)
		exito_primera_mision, err_franklin := client_franklin.InformarEstadoSegundaFase(context.Background(), request_franklin)

		if err_franklin != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err_franklin)
			return
		}
		time.Sleep(80 * time.Millisecond)
		if exito_primera_mision.GetExito() {
			fmt.Printf("\nFase 2 finalizada con exito!!!\n")
			time.Sleep(time.Second)
		} else {
			fmt.Printf("\nFase 2 fracasó D:\n")
			time.Sleep(time.Second)
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
		time.Sleep(2 * time.Second)

		exito_primera_mision, err_trevor := client_trevor.InformarEstadoSegundaFase(context.Background(), request_trevor)

		if err_trevor != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err_trevor)
			return
		}

		if exito_primera_mision.GetExito() {
			fmt.Printf("\nFase 2 finalizada con exito!!!\n")
			time.Sleep(time.Second)
		} else {
			fmt.Printf("\nFase 2 fracasó D:\n")
			time.Sleep(time.Second)
		}
	}

// ------------------ FASE 3 ------------------------
fmt.Println("\n========== FASE 3: EL GOLPE ==========")

// Si Franklin hizo la Fase 2, ahora la Fase 3 la hace Trevor; y viceversa.
objetivo := "Trevor"
probSeleccionado := prob_trevor
hostPersonaje := "trevor:50051"
if !franklin { // en Fase 2 trabajó Trevor → ahora Franklin
	objetivo = "Franklin"
	probSeleccionado = prob_franklin
	hostPersonaje = "franklin:50051"
}
fmt.Printf("Seleccionado para Fase 3: %s (por descarte)\n", objetivo)

// 1) Pedir a Lester que inicie las notificaciones de estrellas
mon := pb.NewMonitoreoClient(conn_lester)
ctxMon, cancelMon := context.WithTimeout(context.Background(), 3*time.Second)
defer cancelMon()
_, err := mon.IniciarNotificaciones(ctxMon, &pb.ObjetivoNotificacion{
	Personaje:      objetivo,
	RiesgoPolicial: riesgo,
})
if err != nil {
	fmt.Printf("[Michael] ❌ Error IniciarNotificacionEstrellas() -> %s: %v\n", objetivo, err)
	return
}
fmt.Printf("[Michael] 📡 IniciarNotificacionEstrellas() -> %s\n", objetivo)

// 2) Ordenar al personaje que inicie el golpe
connPers, err := grpc.Dial(hostPersonaje, grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
	fmt.Printf("[Michael] ❌ No se pudo conectar a %s: %v\n", objetivo, err)
	return
}
defer connPers.Close()

cFase3 := pb.NewTerceraFaseClient(connPers)
req := &pb.OrdenGolpe{
	ProbabilidadExito: probSeleccionado,
	RiesgoPolicial:    riesgo,
	BotinInicial:      botin,
	Quien:             objetivo,
}

fmt.Printf("[Michael] ▶️ IniciarGolpe() -> %s (gRPC)\n", objetivo)
ack, err := cFase3.IniciarGolpe(context.Background(), req)
if err != nil {
	fmt.Printf("[Michael] ❌ Error IniciarGolpe con %s: %v\n", objetivo, err)
	return
}
if ack.GetOk() {
	fmt.Printf("[Michael] ⬅️ ConfirmarInicio() de %s: %s\n", objetivo, ack.GetDetalle())
} else {
	fmt.Printf("[Michael] ⬅️ ConfirmarInicio() de %s: rechazado\n", objetivo)
}

fmt.Println("========== FASE 3: INICIO CONFIRMADO ==========\n")

// (Opcional) cuando el golpe termine podrías:
// ctxStop, cancelStop := context.WithTimeout(context.Background(), 3*time.Second)
// defer cancelStop()
// _, _ = mon.DetenerNotificaciones(ctxStop, &pb.ObjetivoNotificacion{Personaje: objetivo})

}
