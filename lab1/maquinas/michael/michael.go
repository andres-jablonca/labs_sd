package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	pb "michael/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/*
generar_reporte_exito()
***
Parametros:

	botin_inicial : int32
	botin_final : int
	botin_extra : int32
	pago_lester : int32
	resto : int
	msj_lester : string
	pago_franklin : int32
	msj_franklin : string
	pago_trevor : int32
	msj_trevor : string

***
Retorno:

	None

***
Genera un archivo 'Reporte.txt' con el detalle de una misión exitosa indicando
botín base, botín extra, botín total y pagos a cada entidad incluyendo el resto asignado a Lester.
***
*/
func generar_reporte_exito(botin_inicial int32, botin_final int, botin_extra int32,
	pago_lester int32, resto int, msj_lester string,
	pago_franklin int32, msj_franklin string,
	pago_trevor int32, msj_trevor string) {
	file, err := os.Create("Reporte.txt")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	fmt.Fprintf(file, "=========================================================\n")
	fmt.Fprintf(file, "== REPORTE FINAL DE LA MISION ==\n")
	fmt.Fprintf(file, "=========================================================\n")

	banco := rand.Intn(10000) + 1000
	fmt.Fprintf(file, "Mision: Asalto al Banco # %d\n", banco)

	fmt.Fprintf(file, "Resultado Global: MISION COMPLETADA CON EXITO!\n")

	fmt.Fprintf(file, "--- REPARTO DEL BOTIN ---\n")
	fmt.Fprintf(file, "Botin Base: $%d\n", botin_inicial)

	fmt.Fprintf(file, "Botin Extra (Habilidad de Chop): $%d\n", botin_extra)

	fmt.Fprintf(file, "Botin Total: $%d\n", botin_final)
	fmt.Fprintf(file, "----------------------------------------------------\n")

	fmt.Fprintf(file, "Pago a Franklin: $%d\n", pago_franklin)
	fmt.Fprintf(file, "Respuesta de Franklin: \"%s\"\n", msj_franklin)
	fmt.Fprintf(file, "Pago a Trevor: $%d\n", pago_trevor)
	fmt.Fprintf(file, "Respuesta de Trevor: \"%s\"\n", msj_trevor)
	fmt.Fprintf(file, "Pago a Lester: $%d (reparto) + $%d (resto)\n", pago_lester-int32(resto), resto)
	fmt.Fprintf(file, "Respuesta de Lester: \"%s\"\n", msj_lester)

	fmt.Fprintf(file, "----------------------------------------------------\n")
	fmt.Fprintf(file, "Saldo Final de la Operacion: $%d\n", botin_final)
	fmt.Fprintf(file, "=========================================================\n")
}

/*
***
Parametros:

	botin_inicial : int32
	botin_final : int
	botin_extra : int32
	fase : int32
	franklin : bool
	motivo_fracaso : string

***
Retorno:

	None

***
Genera un archivo "Reporte.txt" con el detalle de una misión fracasada indicando la fase de fallo, quién falló, motivo, y deja todos los pagos en $0.
***
*/
func generar_reporte_fracaso(botin_inicial int32, botin_final int, botin_extra int32, fase int32, franklin bool, motivo_fracaso string) {
	var quien string
	if franklin && fase == 2 {
		quien = "Franklin"
	} else {
		quien = "Trevor"
	}

	file, err := os.Create("Reporte.txt")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	fmt.Fprintf(file, "=========================================================\n")
	fmt.Fprintf(file, "== REPORTE FINAL DE LA MISION ==\n")
	fmt.Fprintf(file, "=========================================================\n")

	banco := rand.Intn(10000) + 1000
	fmt.Fprintf(file, "Mision: Asalto al Banco # %d\n", banco)

	fmt.Fprintf(file, "Resultado Global: MISION FRACASADA EN FASE %d\n", fase)

	fmt.Fprintf(file, "\nQuien fracasó: %s\n", quien)
	fmt.Fprintf(file, "Motivo del fracaso: %s\n", motivo_fracaso)

	fmt.Fprintf(file, "--- REPARTO DEL BOTIN ---\n")
	fmt.Fprintf(file, "Botin Base: $%d\n", botin_inicial)

	fmt.Fprintf(file, "Botin Extra (Habilidad de Chop): $%d\n", botin_extra)

	fmt.Fprintf(file, "Botin Total: $%d\n", botin_final)
	fmt.Fprintf(file, "----------------------------------------------------\n")

	fmt.Fprintf(file, "Pago a Franklin: $0\n")
	fmt.Fprintf(file, "Pago a Trevor: $0\n")
	fmt.Fprintf(file, "Pago a Lester: $0\n")

	fmt.Fprintf(file, "----------------------------------------------------\n")
	fmt.Fprintf(file, "Saldo Final de la Operacion: $%d\n", botin_final)
	fmt.Fprintf(file, "=========================================================\n")
}

/*
main()
***
Orquesta el flujo completo de la misión (cliente Michael):
1) Establece conexiones gRPC con Lester, Trevor y Franklin.
2) Negocia ofertas con Lester (fase 1) con validación y aceptación/rechazo.
3) Inicia la distracción (fase 2) con el integrante de mayor probabilidad de exito.
4) Inicia el golpe (fase 3) avisando a Lester el inicio/detencion de notificaciones de estrellas.
5) Calcula reparto, paga a las entidades y genera el reporte final de la mision sin importar si fue un exito o fracaso (fase 4).
***
*/
func main() {

	var botin int32
	var prob_franklin float32
	var prob_trevor float32
	var riesgo float32

	// ESTABLECER CONEXIONES

	conn_lester, err_lester := grpc.Dial("10.35.168.43:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err_lester != nil {
		fmt.Printf("No se pudo conectar: %v\n", err_lester)
		return
	}
	defer conn_lester.Close()

	conn_trevor, err_trevor := grpc.Dial("10.35.168.46:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err_trevor != nil {
		fmt.Printf("No se pudo conectar: %v\n", err_trevor)
		return
	}
	defer conn_trevor.Close()

	conn_franklin, err_franklin := grpc.Dial("10.35.168.45:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err_franklin != nil {
		fmt.Printf("No se pudo conectar: %v\n", err_franklin)
		return
	}
	defer conn_franklin.Close()

	// INICIO

	fmt.Println("----------\nIniciar fase 1!")
	time.Sleep(2 * time.Second)
	client_lester := pb.NewMichaelLesterClient(conn_lester)

	for {

		request_lester := &pb.SolicitudOferta{}

		fmt.Printf("----------\nSolicitando oferta...\n\n")
		time.Sleep(2 * time.Second)

		response, err_lester := client_lester.EntregarOferta(context.Background(), request_lester)

		if err_lester != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err_lester)
			return
		}

		if response.GetDisponible() && (response.GetBotinInicial() == 0 || response.GetProbabilidadFranklin() == 0.0 || response.GetProbabilidadTrevor() == 0.0 || response.GetRiesgoPolicial() == 0.0) {
			//---INFORMAR A LESTER QUE SE RECHAZO
			fmt.Printf("Oferta INVÁLIDA, se enviará otra solicitud...\n")
			time.Sleep(2 * time.Second)
			request_lester := &pb.ConfirmacionOferta{Aceptada: false}
			fmt.Println("\nInformando rechazo de oferta...")
			time.Sleep(2 * time.Second)
			_, err_lester := client_lester.ConfirmarOferta(context.Background(), request_lester)
			if err_lester != nil {
				fmt.Printf("Error al llamar al servicio: %v\n", err_lester)
				return
			}
		} else if response.GetDisponible() {

			fmt.Printf("Oferta recibida:\n\n")
			time.Sleep(2 * time.Second)
			fmt.Printf("Botín inicial: %d\n", response.GetBotinInicial())
			fmt.Printf("Probabilidad de éxito Franklin: %f\n", response.GetProbabilidadFranklin())
			fmt.Printf("Probabilidad de éxito Trevor: %f\n", response.GetProbabilidadTrevor())
			fmt.Printf("Riesgo Policial: %f\n", response.GetRiesgoPolicial())
			time.Sleep(2 * time.Second)
			if (response.GetProbabilidadFranklin() > 0.5 || response.GetProbabilidadTrevor() > 0.5) && response.GetRiesgoPolicial() < 0.8 {
				botin = response.GetBotinInicial()
				prob_franklin = response.GetProbabilidadFranklin()
				prob_trevor = response.GetProbabilidadTrevor()
				riesgo = response.GetRiesgoPolicial()
				fmt.Println("\nOferta ACEPTADA")
				time.Sleep(2 * time.Second)
				// --- INFORMAR A LESTER QUE SE ACEPTO
				request_lester := &pb.ConfirmacionOferta{Aceptada: true}
				fmt.Println("\n\nInformando aceptación de oferta...")
				time.Sleep(2 * time.Second)
				_, err_lester := client_lester.ConfirmarOferta(context.Background(), request_lester)
				if err_lester != nil {
					fmt.Printf("Error al llamar al servicio: %v\n", err_lester)
					return
				}
				break
			} else {
				//---INFORMAR A LESTER QUE SE RECHAZO
				fmt.Printf("\nOferta RECHAZADA, se enviará otra solicitud...\n")
				time.Sleep(2 * time.Second)
				request_lester := &pb.ConfirmacionOferta{Aceptada: false}
				fmt.Println("----------\nInformando rechazo de oferta...")
				time.Sleep(2 * time.Second)
				_, err_lester := client_lester.ConfirmarOferta(context.Background(), request_lester)
				if err_lester != nil {
					fmt.Printf("Error al llamar al servicio: %v\n", err_lester)
					return
				}
			}

		} else {
			fmt.Print("Solicitaré otra oferta en 3 segundos...\n")
			time.Sleep(3 * time.Second)
		}
	}

	fmt.Printf("\n====== OFERTA FINAL ======\n\n")
	fmt.Printf("BOTIN INICIAL: %d\n", botin)
	fmt.Printf("PROBABILIDAD DE EXITO DE FRANKLIN: %f\n", prob_franklin)
	fmt.Printf("PROBABILIDAD DE EXITO DE TREVOR: %f\n", prob_trevor)
	fmt.Printf("RIESGO POLICIAL: %f\n", riesgo)
	fmt.Printf("\n====== OFERTA FINAL ======\n")
	time.Sleep(2 * time.Second)
	fmt.Printf("\nProceder a fase 2!\n\n")
	time.Sleep(2 * time.Second)
	franklin := prob_franklin > prob_trevor

	// ------------------ FASE 2 -------------------------
	exito_fase_2 := false
	motivo_fracaso_fase_2 := ""

	if franklin { // Caso de Franklin en fase 2

		client_franklin := pb.NewMichaelTrevorFranklinClient(conn_franklin)
		request_franklin := &pb.InfoDistraccion{ProbabilidadExito: prob_franklin}

		fmt.Println("----------\nInformando a Franklin para proceder a la fase 2...")
		time.Sleep(2 * time.Second)
		exito_segunda_fase, err_franklin := client_franklin.InformarDistraccion(context.Background(), request_franklin)

		if err_franklin != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err_franklin)
			return
		}
		if exito_segunda_fase.GetExito() {
			fmt.Printf("\nFase 2 finalizada con éxito!!!\n----------\n")
			exito_fase_2 = true
			time.Sleep(2 * time.Second)
		} else {
			fmt.Printf("\nFase 2 fracasó terriblemente...\n----------\n\n")
			time.Sleep(2 * time.Second)
			motivo_fracaso_fase_2 = exito_segunda_fase.GetMotivo()
		}

	} else { // Caso de Trevor en fase 2

		client_trevor := pb.NewMichaelTrevorFranklinClient(conn_trevor)
		request_trevor := &pb.InfoDistraccion{ProbabilidadExito: prob_trevor}

		fmt.Println("----------\nInformando a Trevor para proceder con la fase 2...")
		time.Sleep(2 * time.Second)

		exito_segunda_fase, err_trevor := client_trevor.InformarDistraccion(context.Background(), request_trevor)

		if err_trevor != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err_trevor)
			return
		}

		if exito_segunda_fase.GetExito() {
			fmt.Printf("\nFase 2 finalizada con éxito!!!\n----------\n")
			exito_fase_2 = true
			time.Sleep(2 * time.Second)
		} else {
			fmt.Printf("\nFase 2 fracasó terriblemente...\n----------\n\n")
			motivo_fracaso_fase_2 = exito_segunda_fase.GetMotivo()
			time.Sleep(2 * time.Second)
		}
	}

	// ------------------ FASE 3 ------------------------
	exito_fase_3 := false
	botin_final := 0
	botin_fase3 := 0
	motivo_fracaso_fase_3 := ""
	botin_extra := 0
	estrellas_final := 0

	if !franklin && exito_fase_2 { // Caso de exito de Trevor en fase 2 -> Franklin para fase 3
		// Notificar a Lester que inicie notificaciones para Franklin
		fmt.Println("Solicitando a Lester que inicie el envío de notificaciones para Franklin...")
		inicioNotifReq := &pb.InicioNotifEstrellas{
			Personaje:      "Franklin",
			RiesgoPolicial: riesgo,
		}
		_, err := client_lester.IniciarNotificacionesEstrellas(context.Background(), inicioNotifReq)
		if err != nil {
			fmt.Printf("Error al iniciar envío de notificaciones: %v\n", err)
			return
		}

		// Informar a franklin para fase 3
		client_franklin := pb.NewMichaelTrevorFranklinClient(conn_franklin)
		request_franklin := &pb.InfoGolpe{
			ProbabilidadExito: prob_franklin,
			Botin:             botin,
			RiesgoPolicial:    riesgo,
		}

		fmt.Println("\nInformando a Franklin para proceder a la fase 3...")
		time.Sleep(2 * time.Second)
		exito_tercera_fase, err_franklin := client_franklin.InformarGolpe(context.Background(), request_franklin)

		if err_franklin != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err_franklin)
			return
		}

		// Informar a Lester para detener notificaciones
		detenerNotifReq := &pb.DetenerNotifEstrellas{Personaje: "Franklin", Exito: exito_tercera_fase.GetExito()}
		_, err = client_lester.DetenerNotificacionesEstrellas(context.Background(), detenerNotifReq)
		if err != nil {
			fmt.Printf("Error al detener envío de notificaciones: %v\n", err)
		}

		if exito_tercera_fase.GetExito() {
			exito_fase_3 = true
			fmt.Printf("\nFase 3 finalizada con éxito!!!\n----------\n")
			botin_extra = int(exito_tercera_fase.GetBotinExtra())
			botin_fase3 = int(exito_tercera_fase.GetBotin())

			estrellas_final = int(exito_tercera_fase.GetEstrellasFinal())
			fmt.Printf("Botín recolectado por Franklin: %v\n", botin_fase3)
			fmt.Printf("Botín extra recolectado por Chop: %v\n", botin_extra)
			fmt.Printf("Misión completada con: %v estrellas\n\n", estrellas_final)
			time.Sleep(2 * time.Second)
		} else {
			fmt.Printf("\nFase 3 fracasó terriblemente...\n\nInformando a Lester\n----------\n")
			motivo_fracaso_fase_3 = exito_tercera_fase.GetMotivo()
			botin_extra = int(exito_tercera_fase.GetBotinExtra())
			//estrellas_final = int(exito_tercera_fase.GetEstrellasFinal())
			fmt.Printf("Motivo: %s\n", motivo_fracaso_fase_3)
			//fmt.Printf("Estrellas finales: %v\n", estrellas_final)
			time.Sleep(2 * time.Second)
		}

	} else if franklin && exito_fase_2 { // Caso de exito de Franklin en fase 2 -> Trevor para fase 3
		// Notificar a Lester que inicie notificaciones para Trevor
		fmt.Println("Solicitando a Lester que inicie el envío de notificaciones para Trevor...")
		inicioNotifReq := &pb.InicioNotifEstrellas{
			Personaje:      "Trevor",
			RiesgoPolicial: riesgo,
		}
		_, err := client_lester.IniciarNotificacionesEstrellas(context.Background(), inicioNotifReq)
		if err != nil {
			fmt.Printf("Error al iniciar envío de notificaciones: %v\n", err)
			return
		}

		// Informar a Trevor para fase 3
		client_trevor := pb.NewMichaelTrevorFranklinClient(conn_trevor)
		request_trevor := &pb.InfoGolpe{
			ProbabilidadExito: prob_trevor,
			Botin:             botin,
			RiesgoPolicial:    riesgo,
		}

		fmt.Println("\nInformando a Trevor para proceder con la fase 3...")
		time.Sleep(2 * time.Second)

		exito_tercera_fase, err_trevor := client_trevor.InformarGolpe(context.Background(), request_trevor)

		if err_trevor != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err_trevor)
			return
		}

		// Informar a Lester para detener notificaciones
		detenerNotifReq := &pb.DetenerNotifEstrellas{Personaje: "Trevor", Exito: exito_tercera_fase.GetExito()}
		_, err = client_lester.DetenerNotificacionesEstrellas(context.Background(), detenerNotifReq)
		if err != nil {
			fmt.Printf("Error al detener envío de notificaciones: %v\n", err)
		}

		if exito_tercera_fase.GetExito() {
			exito_fase_3 = true
			fmt.Printf("\nFase 3 finalizada con éxito!!!\n----------\n")
			botin_fase3 = int(exito_tercera_fase.GetBotin())
			botin_extra = int(exito_tercera_fase.GetBotinExtra())
			estrellas_final = int(exito_tercera_fase.GetEstrellasFinal())
			fmt.Printf("Botín recolectado por Trevor: %v\n", botin_fase3)
			fmt.Printf("Misión completada con: %v estrellas\n\n", estrellas_final)
			time.Sleep(2 * time.Second)
		} else {
			fmt.Printf("\nFase 3 fracasó terriblemente...\n\nInformando a Lester...\n----------\n")
			motivo_fracaso_fase_3 = exito_tercera_fase.GetMotivo()
			botin_extra = int(exito_tercera_fase.GetBotinExtra())
			//estrellas_final = int(exito_tercera_fase.GetEstrellasFinal())
			fmt.Printf("Motivo: %s\n", motivo_fracaso_fase_3)
			//fmt.Printf("Estrellas finales: %v\n", estrellas_final)
			time.Sleep(2 * time.Second)
		}
	}

	// ------------------- FASE 4 ---------------------------

	correspondencias := 0
	correspondencias_lester := 0
	resto := 0

	// Calcular reparto y resto
	botin_final = botin_fase3 + botin_extra
	correspondencias = botin_final / 4
	correspondencias_lester = correspondencias
	resto = botin_final % 4

	fmt.Printf("Informando término de la misión...\n")

	// Pagar si corresponde y generar reportes finales
	if exito_fase_2 && exito_fase_3 { // Caso exito fase 2 y fase 3
		fmt.Printf("----------\n")
		if resto != 0 {
			correspondencias_lester = correspondencias + resto
		}

		fmt.Printf("Pagando a Lester...\n")
		request_lester := &pb.MontoPago{Correspondencia: int32(correspondencias_lester), Total: int32(botin_final)}

		respuesta_lester, err := client_lester.PagarLester(context.Background(), request_lester)
		if err != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err)
			return
		}
		time.Sleep(2 * time.Second)

		client_franklin := pb.NewMichaelTrevorFranklinClient(conn_franklin)
		fmt.Printf("Pagando a Franklin...\n")
		request_franklin := &pb.MontoPagoMiembro{Correspondencia: int32(correspondencias), Total: int32(botin_final)}

		respuesta_franklin, err := client_franklin.PagarMiembro(context.Background(), request_franklin)
		if err != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err)
			return
		}
		time.Sleep(2 * time.Second)

		client_trevor := pb.NewMichaelTrevorFranklinClient(conn_trevor)
		fmt.Printf("Pagando a Trevor...\n")
		request_trevor := &pb.MontoPagoMiembro{Correspondencia: int32(correspondencias), Total: int32(botin_final)}

		respuesta_trevor, err := client_trevor.PagarMiembro(context.Background(), request_trevor)
		if err != nil {
			fmt.Printf("Error al llamar al servicio: %v\n", err)
			return
		}
		time.Sleep(2 * time.Second)

		if respuesta_franklin.GetCorrecto() && respuesta_trevor.GetCorrecto() && respuesta_lester.GetCorrecto() {
			fmt.Printf("Fue un placer trabajar con ustedes muchachos!\n")
		} else {
			fmt.Printf("Ups...\n")
		}
		fmt.Printf("\nGenerando reporte final de la misión...\n----------\n")
		generar_reporte_exito(botin, botin_final, int32(botin_extra), int32(correspondencias_lester), resto, respuesta_lester.GetMensaje(), int32(correspondencias), respuesta_franklin.GetMensaje(), int32(correspondencias), respuesta_trevor.GetMensaje())

	} else if exito_fase_2 && !exito_fase_3 { // Caso exito fase 2 y fracaso fase 3
		fmt.Printf("\n----------\n")
		time.Sleep(2 * time.Second)

		fmt.Printf("\nGenerando reporte final de la misión...\n\n----------\n")
		generar_reporte_fracaso(botin, botin_final, int32(botin_extra), 3, franklin, motivo_fracaso_fase_3)

	} else if !exito_fase_2 && !exito_fase_3 { // Caso fracaso fase 2
		fmt.Printf("\n----------\n")
		time.Sleep(2 * time.Second)

		fmt.Printf("\nGenerando reporte final de la misión...\n\n----------\n")
		generar_reporte_fracaso(botin, botin_final, int32(botin_extra), 2, franklin, motivo_fracaso_fase_2)

	}

}
