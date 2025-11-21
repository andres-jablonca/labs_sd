package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes" // IMPORTANTE: Para detectar código de error
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status" // IMPORTANTE: Para leer el estatus del error

	pb "lab3/proto"
)

const (
	COORDINATOR_ADDR = "coordinator:50055"
)

var knownFlights = []string{
	"LA-500",
	"SK-772",
	"AA-901",
	"DL-456",
	"AF-021",
	"IB-6833",
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// 1. CONFIGURACIÓN DE IDENTIDAD
	idPtr := flag.String("id", "RYW-Generic", "ID del Cliente para logs")
	flag.Parse()

	// Configurar el prefijo del log para identificar quién habla
	clientIDLog := fmt.Sprintf("[%s] ", *idPtr)
	log.SetPrefix(clientIDLog)

	log.Println("Esperando 10s para iniciar tráfico...")
	time.Sleep(10 * time.Second)
	log.Println("--- INICIANDO SIMULACIÓN DE PASAJEROS ---")

	// Conexión al Coordinador
	conn, err := grpc.Dial(COORDINATOR_ADDR, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("FATAL: No se pudo conectar al Coordinador: %v", err)
	}
	defer conn.Close()

	client := pb.NewCheckInCoordinatorClient(conn)

	// --- CONTADOR DE FALLOS ---
	consecutiveFailures := 0
	const maxFailures = 3

	for {
		// Generar datos aleatorios
		passengerID := fmt.Sprintf("Pasajero-%d", rand.Intn(10000))
		flightID := knownFlights[rand.Intn(len(knownFlights))]
		seatRow := rand.Intn(30) + 1
		seatLetter := string(rune('A' + rand.Intn(6)))
		targetSeat := fmt.Sprintf("%d%s", seatRow, seatLetter)
		reqUUID := uuid.New().String()

		log.Printf("Inicia check-in: Cliente %s, Vuelo %s, Asiento %s", passengerID, flightID, targetSeat)

		// 1. ESCRITURA
		checkInCtx, cancelWrite := context.WithTimeout(context.Background(), 5*time.Second)
		checkInResp, err := client.ProcessCheckIn(checkInCtx, &pb.CheckInRequest{
			ClientId:    passengerID,
			FlightId:    flightID,
			SeatNumber:  targetSeat,
			RequestUuid: reqUUID,
		})
		cancelWrite()

		if err != nil {
			// --- MANEJO DE SUICIDIO POR FALLO DE CONEXIÓN ---
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.Unavailable {
				consecutiveFailures++
				log.Printf("[ALERTA] Fallo de conexión con Coordinador (%d/%d).", consecutiveFailures, maxFailures)

				if consecutiveFailures >= maxFailures {
					log.Println("[SHUTDOWN] El Coordinador no responde tras 3 intentos. Cerrando Cliente RYW.")
					os.Exit(0)
				}
			} else {
				// Si responde con otro error, es que el server está vivo. Reiniciamos contador.
				consecutiveFailures = 0
			}
			// ------------------------------------------------

			log.Printf("Error RPC Check-in: %v. Saltando...", err)
			waitNextIteration()
			continue
		}

		// Si llegamos aquí, la conexión funcionó
		consecutiveFailures = 0

		if !checkInResp.Success {
			log.Printf("Rechazado: %s.", checkInResp.Message)
			waitNextIteration()
			continue
		}

		log.Printf("Escritura OK: %s", checkInResp.Message)

		// 2. LECTURA (RYW)
		readCtx, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
		boardingPass, err := client.GetBoardingPass(readCtx, &pb.BoardingPassRequest{
			ClientId: passengerID,
			FlightId: flightID,
		})
		cancelRead()

		if err != nil {
			// También revisamos aquí por si el coordinador muere justo entre la escritura y lectura
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.Unavailable {
				log.Println("[ALERTA] Coordinador desapareció durante la lectura.")
				consecutiveFailures++ // Aumentamos, la próxima iteración probablemente matará el proceso
			}

			log.Printf("Error RPC Lectura: %v", err)
			waitNextIteration()
			continue
		}

		// 3. VALIDACIÓN
		if boardingPass.SeatAssigned == targetSeat {
			log.Printf("EXITO RYW: Leído %s correctamente.", boardingPass.SeatAssigned)
		} else {
			log.Printf("FALLO DE CONSISTENCIA: Esperaba %s, recibió '%s'", targetSeat, boardingPass.SeatAssigned)
		}

		waitNextIteration()
	}
}

func waitNextIteration() {
	delay := time.Duration(rand.Intn(5)+2) * time.Second
	time.Sleep(delay)
}
