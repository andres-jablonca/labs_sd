package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "lab3/proto"
)

const (
	COORDINATOR_ADDR = "coordinator:50055"
)

// Lista de vuelos conocidos (extraídos de tus logs anteriores)
var knownFlights = []string{
	"LA-500",  // Latam
	"SK-772",  // Sky
	"AA-901",  // American
	"DL-456",  // Delta
	"AF-021",  // Air France
	"IB-6833", // Iberia
}

func main() {
	// Semilla para aleatoriedad
	rand.Seed(time.Now().UnixNano())

	// Esperar un poco a que el sistema levante completamente
	log.Println("⏳ Esperando 10s para iniciar tráfico de pasajeros...")
	time.Sleep(10 * time.Second)
	log.Println("--- 🛫 TRÁFICO DE PASAJEROS RYW INICIADO 🛬 ---")

	// Conexión PERSISTENTE al Coordinador (se reutiliza para todos los pasajeros)
	conn, err := grpc.Dial(COORDINATOR_ADDR, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("❌ FATAL: No se pudo conectar al Coordinador: %v", err)
	}
	defer conn.Close()

	client := pb.NewCheckInCoordinatorClient(conn)

	// BUCLE INFINITO DE SIMULACIÓN
	for {
		// 1. Generar datos aleatorios para un NUEVO pasajero
		passengerID := fmt.Sprintf("Pasajero-%d", rand.Intn(10000)) // Ej: Pasajero-4023
		flightID := knownFlights[rand.Intn(len(knownFlights))]      // Vuelo al azar
		seatRow := rand.Intn(30) + 1                                // Fila 1 a 30
		seatLetter := string(rune('A' + rand.Intn(6)))              // Letra A-F
		targetSeat := fmt.Sprintf("%d%s", seatRow, seatLetter)      // Ej: "12C"
		reqUUID := uuid.New().String()

		log.Printf("---------------------------------------------------------------")
		log.Printf("👤 %s inicia proceso para Vuelo %s, Asiento %s", passengerID, flightID, targetSeat)

		// ========================================================================
		// PASO 1: ESCRITURA (Check-in)
		// ========================================================================
		// log.Printf("🎫 Enviando Check-in (UUID: %s)...", reqUUID)

		checkInCtx, cancelWrite := context.WithTimeout(context.Background(), 5*time.Second)
		checkInResp, err := client.ProcessCheckIn(checkInCtx, &pb.CheckInRequest{
			ClientId:    passengerID,
			FlightId:    flightID,
			SeatNumber:  targetSeat,
			RequestUuid: reqUUID,
		})
		cancelWrite()

		if err != nil {
			log.Printf("❌ Error RPC Check-in: %v. Saltando pasajero...", err)
			waitNextIteration()
			continue
		}

		if !checkInResp.Success {
			log.Printf("⛔ Check-in rechazado por lógica de negocio: %s. Saltando...", checkInResp.Message)
			waitNextIteration()
			continue
		}

		log.Printf("✅ Escritura Confirmada: %s", checkInResp.Message)

		// ========================================================================
		// PASO 2: LECTURA INMEDIATA (Read Your Writes)
		// ========================================================================
		// log.Println("🔎 Solicitando Tarjeta de Embarque (Verificación RYW)...")

		readCtx, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
		boardingPass, err := client.GetBoardingPass(readCtx, &pb.BoardingPassRequest{
			ClientId: passengerID,
			FlightId: flightID,
		})
		cancelRead()

		if err != nil {
			log.Printf("❌ Error RPC GetBoardingPass: %v", err)
			waitNextIteration()
			continue
		}

		// ========================================================================
		// PASO 3: VALIDACIÓN DE CONSISTENCIA
		// ========================================================================
		if boardingPass.SeatAssigned == targetSeat {
			log.Printf("✨ ÉXITO RYW: %s obtuvo su tarjeta para %s en %s correctamente.",
				passengerID, boardingPass.FlightId, boardingPass.SeatAssigned)
		} else {
			log.Printf("💀 FALLO DE CONSISTENCIA CRÍTICO: Esperaba %s, recibió '%s'", targetSeat, boardingPass.SeatAssigned)
		}

		// Esperar antes del siguiente pasajero para no saturar el log instantáneamente
		waitNextIteration()
	}
}

func waitNextIteration() {
	// Tiempo aleatorio entre 3 y 6 segundos
	delay := time.Duration(rand.Intn(4)+3) * time.Second
	// log.Printf("💤 Esperando %v para el siguiente pasajero...", delay)
	time.Sleep(delay)
}
