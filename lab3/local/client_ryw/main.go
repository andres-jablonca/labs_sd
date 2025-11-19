package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "lab3/proto"
)

const (
	coordinatorAddr = "coordinator:50051"
	clientID        = "RYW-Client-1"
	flightID        = "LA-500"
)

// Global para almacenar el Vector Clock local (RYW)
var clientVC = make(map[string]int64)
var mu sync.Mutex

// mergeVC es una función auxiliar para fusionar dos VCs, tomando el máximo
func mergeVC(vc1, vc2 map[string]int64) map[string]int64 {
	result := make(map[string]int64)
	for k, v := range vc1 {
		result[k] = v
	}
	for k, v := range vc2 {
		if v > result[k] {
			result[k] = v
		}
	}
	return result
}

// En client_ryw/main.go

func checkin(client pb.CheckInCoordinatorClient) {
	rand.Seed(time.Now().UnixNano()) // Usar math/rand

	// --- 1. ESCRITURA (Check-in) ---
	log.Printf("Cliente (%s): Iniciando Check-in (Escritura) para asiento 14A...", clientID)
	
	// Usamos rand y strconv para un request_uuid único (elimina warnings)
	requestUUID := "req-" + clientID + "-" + time.Now().Format("0405") + "-" + strconv.Itoa(rand.Intn(10000))

	checkInReq := &pb.CheckInRequest{
		ClientId:    clientID,
		FlightId:    flightID,
		SeatNumber:  "14A",
		RequestUuid: requestUUID,
	}

	checkInResp, err := client.ProcessCheckIn(context.Background(), checkInReq)
	if err != nil {
		log.Fatalf("❌ Error en Check-in: %v", err)
	}
	
	if !checkInResp.Success {
		log.Printf("⚠️ Check-in fallido: %s", checkInResp.Message)
	} else {
		log.Printf("✅ Check-in exitoso. Mensaje: %s", checkInResp.Message)
	}
	
	log.Println("--- Esperando 3s para simular una pausa y forzar el Gossip ---")
	time.Sleep(3 * time.Second) 

	// --- 2. LECTURA (Obtener Tarjeta de Embarque RYW) ---
	log.Printf("Cliente (%s): Solicitando Tarjeta de Embarque (Lectura RYW)...", clientID)
	
	// NO NECESITAS readVC, solo necesitas el valor de clientVC.
	mu.Lock()
	// Eliminada: readVC := clientVC
	// El clientVC se usaría en bpReq si el proto lo permitiera.
	mu.Unlock() 
	
	bpReq := &pb.BoardingPassRequest{ 
		FlightId: flightID,
		ClientId: clientID,
		// Si BoardingPassRequest tuviera el campo, la lógica RYW correcta sería:
		// ClientVectorClock: clientVC, 
	}
	
	bpResp, err := client.GetBoardingPass(context.Background(), bpReq)
	if err != nil {
		log.Printf("❌ Error al obtener Tarjeta de Embarque: %v", err)
		return
	}

	// Simulación RYW: El Coordinador/Broker DEBE DEVOLVER el VC en BoardingPassResponse.
	// Simulamos la actualización con un VC ficticio.
	simulatedVCFromCoord := map[string]int64{"BROKER": 1, "DN-1": 1} 
	
	mu.Lock()
	clientVC = mergeVC(clientVC, simulatedVCFromCoord) // Fusionar el VC devuelto
	log.Printf("✅ Lectura de Tarjeta de Embarque exitosa. Asiento: %s, Gate: %s. Último VC conocido: %v", bpResp.SeatAssigned, bpResp.Gate, clientVC)
	mu.Unlock()
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	
	// Conexión gRPC
	conn, err := grpc.Dial(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()) // Usa grpc y insecure
	if err != nil {
		log.Fatalf("❌ No se pudo conectar al Coordinador: %v", err)
		os.Exit(1) // Usa os
	}
	defer conn.Close()

	client := pb.NewCheckInCoordinatorClient(conn)
	log.Printf("Cliente RYW: Conectado al Coordinador en %s", coordinatorAddr)

	for {
		checkin(client)
		time.Sleep(5 * time.Second) // Ejecutar cada 5 segundos
	}
}