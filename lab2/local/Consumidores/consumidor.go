package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const brokerAddress = "broker:50051"

var (
	// Change these for each of the 12 consumers
	entityID   = flag.String("id", "C1-1", "ID único del consumidor.")
	entityPort = flag.String("port", ":50071", "Puerto local del servidor gRPC del Consumidor.")
)

func registerWithBroker(client pb.EntityManagementClient) {
	fmt.Printf("[%s] Coordinando el registro con el Broker...\n", *entityID)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	req := &pb.RegistrationRequest{
		EntityId:   *entityID,
		EntityType: "Consumer",
		Address:    "localhost" + *entityPort,
	}

	resp, err := client.RegisterEntity(ctx, req)
	if err != nil {
		fmt.Printf("[%s] ❌ No se logró conectar con el broker: %v\n", *entityID, err)
		os.Exit(1)
	}

	fmt.Printf("[%s] Respuesta del Broker: %s\n", *entityID, resp.Message)

	if !resp.Success {
		os.Exit(1)
	}
}

func main() {
	flag.Parse()

	// 1. Connect to the Broker
	conn, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[%s] No se logró conexión con broker: %v\n", *entityID, err)
		os.Exit(1)
	}
	defer conn.Close()

	// Create the client for the EntityManagement service
	client := pb.NewEntityManagementClient(conn)

	// 2. Perform Registration (Phase 1)
	registerWithBroker(client)

	fmt.Printf("[%s] Registro completo. Listo para recibir notificaciones.\n", *entityID)

	fmt.Printf("[%s] Esperando ofertas...\n", *entityID)
	time.Sleep(20 * time.Second)
}
