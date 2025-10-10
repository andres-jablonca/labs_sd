package main

import (
	"context"
	"fmt"
	"os"
	"time"

	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// Change these for each of the 12 consumers
	entityID      = "C-E1"
	entityPort    = ":50071" // Consumer's gRPC server port
	brokerAddress = "broker:50051"
)

func registerWithBroker(client pb.EntityManagementClient) {
	fmt.Printf("Coordinando el registro con el Broker...\n")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	req := &pb.RegistrationRequest{
		EntityId:   entityID,
		EntityType: "Consumer",
		Address:    "localhost" + entityPort,
	}

	resp, err := client.RegisterEntity(ctx, req)
	if err != nil {
		fmt.Printf("❌ No se logró conectar con el broker: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Respuesta del Broker: Éxito=%t, Mensaje=%s\n", resp.Success, resp.Message)

	if !resp.Success {
		os.Exit(1)
	}
}

func main() {
	// 1. Connect to the Broker
	conn, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("No se logró conexión con broker: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Create the client for the EntityManagement service
	client := pb.NewEntityManagementClient(conn)

	// 2. Perform Registration (Phase 1)
	registerWithBroker(client)

	fmt.Printf("Registro completo. Listo para recibir notificaciones (Fase 4).\n")

	// In Phase 4, the Consumer would start its gRPC server to receive notifications.
}
