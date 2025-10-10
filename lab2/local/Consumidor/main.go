package main

import (
	"context"
	"log"
	"os"
	"time"

	pb "lab2/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// Change these for each of the 12 consumers
	entityID = "C-E1" 
	entityPort = ":50071" // Consumer's gRPC server port
	brokerAddress = "localhost:50051"
)

func registerWithBroker(client pb.EntityManagementClient) {
	log.Printf("[%s] Starting registration with Broker...", entityID)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	req := &pb.RegistrationRequest{
		EntityId: entityID,
		EntityType: "Consumer",
		Address: "localhost" + entityPort,
	}

	resp, err := client.RegisterEntity(ctx, req)
	if err != nil {
		log.Fatalf("[%s] ❌ Could not connect or register with broker: %v", entityID, err)
	}
	
	log.Printf("[%s] Broker Response: Success=%t, Message=%s", entityID, resp.Success, resp.Message)

	if !resp.Success {
		os.Exit(1)
	}
}

func main() {
	// 1. Connect to the Broker
	conn, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[%s] Did not connect to broker: %v", entityID, err)
	}
	defer conn.Close()
	
	// Create the client for the EntityManagement service
	client := pb.NewEntityManagementClient(conn)

	// 2. Perform Registration (Phase 1)
	registerWithBroker(client)

	log.Printf("[%s] Registration complete. Ready for Notification (Phase 4).", entityID)
    
    // In Phase 4, the Consumer would start its gRPC server to receive notifications.
}