package main

import (
	"context"
	"log"
	"net"
	"os"

	pbConsensus "lab3/proto"

	"google.golang.org/grpc"
)

// ConsensusServer implementa el servicio de Consenso (Raft/Paxos simplificado)
type ConsensusServer struct {
	pbConsensus.UnimplementedConsensusServiceServer
	nodeID string
	// Estado interno del nodo: log, estado (líder, follower, candidate), término
	isLeader    bool
	currentTerm int64
}

// RequestRunwayAssignment: Solicitud de toma de decisión crítica desde el Broker
func (s *ConsensusServer) RequestRunwayAssignment(ctx context.Context, req *pbConsensus.RunwayAssignmentRequest) (*pbConsensus.RunwayAssignmentResponse, error) {
	log.Printf("Recibida solicitud de asignación de pista para vuelo %s", req.GetFlightId())

	// Lógica de Fase 4: Verificar si soy el líder. Si no, redirigir o fallar.
	if !s.isLeader {
		return &pbConsensus.RunwayAssignmentResponse{
			Accepted: false,
			Message:  "No soy el líder. Por favor, contacte al líder (ID_LIDER)",
			LeaderId: "ID_LIDER_PENDIENTE",
		}, nil
	}

	// Lógica de Fase 4: Propagar la solicitud al log, esperar quorum.
	assignedRunway := "Pista_" + req.GetRequestedRunway() // Placeholder de asignación

	return &pbConsensus.RunwayAssignmentResponse{
		Accepted:       true,
		AssignedRunway: assignedRunway,
		LeaderId:       s.nodeID,
	}, nil
}

// RequestVote: Implementación básica de la llamada de voto de Raft/Paxos
func (s *ConsensusServer) RequestVote(ctx context.Context, req *pbConsensus.RequestVoteRequest) (*pbConsensus.RequestVoteResponse, error) {
	// Lógica de Fase 4: Votar o no votar basado en el término y el log
	if req.GetTerm() > s.currentTerm {
		s.currentTerm = req.GetTerm()
		// Votar sí (placeholder)
		return &pbConsensus.RequestVoteResponse{Term: s.currentTerm, VoteGranted: true}, nil
	}
	// Votar no (placeholder)
	return &pbConsensus.RequestVoteResponse{Term: s.currentTerm, VoteGranted: false}, nil
}

// AppendEntries: Implementación básica de la replicación de log de Raft/Paxos
func (s *ConsensusServer) AppendEntries(ctx context.Context, req *pbConsensus.AppendEntriesRequest) (*pbConsensus.AppendEntriesResponse, error) {
	// Lógica de Fase 4: Replicar log y responder al líder
	log.Printf("Recibido Heartbeat/Log del líder %s en el término %d", req.GetLeaderId(), req.GetTerm())
	return &pbConsensus.AppendEntriesResponse{Term: s.currentTerm, Success: true}, nil
}

func main() {
	// Cada nodo de Consenso necesita un ID único y un puerto propio
	nodeID := os.Getenv("NODE_ID") // Se usará en Docker/Makefile
	if nodeID == "" {
		nodeID = "Consensus_1"
	}
	const consensusPort = ":6001"

	lis, err := net.Listen("tcp", consensusPort)
	if err != nil {
		log.Fatalf("Fallo al escuchar en puerto %s: %v", consensusPort, err)
	}

	s := grpc.NewServer()

	consensusServer := &ConsensusServer{
		nodeID:      nodeID,
		isLeader:    true, // Asumimos que el primero es el líder por simplicidad en Fase 1
		currentTerm: 1,
	}

	pbConsensus.RegisterConsensusServiceServer(s, consensusServer)

	log.Printf("Nodo de Consenso (%s) escuchando en %s. Inicialmente Líder: %v", nodeID, consensusPort, consensusServer.isLeader)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Fallo al servir: %v", err)
	}
}
