package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings" // Usado para strings.ToLower()
	"time"

	pb "lab2/proto" 
	
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- Constantes y Variables Globales ---

const brokerAddress = "localhost:50051"

var (
	entityID   = flag.String("id", "Riploy", "ID único de la entidad.")
	entityPort = flag.String("port", ":50052", "Puerto local del servidor gRPC del Productor.")
)

type ProductBase struct {
	Product   string
	Category  string
	BasePrice int64
}

// --- Funciones de Fase 1: Registro ---

func registerWithBroker(client pb.EntityManagementClient) {
	log.Printf("[%s] Starting registration with Broker...", *entityID)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	req := &pb.RegistrationRequest{
		EntityId:   *entityID,
		EntityType: "Producer",
		Address:    "localhost" + *entityPort,
	}

	resp, err := client.RegisterEntity(ctx, req)
	if err != nil {
		log.Fatalf("[%s] ❌ Could not connect or register with broker: %v", *entityID, err)
	}

	log.Printf("[%s] Broker Response: Success=%t, Message=%s", *entityID, resp.Success, resp.Message)

	if !resp.Success {
		os.Exit(1)
	}
}

// --- Lógica de Fase 2: Producción de Ofertas ---

// loadCatalog lee el archivo CSV del catálogo.
func loadCatalog(filename string) []ProductBase {
	catalog := []ProductBase{}
	log.Printf("[%s] Loading catalog from %s...", *entityID, filename)
	
	file, err := os.Open(filename)
	if err != nil {
		// Este error ahora debería ser raro si la ruta es correcta
		log.Fatalf("[%s] FATAL: Cannot open catalog file %s. Error: %v", *entityID, filename, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// ⚠️ CORRECCIÓN 2: El archivo tiene 6 campos, no 3. 
    // Lo configuramos para que espere el número correcto, o nos devuelva un error si es inconsistente.
	reader.FieldsPerRecord = 6 
	
	// Saltar la cabecera (Warning en log, no fatal)
	_, err = reader.Read() 
    if err != nil && err != io.EOF {
        log.Printf("[%s] Warning reading catalog header: %v", *entityID, err)
    }

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
            // Este error de 'wrong number of fields' ya no debería ocurrir si FieldsPerRecord = 6
			log.Fatalf("[%s] Error reading CSV record: %v", *entityID, err)
		}
        
        // Los campos relevantes en el array 'record' son (usando tu estructura):
        // [2]: categoria
        // [3]: producto
        // [4]: precio_base

        category := record[2]
        productName := record[3]

		price, err := strconv.ParseInt(record[4], 10, 64)
		if err != nil {
			log.Printf("[%s] Invalid base price '%s' for product '%s'. Skipping.", *entityID, record[4], productName)
			continue
		}

		catalog = append(catalog, ProductBase{
			Product:   productName,
			Category:  category,
			BasePrice: price,
		})
	}
    
    if len(catalog) == 0 {
        log.Fatalf("[%s] FATAL: Catalog is empty after reading. Cannot proceed.", *entityID)
    }

	log.Printf("[%s] Loaded %d products from catalog.", *entityID, len(catalog))
	return catalog
}

// generateOffer crea una nueva oferta (descuento 10-50%, stock > 0, UUID).
func generateOffer(base ProductBase, tienda string) *pb.Offer {
	// Descuento aleatorio entre 10% a 50%
	discount := float64(rand.Intn(41)+10) / 100.0
	newPrice := int64(float64(base.BasePrice) * (1.0 - discount))

	// Stock estrictamente mayor que cero (entre 1 y 100)
	stock := rand.Int31n(100) + 1 

	// Identificador único (UUIDv4)
	offerID := uuid.New().String()

	// Fecha correspondiente al momento exacto de su generación
	fecha := time.Now().Format("2006-01-02 15:04:05")

	return &pb.Offer{
		OfertaId:  offerID,
		Tienda:    tienda,
		Categoria: base.Category,
		Producto:  base.Product,
		Precio:    newPrice,
		Stock:     stock,
		Fecha:     fecha,
	}
}

// startOfferProduction maneja el ciclo continuo de envío de ofertas al Broker.
func startOfferProduction(catalog []ProductBase) {
	conn, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[%s] Failed to connect to broker for production: %v", *entityID, err)
	}
	defer conn.Close()

	client := pb.NewOfferSubmissionClient(conn)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	log.Printf("[%s] Starting continuous offer production...", *entityID)
	
    for {
		base := catalog[r.Intn(len(catalog))]
		offer := generateOffer(base, *entityID)

		// Enviar al Broker (Fase 2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		resp, err := client.SendOffer(ctx, offer)
		cancel()

		if err != nil {
			log.Printf("[%s] ❌ Error sending offer %s (Broker down?): %v", *entityID, offer.OfertaId, err)
		} else if resp.Accepted {
			log.Printf("[%s] ✅ Offer %s sent and ACCEPTED (P: %d, S: %d)", *entityID, offer.OfertaId, offer.Precio, offer.Stock)
		} else {
			log.Printf("[%s] ⚠️ Offer %s REJECTED by Broker: %s", *entityID, offer.OfertaId, resp.Message)
		}

		// Frecuencia de emisión: 1-2 segundos
		delay := time.Duration(r.Intn(1000)+1000) * time.Millisecond
		time.Sleep(delay)
	}
}

// --- Función Principal (Corregida) ---

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	// 1. Registro (Fase 1)
	connReg, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[%s] Did not connect to broker for registration: %v", *entityID, err)
	}
	defer connReg.Close()

	clientReg := pb.NewEntityManagementClient(connReg)
	registerWithBroker(clientReg)
    
	// 2. Carga del Catálogo y Producción de Ofertas (Fase 2)
	
    // ⚠️ CORRECCIÓN 1: Convierte el EntityID a minúsculas para buscar el archivo, 
    // ya que los nombres de archivo CSV están en minúsculas.
	lowerCaseID := strings.ToLower(*entityID) 
	catalogFile := fmt.Sprintf("Productor/catalogos/%s_catalogo.csv", lowerCaseID)
    
	catalog := loadCatalog(catalogFile)

	if len(catalog) > 0 {
		startOfferProduction(catalog) 
	} else {
		log.Fatalf("[%s] Could not start production: Catalog is empty.", *entityID)
	}
}