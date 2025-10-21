package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- Constantes y Variables Globales ---

const brokerAddress = "broker:50095"

var (
	entityID   = flag.String("id", "Riploy", "ID único de la entidad.")
	entityPort = flag.String("port", ":50052", "Puerto local del servidor gRPC del Productor.")
)

// Categorías permitidas (match exacto tras normalización)
var allowedCategories = map[string]struct{}{
	"Electrónica":       {},
	"Moda":              {},
	"Hogar":             {},
	"Deportes":          {},
	"Belleza":           {},
	"Infantil":          {},
	"Computación":       {},
	"Electrodomésticos": {},
	"Herramientas":      {},
	"Juguetes":          {},
	"Automotriz":        {},
	"Mascotas":          {},
}

type ProductBase struct {
	Product   string
	Category  string
	BasePrice int64
	BaseStock int32
}

const (
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Reset  = "\033[0m"
)

// Normaliza categorías: quita bullets y espacios
func normalizeCategory(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "•") {
		s = strings.TrimSpace(strings.TrimPrefix(s, "•"))
	}
	return s
}

func isAllowedCategory(cat string) bool {
	_, ok := allowedCategories[normalizeCategory(cat)]
	return ok
}

// -----------------------------------------------------------------------
// 💡 FUNCIÓN DE GENERACIÓN DE ID SIN LIBRERÍA UUID
// -----------------------------------------------------------------------
func generatePseudoUUID() string {
	timestamp := time.Now().UnixNano()
	randomPart := rand.Intn(1000000)
	return fmt.Sprintf("%x-%x", timestamp, randomPart)
}

// --- Funciones de Fase 1: Registro ---

func registerWithBroker(client pb.EntityManagementClient) {
	fmt.Printf("Coordinando el registro con el Broker...\n")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	dockerServiceName := strings.ToLower(*entityID)
	addressToRegister := dockerServiceName + *entityPort

	req := &pb.RegistrationRequest{
		EntityId:   *entityID,
		EntityType: "Producer",
		Address:    addressToRegister,
	}

	resp, err := client.RegisterEntity(ctx, req)
	if err != nil {
		fmt.Printf("No se logró conectar con el broker: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Respuesta del Broker: Éxito=%t, Mensaje=%s\n", resp.Success, resp.Message)

	if !resp.Success {
		os.Exit(1)
	}
}

// --- Lógica de Fase 2: Producción de Ofertas ---

// loadCatalog lee el archivo CSV del catálogo y filtra por categorías permitidas.
func loadCatalog(filename string) []ProductBase {
	catalog := []ProductBase{}
	fmt.Printf("Cargando catalogo desde %s...\n", filename)

	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("No se pudo abrir el archivo del catalogo %s. Error: %v\n", filename, err)
		os.Exit(1)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 6

	// Saltar la cabecera
	_, err = reader.Read()
	if err != nil && err != io.EOF {
		fmt.Printf("Warning leyendo el header del catalogo: %v\n", err)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error leyendo contenido del CSV: %v\n", err)
			os.Exit(1)
		}

		category := normalizeCategory(record[2])
		productName := record[3]

		if !isAllowedCategory(category) {
			fmt.Printf(Yellow+"[SKIP] Producto '%s' ignorado por categoría no permitida: '%s'\n"+Reset, productName, record[2])
			continue
		}

		price, err := strconv.ParseInt(record[4], 10, 64)
		if err != nil {
			fmt.Printf("Precio base inválido '%s' para el producto '%s'. Skipeando.\n", record[4], productName)
			continue
		}

		stockParsed, err := strconv.ParseInt(record[5], 10, 64)
		if err != nil {
			fmt.Printf("Stock base inválido '%s' para el producto '%s'. Skipeando.\n", record[5], productName)
			continue
		}
		if stockParsed <= 0 {
			fmt.Printf("Stock base no positivo '%s' para el producto '%s'. Skipeando.\n", record[5], productName)
			continue
		}

		catalog = append(catalog, ProductBase{
			Product:   productName,
			Category:  category, // ya normalizada
			BasePrice: price,
			BaseStock: int32(stockParsed),
		})
	}

	if len(catalog) == 0 {
		fmt.Printf(Red + "Catálogo sin ítems válidos en categorías permitidas. Abortando.\n" + Reset)
		os.Exit(1)
	}

	fmt.Printf("Se cargaron %d productos válidos (categorías permitidas).\n", len(catalog))
	return catalog
}

// generateOffer crea una nueva oferta (descuento 10-50%, stock > 0, ID generado).
func generateOffer(base ProductBase, tienda string) *pb.Offer {
	// Descuento aleatorio entre 10% a 50%
	discount := float64(rand.Intn(41)+10) / 100.0
	newPrice := int64(float64(base.BasePrice) * (1.0 - discount))

	// Stock estricto > 0 (hasta BaseStock)
	stock := rand.Int31n(base.BaseStock) + 1

	// Identificador pseudo-único
	offerID := generatePseudoUUID()

	// Fecha en el momento de generación
	fecha := time.Now().Format("2006-01-02 15:04:05")

	return &pb.Offer{
		OfertaId:  offerID,
		Tienda:    tienda,
		Categoria: base.Category, // ya verificada/permitida
		Producto:  base.Product,
		Precio:    newPrice,
		Stock:     stock,
		Fecha:     fecha,
		Descuento: float32(discount),
	}
}

// startOfferProduction maneja el ciclo continuo de envío de ofertas al Broker.
func startOfferProduction(catalog []ProductBase) {
	conn, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("Fallo al conectar con broker para producción de ofertas: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewOfferSubmissionClient(conn)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	fmt.Printf("Iniciando producción de ofertas...\n")
	for {
		base := catalog[r.Intn(len(catalog))]

		// Seguridad adicional: si por alguna razón llegara una categoría no permitida, se salta
		if !isAllowedCategory(base.Category) {
			fmt.Printf(Yellow+"[SKIP-RUNTIME] Categoría '%s' no permitida para '%s'\n"+Reset, base.Category, base.Product)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		offer := generateOffer(base, *entityID)

		// Enviar al Broker (Fase 2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		resp1, err := client.SendOffer(ctx, offer)
		cancel()
		if err != nil {
			fmt.Printf("Error enviando oferta %s (Broker caído?): %v\n", offer.OfertaId, err)
		} else if resp1.Accepted {
			fmt.Printf("Oferta %s de Producto **%s** con descuento de %f enviada y ACEPTADA (Precio: %d, Stock: %d)\n",
				offer.OfertaId, offer.Producto, offer.Descuento, offer.Precio, offer.Stock)
		}

		if resp1 != nil && resp1.GetTermino() {
			fmt.Printf("Cyberday Finalizado\n")
			break
		}
		time.Sleep(2 * time.Second)
	}
}

// --- Función Principal ---

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	// 1. Registro (Fase 1)
	connReg, err := grpc.Dial(brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("No se logró conectar con broker para el registro: %v\n", err)
		os.Exit(1)
	}
	defer connReg.Close()

	clientReg := pb.NewEntityManagementClient(connReg)
	registerWithBroker(clientReg)

	clientConf := pb.NewConfirmarInicioClient(connReg)
	for {
		req := &pb.ConfirmRequest{}
		resp, err := clientConf.Confirmacion(context.Background(), req)
		if err != nil {
			fmt.Printf("No se logró conectar con el broker: %v\n", err)
			os.Exit(1)
		}
		if resp.GetReady() {
			fmt.Println("Broker READY. ¡Comenzando a enviar ofertas!")
			break
		}
		fmt.Println("Broker NO READY. Esperando 5 segundos antes de volver a preguntar...")
		time.Sleep(5 * time.Second)
	}

	// 2. Carga del Catálogo y Producción de Ofertas (Fase 2)
	lowerCaseID := strings.ToLower(*entityID)
	catalogFile := fmt.Sprintf("Productores/catalogos/%s_catalogo.csv", lowerCaseID)

	catalog := loadCatalog(catalogFile)
	startOfferProduction(catalog)

	fmt.Printf("%s Cerrando tienda \n", *entityID)
}
