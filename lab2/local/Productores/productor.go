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

	// La librería "github.com/google/uuid" ha sido removida
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- Constantes y Variables Globales ---

const brokerAddress = "broker:50095"

var (
	entityID   = flag.String("id", "Riploy", "ID único de la entidad.")
	entityPort = flag.String("port", ":50052", "Puerto local del servidor gRPC del Productor.")
)

type ProductBase struct {
	Product   string
	Category  string
	BasePrice int64
}

// -----------------------------------------------------------------------
// 💡 FUNCIÓN DE GENERACIÓN DE ID SIN LIBRERÍA UUID
// Crea un ID pseudo-único concatenando la marca de tiempo en nanosegundos
// (que es altamente única) y un número aleatorio para mayor garantía.
// -----------------------------------------------------------------------
func generatePseudoUUID() string {
	// 1. Marca de tiempo en nanosegundos (altamente única)
	timestamp := time.Now().UnixNano()

	// 2. Número aleatorio de 6 dígitos
	// Se usa rand.Intn(1000000) para obtener un número entre 0 y 999999.
	randomPart := rand.Intn(1000000)

	// 3. Concatenar y formatear como string
	// El formato hexadecimal es similar a un UUID y es conciso.
	// Usamos fmt.Sprintf ya que fmt está permitido.
	return fmt.Sprintf("%x-%x", timestamp, randomPart)
}

// --- Funciones de Fase 1: Registro ---

func registerWithBroker(client pb.EntityManagementClient) {
	fmt.Printf("Coordinando el registro con el Broker...\n")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// ⚠️ Importante: Corregir la dirección para usar el nombre del servicio Docker
	// ya que "localhost" causaría "connection refused" en la red Docker.
	dockerServiceName := strings.ToLower(*entityID)
	addressToRegister := dockerServiceName + *entityPort

	req := &pb.RegistrationRequest{
		EntityId:   *entityID,
		EntityType: "Producer",
		Address:    addressToRegister,
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

// --- Lógica de Fase 2: Producción de Ofertas ---

// loadCatalog lee el archivo CSV del catálogo.
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

		category := record[2]
		productName := record[3]

		price, err := strconv.ParseInt(record[4], 10, 64)
		if err != nil {
			fmt.Printf("Precio base inválido '%s' para el producto '%s'. Skipeando.\n", record[4], productName)
			continue
		}

		catalog = append(catalog, ProductBase{
			Product:   productName,
			Category:  category,
			BasePrice: price,
		})
	}

	if len(catalog) == 0 {
		fmt.Printf("Catalogo vacío. No se puede seguir.\n")
		os.Exit(1)
	}

	fmt.Printf("Se cargaron %d productos desde el catalogo.\n", len(catalog))
	return catalog
}

// generateOffer crea una nueva oferta (descuento 10-50%, stock > 0, ID generado).
func generateOffer(base ProductBase, tienda string) *pb.Offer {
	// Descuento aleatorio entre 10% a 50%
	discount := float64(rand.Intn(41)+10) / 100.0
	newPrice := int64(float64(base.BasePrice) * (1.0 - discount))

	// Stock estrictamente mayor que cero (entre 1 y 100)
	stock := rand.Int31n(100) + 1

	// Identificador único (sustitución de UUID)
	offerID := generatePseudoUUID()

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
		offer := generateOffer(base, *entityID)

		// Enviar al Broker (Fase 2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		resp, err := client.SendOffer(ctx, offer)
		cancel()

		if err != nil {
			fmt.Printf("❌ Error enviando oferta %s (Broker caído?): %v\n", offer.OfertaId, err)
		} else if resp.Accepted {
			// 💡 CORRECCIÓN: Usar los marcadores de formato correctos.
			// Producto (string) -> %s
			// Precio (int64) -> %d
			// Stock (int32) -> %d

			fmt.Printf("✅ Oferta %s de Producto **%s** con descuento de %f enviada y ACEPTADA (P: %d, S: %d)\n", offer.OfertaId, offer.Producto, offer.Descuento, offer.Precio, offer.Stock)
		} else {
			fmt.Printf("⚠️ Oferta %s RECHAZADA por Broker: %s\n", offer.OfertaId, resp.Message)
		}

		if resp.GetTermino() {
			fmt.Printf("Cyberday Finalizado\n")
			break
		}
		// Frecuencia de emisión: 5 segundos
		delay := 5 * time.Second
		time.Sleep(delay)
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
			fmt.Printf("❌ No se logró conectar con el broker: %v\n", err)
			os.Exit(1)
			continue
		}
		if resp.GetReady() {
			fmt.Println("✅ Broker READY. ¡Comenzando a enviar ofertas!")
			break
		}
		fmt.Println("💤 Broker NO READY. Esperando 5 segundos antes de volver a preguntar...")
		time.Sleep(5 * time.Second)
	}

	// 2. Carga del Catálogo y Producción de Ofertas (Fase 2)
	lowerCaseID := strings.ToLower(*entityID)
	catalogFile := fmt.Sprintf("Productores/catalogos/%s_catalogo.csv", lowerCaseID)

	catalog := loadCatalog(catalogFile)

	if len(catalog) > 0 {
		startOfferProduction(catalog)
	} else {
		fmt.Printf("No se pudo iniciar producción: Catalogo está vacío.\n")
		os.Exit(1)
	}
	fmt.Printf("%s Cerrando tienda \n", *entityID)
}
