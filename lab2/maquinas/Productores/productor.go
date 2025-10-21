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

const direccionBroker = "10.35.168.43:50095"

// Flags default (cambian con cada productor)

var (
	entityID   = flag.String("id", "Riploy", "ID único de la entidad.")
	entityPort = flag.String("port", ":50052", "Puerto local del servidor gRPC del Productor.")
)

// Categorías permitidas segun enunciado
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

// Producto que viene en catalogo
type ProductoBase struct {
	IDProducto string
	Producto   string
	Categoria  string
	PrecioBase int64
	StockBase  int32
}

// Colores para mayor diferenciacion en prints
const (
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Reset  = "\033[0m"
)

/*
Nombre: CategoriaPermitida
Parámetros: cat string
Retorno: bool
Descripción: Verifica si la categoría está permitida.
*/
func CategoriaPermitida(cat string) bool {
	_, ok := allowedCategories[cat]
	return ok
}

/*
Nombre: GenerarUUID
Parámetros: ninguno
Retorno: string
Descripción: Simula la generacion de un UUID basado en tiempo y aleatoriedad.
*/
func GenerarUUID() string {
	timestamp := time.Now().UnixNano()
	randomPart := rand.Intn(1000000)
	return fmt.Sprintf("%x-%x", timestamp, randomPart)
}

/*
Nombre: RegistrarConBroker
Parámetros: client pb.RegistroEntidadesClient
Retorno: ninguno
Descripción: Registra el productor en el broker con su dirección gRPC.
*/
func RegistrarConBroker(client pb.RegistroEntidadesClient) {
	fmt.Printf("Coordinando el registro con el Broker...\n")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	publicIP := os.Getenv("PUBLIC_IP")
	if publicIP == "" {
		fmt.Println("ERROR: Debes setear PUBLIC_IP en el contenedor del productor")
		os.Exit(1)
	}
	addressToRegister := publicIP + *entityPort

	req := &pb.SolicitudRegistro{
		IdEntidad:   *entityID,
		TipoEntidad: "Producer",
		Direccion:   addressToRegister,
	}

	resp, err := client.RegistrarEntidad(ctx, req)
	if err != nil {
		fmt.Printf("No se logró conectar con el broker: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Respuesta del Broker: Éxito=%t, Mensaje=%s\n", resp.GetExito(), resp.GetMensaje())

	if !resp.GetExito() {
		os.Exit(1)
	}
}

/*
Nombre: CargarCatalogo
Parámetros: filename string
Retorno: []ProductoBase
Descripción: Carga catálogo CSV y filtra por categorías válidas.
*/
func CargarCatalogo(filename string) []ProductoBase {
	catalog := []ProductoBase{}
	fmt.Printf("Cargando catalogo desde %s...\n", filename)

	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("No se pudo abrir el archivo del catalogo %s. Error: %v\n", filename, err)
		os.Exit(1)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 6

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

		IDproducto := strings.TrimSpace(record[0])
		if IDproducto == "" {
			fmt.Printf("Fila con IDproducto vacío. Skipeando.\n")
			continue
		}

		categoria := record[2]
		productName := record[3]

		if !CategoriaPermitida(categoria) {
			fmt.Printf(Yellow+"[SKIP] Producto '%s' ignorado por categoría no permitida: '%s'\n"+Reset, productName, record[2])
			continue
		}

		precio, err := strconv.ParseInt(record[4], 10, 64)
		if err != nil {
			fmt.Printf("Precio base inválido '%s' para el producto '%s'. Skipeando.\n", record[4], productName)
			continue
		}

		stock, err := strconv.ParseInt(record[5], 10, 64)
		if err != nil {
			fmt.Printf("Stock base inválido '%s' para el producto '%s'. Skipeando.\n", record[5], productName)
			continue
		}
		if stock <= 0 {
			fmt.Printf("Stock base no positivo '%s' para el producto '%s'. Skipeando.\n", record[5], productName)
			continue
		}

		catalog = append(catalog, ProductoBase{
			IDProducto: IDproducto,
			Producto:   productName,
			Categoria:  categoria,
			PrecioBase: precio,
			StockBase:  int32(stock),
		})
	}

	if len(catalog) == 0 {
		fmt.Printf(Red + "Catálogo sin ítems válidos en categorías permitidas. Abortando.\n" + Reset)
		os.Exit(1)
	}

	fmt.Printf("Se cargaron %d productos válidos (categorías permitidas).\n", len(catalog))
	return catalog
}

/*
Nombre: GenerarOferta
Parámetros: base ProductoBase, tienda string
Retorno: *pb.Oferta
Descripción: Genera una nueva oferta con descuento y stock aleatorios (entre 10-50 y mayor a 0, respectivamente). Ademas agrega el id del producto base y lo junta con el uuid generado
*/
func GenerarOferta(base ProductoBase, tienda string) *pb.Oferta {
	descuento := float64(rand.Intn(41)+10) / 100.0
	nuevoPrecio := int64(float64(base.PrecioBase) * (1.0 - descuento))
	stock := rand.Int31n(base.StockBase) + 1
	ofertaID := fmt.Sprintf("%s-%s", base.IDProducto, GenerarUUID())
	fecha := time.Now().Format("2006-01-02 15:04:05")

	return &pb.Oferta{
		OfertaId:  ofertaID,
		Tienda:    tienda,
		Categoria: base.Categoria,
		Producto:  base.Producto,
		Precio:    nuevoPrecio,
		Stock:     stock,
		Fecha:     fecha,
		Descuento: float32(descuento),
	}
}

/*
Nombre: IniciarProduccionOfertas
Parámetros: catalog []ProductoBase
Retorno: ninguno
Descripción: Envía ofertas periódicamente al broker (2s) y detecta fin del evento cuando ya llega al limite de ofertas.
*/
func IniciarProduccionOfertas(catalog []ProductoBase) {
	conn, err := grpc.Dial(direccionBroker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("Fallo al conectar con broker para producción de ofertas: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewOfertasClient(conn)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	fmt.Printf("Iniciando producción de ofertas...\n")
	for {
		base := catalog[r.Intn(len(catalog))]

		if !CategoriaPermitida(base.Categoria) {
			fmt.Printf(Yellow+"[SKIP-RUNTIME] Categoría '%s' no permitida para '%s'\n"+Reset, base.Categoria, base.Producto)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		offer := GenerarOferta(base, *entityID)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		resp1, err := client.EnviarOferta(ctx, offer)
		cancel()
		if err != nil {
			fmt.Printf("Error enviando oferta %s (Broker caído?): %v\n", offer.GetOfertaId(), err)
		} else if resp1.GetAceptado() {
			fmt.Printf("Oferta %s de Producto %q con descuento %.2f enviada y ACEPTADA (Precio: %d, Stock: %d)\n",
				offer.GetOfertaId(), offer.GetProducto(), offer.GetDescuento(), offer.GetPrecio(), offer.GetStock())
		}

		if resp1 != nil && resp1.GetTermino() {
			fmt.Printf("Cyberday Finalizado\n")
			break
		}
		time.Sleep(2 * time.Second)
	}
}

/*
Nombre: main
Parámetros: ninguno
Retorno: ninguno
Descripción: Registra el productor, espera READY del broker y envía ofertas.
*/
func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	connReg, err := grpc.Dial(direccionBroker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("No se logró conectar con broker para el registro: %v\n", err)
		os.Exit(1)
	}
	defer connReg.Close()

	clientReg := pb.NewRegistroEntidadesClient(connReg)
	RegistrarConBroker(clientReg)

	clientConf := pb.NewConfirmarInicioClient(connReg)
	time.Sleep(2 * time.Second)
	for {
		req := &pb.SolicitudInicio{}
		resp, err := clientConf.Confirmacion(context.Background(), req)
		if err != nil {
			fmt.Printf("No se logró conectar con el broker: %v\n", err)
			os.Exit(1)
		}
		if resp.GetListo() {
			fmt.Println("Broker READY. ¡Comenzando a enviar ofertas!")
			break
		}
		fmt.Println("Broker NO READY. Esperando 5 segundos...")
		time.Sleep(5 * time.Second)
	}

	lowerCaseID := strings.ToLower(*entityID)
	archivoCatalogo := fmt.Sprintf("Productores/catalogos/%s_catalogo.csv", lowerCaseID)

	catalogo := CargarCatalogo(archivoCatalogo)
	IniciarProduccionOfertas(catalogo)

	fmt.Printf(Green+"%s Cerrando tienda \n"+Reset, *entityID)
}
