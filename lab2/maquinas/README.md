# Laboratorio 1 Sistemas Distribuidos Grupo 9
### Integrantes:
* Andrés Jablonca 202173578-K
* Walter Sanhueza 202023564-3
* Matías Pérez 202187515-8

## Tener en cuenta que la tarea completa se subió a cada máquina virtual, sin embargo ciertas entidades están pensadas para ser ejecutada en una máquina virtual en específico.
### - dist033: Broker (IP: 10.35.168.43) (mv1)
### - dist034: BD1, Consumidor1, Riploy (IP: 10.35.168.44) (mv2)
### - dist035: BD2, Consumidor2, Falabellox (IP: 10.35.168.45) (mv3)
### - dist036: BD3, Consumidor3, Parisio (IP: 10.35.168.46) (mv4)

## Instrucciones de compilación y ejecución
### Acceder a Máquina Virtual y luego a grupo-9/"Laboratorio 2"/

### Actualizar archivo go.sum por si acaso
make tidy

### Compilar archivo .proto por si acaso
make proto

### Levantar contenedores de las entidades en sus máquinas virtuales correspondientes
make docker-down
make docker-{mv1,mv2,mv3,mv4}

### La mv1 (broker) debe ser levantada antes que todas las demás, ya que estas dependen del broker

### Para ver reporte final una vez se haya generado (solo disponible en dist033:mv1:Broker)
make ver-reporte

### Para dar de baja los contenedores (en cada máquina virtual)
make docker-down

## Consideraciones y flujo del sistema

-El flujo del sistema simula un evento de CyberDay tolerante a fallos y consistente, donde los productores envían ofertas cada 2 segundos con un ID único al Broker. Este asegura la Idempotencia descartando duplicados por ID, luego coordina la escritura distribuida en N=3 DB y exige la confirmación de al menos 2 escrituras (W=2). Tras la escritura exitosa el Broker filtra la oferta según las preferencias y la notifica a los consumidores relevantes los cuales verifican igualmente que la oferta corresponda a sus preferencias. Si alguna entidad cae, el sistema continúa trabajando siempre y cuando se asegure W=2 y R=2, si un consumidor se recupera después de una caída, este se comunica con el Broker para que le envíe el historial ya filtrado por sus preferencias. Esto ocurre hasta alcanzar los límites de ofertas el cual se definió como constante para que haya un punto de término y la ejecución no sea infinita, por lo que una vez llegado el límite, el broker notifica el fin a todas las entidades para que terminen y así poder generar el reporte final, el cual incluye las metricas recopiladas a lo largo de toda la ejecucion del sistema.

## Con respecto al laboratorio entregado se tomaron las siguientes consideraciones:

* Las BDs y los Consumidores tienen una probabilidad de caerse (20%), por lo que puede haber casos donde ninguno se falle, ya que el sistema no se asegura de que por cada iteración haya caídas. Esto fue tratado así por lo mencionado en la ayudantía online y dado un ejemplo de reporte que indica que puede ocurrir que no hayan caídas.
* Se asume que no está disponible la librería uuid, por lo que no se implementó el identificador con un UUID v4. Debido a lo anterior se propuso simular la generacion de un UUID concatenando la marca de tiempo (en nanosegundos) y un número aleatorio. Si bien lo anterior no corresponde al estándar UUID v4 las ID generadas si son únicas.
* Las BDD se sincronizan entre ellas cuando se produce un fallo, por lo tanto, nuestro Broker no tiene forma de llevar un contador de ello. 
* Todas las entidades finalizan su ejecución en un momento dado por el broker.
* Las tiendas comienzan a mandar ofertas solo cuando el broker logra registrar a todas las entidades necesarias (18 entidades)
* La cantidad de ofertas recibidas para cada consumidor mencionadas en el reporte final, corresponden solo a aquellas que el broker le envió directamente apenas las recibió, no se incluyen aquellas ofertas recuperadas tras una resincronización (por ejemplo, puede darse que en el informe se muestre que recibio 3 ofertas pero en el csv hayan 6, en donde 3 de esas se recuperaron tras una caida-resincronización)
* Se incluyen varias posibles conclusiones las cuales se eligen en base a distintas combinaciones de las metricas finales que nosotros consideramos adecuadas con respecto a la tolerancia a fallos y la consistencia del sistema.
 
## Puntos claves:

* Los nodos DB siempre devuelven ACK tras un intento de almacenar oferta. BDs pueden entregar historial completo a Broker o entre ellas mismas. Si una DB se cae, el sistema puede seguir funcionando, y al reincorporarse solicita el historial a las demás DB para consistencia eventual. 
* Broker valida y registra a las entidades correspondientes. Se encarga de almacenar las preferencias de los consumidores y distribuir correctamente las ofertas. Se asegura de diferenciar escrituras y lecturas exitosas de fallidas al verificar W=2 tras intentar almacenar oferta y R=2 tras solicitar historiales consistentes. Genera reporte final al finalizar el sistema, incluyendo todas las metricas relevantes que se registraron durante la ejecucion (se incluyen en base a los ejemplos subidos y en base al mismo enunciado), ademas de generar una conclusion la cual depende de las metricas registradas.
* Productores generan ofertas dinamicas cada 2s aleatoriamente a partir de su catalogo asegurandose de cumplir con lo solicitado en enunciado. Las ofertas tienen todos los campos requeridos ademas de id unico (uuid simulado) lo que permite descartar duplicados.
* Consumidores se encargan de verificar ofertas recibidas en base a sus preferencias, y luego de una caida se encarga de solicitar resincronizacion al broker para mantenerse al dia.
* Se hace uso exclusivo de GRPC Y Protocol Buffers. Entidades generan prints para cada posible evento ocurrido. Cada entidad se levanta en un contenedor distinto (4 mini consumidores de un grupo se considera una sola entidad, por lo que los 4 se levantan en el mismo contenedor)
