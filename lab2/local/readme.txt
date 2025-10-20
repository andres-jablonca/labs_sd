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

### Acceder a Maquina Virtual y luego a grupo-9/"Laboratorio 2"/

### Actualizar archivo go.sum por si acaso
make tidy

### Compilar archivo .proto por si acaso
make proto

### Levantar contenedores de las entidades en sus máquinas virtuales correspondientes
make docker-down
make docker-{mv1,mv2,mv3,mv4}

### La mv1 (broker) debe ser levantada antes que todas las demás, ya que estas dependen del broker

### Para ver reporte final una vez se haya generado (solo disponible en dist033:mv1:Broker)
Ctrl+C (para detener ejecucion del broker)
make ver-reporte

### Para dar de baja cada contenedor (en cada máquina virtual)
make docker-down

## Consideraciones y flujo del sistema


## Cosas extras
* Broker es quien lee consumidores.csv y filtra según las preferencias
* Dado que libreria UUID no se encuentra dentro de las librerias permitidas, pero se pide en la rubrica, se implementó una funcion que simula crear ids del tipo UUID
