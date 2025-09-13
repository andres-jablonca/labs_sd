# Laboratorio 1 Sistemas Distribuidos Grupo 9
### Integrantes:
* Andrés Jablonca 202173578-K
* Walter Sanhueza 202023564-3
* Matías Pérez 202187515-8

## Tener en cuenta que la tarea completa se subió a cada máquina virtual, sin embargo cada entidad está pensada para ser ejecutada en una máquina virtual en específico.
### - dist033: Lester (IP: 10.35.168.43)
### - dist034: Michael (IP: 10.35.168.44)
### - dist035: Franklin (IP: 10.35.168.45)
### - dist036: Trevor (IP: 10.35.168.46)

## Instrucciones de compilación y ejecución

### Acceder a Maquina Virtual y luego a grupo-9/"Laboratorio 1"/{entidad} (entidad:= lester,michael,franklin,trevor según la máquina virtual en la que se esté trabajando. Tener en cuenta también que cada entidad tiene su propio makefile)

### Actualizar archivo go.sum por si acaso
make tidy-{entidad}

### Compilar archivo .proto por si acaso
make proto-{entidad}

### Levantar contenedores de las entidades en sus máquinas virtuales correspondientes
make docker-off
make docker-{entidad}

### La entidad Michael sólo se debe ejecutar cuando todas las demás entidades estén up, de lo contrario fallará al intentar realizar las comunicaciones

### Para ver reporte final una vez haya terminado la ejecución (solo disponible en dist034:Michael)
make ver-reporte

### Para dar de baja cada contenedor (en cada máquina virtual)
make docker-off

## Consideraciones y flujo del sistema

* Michael inicia comunicandose con Lester para solicitar ofertas y aceptar o rechazar según lo indicado en enunciado (En caso de ofertas invalidas tambien las rechaza). Lester a su vez también sigue las indicaciones del enunciado a la hora de entregar ofertas y enterarse de rechazos.
* Al aceptar una oferta, Michael se comunica con la entidad seleccionada para dar inicio a la fase 2 y como respuesta espera directamente el exito o fracaso de la mision (una sola comunicación grpc).
* La entidad trabajando en la fase 2 avanza un turno cada medio segundo, y a la mitad de turnos la misión puede fallar o no, lo que se le informa a Michael en ambos casos (esto correspondería a la interacción indicada en el diagrama de flujo para informar acerca del estado de la misión).
* Una vez terminada la fase 2, Michael avisa a la entidad restante para dar inicio a la fase 3 y a su vez avisa a Lester para enviar las notificaciones a dicha entidad, notificaciones las cuales empiezan en 0 estrellas.
* La entidad trabaja de la misma manera que en la fase 2 (un turno cada medio segundo) pero revisa notificaciones cada 5 turnos y Lester las envia según la frecuencia indicada en el enunciado. Se implementaron time.Sleeps para poder coordinar el conteo de turnos del trabajador con el conteo de turnos de Lester.
* Ambas entidades (Franklin/Trevor) activan sus habilidades según las condiciones indicadas en el enunciado sólo al enterarse mediante las notificaciones, no cuando Lester las envía, por lo que con Trevor ocurre lo siguiente: 
    1. Si se entera que ya hay 5 estrellas antes de terminar todos sus turnos, se activa su habilidad.
    2. Si no se entera que ya hay 5 estrellas pero ya terminó todos sus turnos, perderá ya que "no alcanzó a activar su habilidad".
* Ambos personajes perderán si:
    1. Se enteran de que llegaron a su limite de estrellas antes de terminar todos sus turnos.
    2. Terminaron todos sus turnos pero no se enteraron de que llegaron a su límite de estrellas (verificación final antes de enviar veredicto de la misión).
* Una vez finalizada la fase 3, Michael informa a Lester para detener el envío de notificaciones.
* En caso de haber perdido en la fase 2 o 3, Michael genera el reporte de fracaso correspondiente. En caso de haber completado ambas fases se reparte el botín según lo indicado en el enunciado, las entidades lo confirman y Michael procede a generar el reporte de éxito final.

## Cosas extras
* Se conservó el archivo ofertas_grande.csv dentro de la carpeta lester, en caso de querer que se lean las ofertas de otro archivo, este deberia estar en la misma ruta y el nombre del archivo se deberia cambiar dentro de lester.go
* Lester lee ofertas aleatorias dentro del archivo csv y las envia, incluso si estas son invalidas (falta uno o mas campos) y es Michael quien las invalida y rechaza, sin embargo la informacion de estas ofertas no se muestran por terminal.
* Las maquinas ya estan con los distintos archivos compilados y actualizados, por lo que con seguir las instrucciones de acceder a la carpeta de cada entidad y ejecutar el comando de make para levantar los contenedores debería bastar, ya que esto fue testeado varias veces