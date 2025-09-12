# Laboratorio 1 Sistemas Distribuidos Grupo 9
### Integrantes:
* Andrés Jablonca 202173578-K
* Walter Sanhueza
* Matías Pérez

### Tener en cuenta que la tarea completa se subió al repositorio correspondiente, sin embargo, por simplicidad sólo se subió una entidad a cada máquina virtual.
### * dist033: Lester
### * dist034: Michael
### * dist035: Franklin
### * dist036: Trevor

## DIST034 (IP: 10.35.168.44) - ENTIDAD: Michael

## Instrucciones de compilación y ejecución

### Acceder a Maquina Virtual y luego a /lab_sd_{entidad} (entidad:= lester,michael,franklin,trevor)

### Instalar go, proto y plugins

### Actualizar archivos go.sum
make tidy

### Compilar archivos .proto
make proto

### Levantar entidades (cada una por una terminal distinta):
make docker-off
make docker-{nombre_entidad}

En donde nombre entidad: {lester, franklin, trevor, michael}

### Cliente (Michael) sólo se debe ejecutar una vez todos los servidores estén up, de lo contrario al intentar establecer conexiones se caerá inmediatamente

### Para ver reporte final una vez haya terminado la ejecución (de Michael)
make ver-reporte

### Consideraciones y flujo del sistema

* Michael inicia comunicandose con Lester para solicitar ofertas y aceptar o rechazar según lo indicado en enunciado (En caso de ofertas invalidas tambien las rechaza). Lester a su vez también sigue las indicaciones del enunciado a la hora de entregar ofertas y enterarse de rechazos.
* Al aceptar una oferta, Michael se comunica con la entidad seleccionada para dar inicio a la fase 2 y como respuesta espera directamente el exito o fracaso de la mision (una sola comunicación grpc).
* La entidad trabajando en la fase 2 avanza un turno cada medio segundo, y a la mitad de turnos la misión puede fallar o no, lo que se le informa a Michael en ambos casos (esto correspondería a la interacción indicada en el diagrama de flujo para informar acerca del estado de la misión).
* Una vez terminada la fase 2, Michael avisa a la entidad restante para dar inicio a la fase 3 y a su vez avisa a Lester para enviar las notificaciones a la dicha entidad, notificaciones las cuales empiezan en 0 estrellas.
* La entidad trabaja de la misma manera que en la fase 2 (un turno cada medio segundo) pero revisa notificaciones cada 5 turnos y Lester las envia según la frecuencia indicada en el enunciado.
* Ambas entidades (Franklin/Trevor) activan sus habilidades según las condiciones indicadas en el enunciado sólo al enterarse mediante las notificaciones, no cuando Lester las envía, por lo que con Trevor ocurre lo siguiente: 
    1. Si se entera que ya hay 5 estrellas antes de terminar todos sus turnos, se activa su habilidad.
    2. Si no se entera que ya hay 5 estrellas pero ya terminó todos sus turnos, perderá ya que no alcanzó a activar su habilidad.
* Ambos personajes perderán si:
    1. Se enteran de que llegaron a su limite de estrellas antes de terminar todos sus turnos.
    2. Terminaron todos sus turnos pero no se enteraron de que llegaron a su límite de estrellas (verificación final antes de enviar veredicto de la misión).
* Una vez finalizada la fase 3, Michael informa a Lester para detener el envío de notificaciones.
* En caso de haber perdido en la fase 2 o 3, Michael genera el reporte de fracaso correspondiente. En caso de haber completado ambas fases se reparte el botín según lo indicado en el enunciado, las entidades lo confirman y Michael procede a generar el reporte de éxito final.
