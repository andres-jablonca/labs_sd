# labs_sd
trabaja mati

## Version actualizada con entidades independientes

## Instalar go, proto y plugins

### Actualizar go.sum
make tidy

### Compilar proto
make proto

## Levantar entidades por terminales separadas:
make docker-down
make docker-{nombre_entidad}

En donde nombre entidad: {lester, franklin, trevor, michael}

### Michael siempre se debe ejecutar una vez todos los servidores estén up, de lo contrario al intentar comunicarse con un servidor que no esta up, va a terminar inmediatamente