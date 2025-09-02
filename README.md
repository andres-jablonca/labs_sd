# labs_sd
trabaja mati

## Version actualizada con entidades independientes

### Compilar proto
make proto

### Para correr todos los contenedores por la misma consola:
docker compose down
docker compose up -- build

## Para correr contenedores por terminales separadas:
make docker-{nombre_entidad}

En donde nombre entidad: {lester, franklin, trevor, michael}

### Michael siempre se debe ejecutar una vez todos los servidores estén up, de lo contrario al intentar comunicarse con un servidor que no esta up, va a terminar inmediatamente