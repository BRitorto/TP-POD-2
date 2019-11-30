# TP-POD-2

# Sistema de consulta de información acerca de vuelos

Trabajo practico especial 2 realizado para la materia Programacion de Objetos Distribuidos.

# Instrucciones

Instalar Maven

	$ sudo apt-get install maven

## Configurar interface de Hazelcast

En el archivo de configuración de hazelcast (**hazelcast.xml**), dentro de la carpeta **rmi**, asegurarse que en la línea **65**, la red que está configurada coincida con la red de la computadora que se está usando para correr el programa. Por default, está configurada así:

	$ <interface>192.*.*.*</interface>

## Buildear el proyecto

Pararse dentro de la carpeta /rmi y correr el script:

	$ ./build.sh

## Correr el servidor (nodo del cluster)

Dentro de la misma carpeta, correr el script:

	$ ./run-server.sh

Para agregar más nodos al cluster, realizar este paso otra vez desde otra terminal.

## Ejecutar las queries

Para ejecutar cada query se tiene que parar en la carpeta descomprimida dentro de: **/client/target/rmi-client-1.0-SNAPSHOT**

### Parámetros

- **addresses**: en donde está corriendo el cluster.
- **inPath**: donde se encuentran los archivos aeropuertos.csv y movimientos.csv (deben estar en la misma carpeta).
- **outPath**: donde se escribirán los archivos de salida (queryn.csv, queryn.txt).
- **n** (opcional): cantidad de resultados requeridos.
- **oaci** (opcional): el código OACI de un aeropuerto a analizar.
- **min** (opcional): parámetro utilizado para una de las queries.

## Query 1

	$ ./query1.sh -addresses=127.0.0.1 -inPath=. -outPath=.

## Query 2

	$ ./query2.sh -addresses=127.0.0.1 -inPath=. -outPath=. -n=5

## Query 3

	$ ./query3.sh -addresses=127.0.0.1 -inPath=. -outPath=.

## Query 4

	$ ./query4.sh -addresses=127.0.0.1 -inPath=. -outPath=. -oaci=SAEZ -n=5

## Query 5

	$ ./query5.sh -addresses=127.0.0.1 -inPath=. -outPath=. -n=5

## Query 6

	$ ./query6.sh -addresses=127.0.0.1 -inPath=. -outPath=. -min=1000

# Integrantes
  - Martina Scomazzon [@Mscomazzon](https://github.com/mscomazzon)
  - Bianca Ritorto [@BRitorto](https://github.com/BRitorto)
  - Esteban Kramer [@Estebank94](https://github.com/estebank94)
  - Oliver Balfour [@Obalfour](https://github.com/obalfour)

## License
[MIT](https://choosealicense.com/licenses/mit/)
