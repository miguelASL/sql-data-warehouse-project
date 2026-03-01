/*
=============================================================
Proyecto: SQL Data Warehouse
=============================================================
Descripción:
  Este proyecto implementa un Data Warehouse siguiendo la
  arquitectura medallón (Bronze, Silver, Gold) para organizar
  y transformar los datos en capas de calidad creciente:

    - Bronze : datos crudos tal como llegan de las fuentes.
    - Silver : datos limpios y normalizados.
    - Gold   : datos agregados y listos para análisis.

Propósito de este archivo:
  En este script ÚNICAMENTE se crea la base de datos
  "DataWarehouse" y sus tres esquemas medallón (bronze,
  silver y gold). No contiene tablas ni datos.
=============================================================
*/

-- Crear la base de datos principal del Data Warehouse donde se almacenarán todos los esquemas y objetos del proyecto
CREATE DATABASE DataWarehouse;

-- Seleccionar la base de datos recién creada para que los comandos siguientes se ejecuten dentro de ella
USE DataWarehouse;

-- Crear el esquema bronze: almacena los datos crudos tal como llegan de las fuentes externas, sin ninguna transformación
CREATE SCHEMA bronze;

-- Crear el esquema silver: almacena los datos limpios, normalizados y validados, listos para ser transformados
CREATE SCHEMA silver;

-- Crear el esquema gold: almacena los datos agregados y enriquecidos, optimizados para el análisis y la generación de reportes
CREATE SCHEMA gold;
