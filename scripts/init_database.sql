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

-- Crear la base de datos principal del Data Warehouse
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Crear los esquemas de la arquitectura medallón
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
