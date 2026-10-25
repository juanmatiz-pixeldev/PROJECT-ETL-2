-- Data Warehouse dimensional (PostgreSQL). Linea base = Primera Entrega (MySQL -> PostgreSQL) + hecho de la API.
-- Idempotente (IF NOT EXISTS). Los cambios de modelo (Seccion 12) los define el equipo de modelado:
-- aqui queda la BASE funcional que el pipeline necesita para ser ejecutable de punta a punta.

CREATE TABLE IF NOT EXISTS dim_ubicacion (
    codigo_municipio    INTEGER PRIMARY KEY,
    municipio           VARCHAR(150) NOT NULL,
    codigo_departamento INTEGER NOT NULL,
    departamento        VARCHAR(150) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_categoria (
    id_categoria  INTEGER PRIMARY KEY,
    categoria     VARCHAR(150) NOT NULL,
    sub_categoria VARCHAR(150) NOT NULL,
    UNIQUE (categoria, sub_categoria)
);

CREATE TABLE IF NOT EXISTS dim_prestador (
    codigo_rnt   BIGINT PRIMARY KEY,
    razon_social VARCHAR(255) NOT NULL,
    nit          VARCHAR(50)  NOT NULL,
    estado_rnt   VARCHAR(50)  NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_tiempo (
    anio INTEGER PRIMARY KEY
);

-- Grano: un snapshot anual de un PST del RNT en un municipio y una categoria (Primera Entrega, 10.1)
CREATE TABLE IF NOT EXISTS fact_turismo_anual (
    id_fact                BIGINT PRIMARY KEY,
    codigo_rnt             BIGINT  NOT NULL REFERENCES dim_prestador(codigo_rnt),
    codigo_municipio       INTEGER NOT NULL REFERENCES dim_ubicacion(codigo_municipio),
    id_categoria           INTEGER NOT NULL REFERENCES dim_categoria(id_categoria),
    anio                   INTEGER NOT NULL REFERENCES dim_tiempo(anio),
    numero_de_habitaciones INTEGER NOT NULL DEFAULT 0 CHECK (numero_de_habitaciones >= 0),
    numero_de_camas        INTEGER NOT NULL DEFAULT 0 CHECK (numero_de_camas >= 0),
    numero_de_empleados    INTEGER NOT NULL DEFAULT 0 CHECK (numero_de_empleados >= 0)
);
CREATE INDEX IF NOT EXISTS ix_fact_turismo_mun_anio ON fact_turismo_anual (codigo_municipio, anio);

-- Hecho de contexto municipal procedente de la API. Grano: un indicador de la API para un municipio y un anio.
-- Formato largo para no acoplar el DDL al esquema de la API; el equipo de modelado puede refinarlo.
CREATE TABLE IF NOT EXISTS fact_api_indicador (
    codigo_municipio INTEGER NOT NULL REFERENCES dim_ubicacion(codigo_municipio),
    anio             INTEGER NOT NULL REFERENCES dim_tiempo(anio),
    indicador        VARCHAR(80) NOT NULL,
    valor            NUMERIC(20,4) NOT NULL,
    PRIMARY KEY (codigo_municipio, anio, indicador)
);
