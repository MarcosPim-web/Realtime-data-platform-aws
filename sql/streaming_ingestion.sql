-- =============================================================================
-- PRE-ENTREGA 6 - REDSHIFT STREAMING INGESTION
-- Proyecto: realtime-data-platform
-- Entorno: dev
--
-- Arquitectura:
--
-- Kinesis Data Streams
--        |
--        v
-- Redshift Streaming Ingestion
--        |
--        v
-- sensor_stream_raw (Materialized View)
--        |
--        v
-- sensor_stream_typed (View)
--        |
--        v
-- sensor_stream_ready (View)
--
-- En paralelo:
--
-- Kinesis -> Flink -> Apache Iceberg -> S3 + Glue
--                                      |
--                                      v
--                            lakehouse_ext.sensor_metrics
--
-- Finalmente:
--
-- sensor_stream_ready JOIN lakehouse_ext.sensor_metrics
--
-- Estrategia de refresh:
-- Durante desarrollo se utiliza REFRESH manual para controlar consumo y costos.
-- En producción podría utilizarse auto-refresh o una frecuencia de 30-60 segundos,
-- dependiendo del SLA de latencia.
-- =============================================================================


-- =============================================================================
-- 1. STREAMING INGESTION: KINESIS -> REDSHIFT
-- =============================================================================

CREATE EXTERNAL SCHEMA kinesis_stream
FROM KINESIS
IAM_ROLE default;


-- =============================================================================
-- 2. MATERIALIZED VIEW RAW
--
-- Consume directamente el stream de Kinesis.
-- JSON_PARSE convierte el payload JSON al tipo SUPER de Redshift.
-- CAN_JSON_PARSE evita que JSON inválido rompa la ingesta.
--
-- Los registros inválidos quedan disponibles en failed_payload.
-- =============================================================================

CREATE MATERIALIZED VIEW sensor_stream_raw AUTO REFRESH YES AS
SELECT
    approximate_arrival_timestamp AS arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,

    CASE
        WHEN CAN_JSON_PARSE(kinesis_data)
        THEN JSON_PARSE(kinesis_data)
        ELSE NULL
    END AS payload,

    CASE
        WHEN NOT CAN_JSON_PARSE(kinesis_data)
        THEN kinesis_data
        ELSE NULL
    END AS failed_payload

FROM kinesis_stream."${KINESIS_STREAM_NAME}";


-- =============================================================================
-- 3. MATERIALIZED VIEW TIPADA
--
-- Extrae campos del objeto SUPER y los convierte a tipos SQL.
--
-- El timestamp se mantiene temporalmente como VARCHAR para conservar
-- el mantenimiento incremental de la Materialized View.
-- =============================================================================

CREATE OR REPLACE VIEW sensor_stream_typed AS
SELECT
    arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,

    payload.sensor_id::VARCHAR AS sensor_id,
    payload.timestamp::VARCHAR AS event_timestamp_raw,
    payload.temperature::DECIMAL(5,2) AS temperature,
    payload.humidity::DECIMAL(5,2) AS humidity,
    payload.air_quality_index::INTEGER AS air_quality_index

FROM sensor_stream_raw
WHERE payload IS NOT NULL;


-- =============================================================================
-- 4. VIEW ANALÍTICA
--
-- La conversión final del timestamp se realiza en una VIEW convencional.
-- De esta forma no perjudicamos el refresh incremental de las MVs.
-- =============================================================================

CREATE OR REPLACE VIEW sensor_stream_ready AS
SELECT
    arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,
    sensor_id,

    TRY_CAST(
        event_timestamp_raw AS TIMESTAMPTZ
    ) AS event_timestamp,

    temperature,
    humidity,
    air_quality_index

FROM sensor_stream_typed;


-- =============================================================================
-- 5. POLITICA DE REFRESH Y FRESHNESS
--
-- sensor_stream_raw utiliza AUTO REFRESH YES para consumir Kinesis de forma
-- automatica. Las capas sensor_stream_typed y sensor_stream_ready son VIEWs
-- convencionales, por lo que no requieren un refresh independiente.
--
-- Objetivo operativo:
--   Freshness objetivo: <= 60 segundos.
--   Warning:              > 90 segundos.
--   Critical:             > 120 segundos sostenidos durante 5 minutos.
--
-- Ante lag elevado:
--   1. Revisar SYS_STREAM_SCAN_STATES y skipped_rows.
--   2. Revisar IteratorAgeMilliseconds y throughput de Kinesis.
--   3. Revisar carga/RPU de Redshift Serverless.
--   4. Escalar shards, procesamiento o capacidad segun el cuello de botella.
--
-- Para una validacion controlada puede forzarse excepcionalmente:
-- REFRESH MATERIALIZED VIEW sensor_stream_raw;
-- =============================================================================

-- =============================================================================
-- 6. VALIDACIÓN DE DATOS HOT
-- =============================================================================

SELECT
    arrival_timestamp,
    sensor_id,
    event_timestamp,
    temperature,
    humidity,
    air_quality_index
FROM sensor_stream_ready
ORDER BY event_timestamp DESC
LIMIT 20;


-- =============================================================================
-- 7. VALIDACIÓN DEL REFRESH INCREMENTAL
--
-- state = 1 indica mantenimiento incremental.
-- =============================================================================

SELECT
    name,
    state,
    is_stale,
    autorefresh
FROM SVV_MV_INFO
WHERE name = 'sensor_stream_raw';


-- =============================================================================
-- 8. INTEGRACIÓN CON GLUE DATA CATALOG / APACHE ICEBERG
-- =============================================================================

CREATE EXTERNAL SCHEMA lakehouse_ext
FROM DATA CATALOG
DATABASE '${GLUE_DATABASE_NAME}'
REGION '${AWS_REGION}'
IAM_ROLE default;


-- =============================================================================
-- 9. VALIDAR TABLAS EXTERNAS DISPONIBLES
-- =============================================================================

SELECT
    schemaname,
    tablename
FROM SVV_EXTERNAL_TABLES
WHERE schemaname = 'lakehouse_ext';


-- =============================================================================
-- 10. CONSULTAR DATOS HISTÓRICOS DE ICEBERG
-- =============================================================================

SELECT
    sensor_id,
    window_start,
    window_end,
    event_count,
    avg_temperature,
    avg_humidity,
    avg_aqi
FROM lakehouse_ext.sensor_metrics
ORDER BY window_start DESC
LIMIT 20;


-- =============================================================================
-- 11. JOIN HOT + HISTÓRICO
--
-- HOT:
-- último evento recibido mediante Kinesis Streaming Ingestion.
--
-- HISTÓRICO:
-- última ventana agregada almacenada en Apache Iceberg.
--
-- El JOIN demuestra que Redshift puede combinar ambos caminos
-- analíticos en una única consulta SQL.
-- =============================================================================

WITH latest_hot AS (
    SELECT
        sensor_id,
        event_timestamp,
        temperature,
        humidity,
        air_quality_index,

        ROW_NUMBER() OVER (
            PARTITION BY sensor_id
            ORDER BY event_timestamp DESC
        ) AS rn

    FROM sensor_stream_ready
),

latest_historical AS (
    SELECT
        sensor_id,
        window_start,
        window_end,
        event_count,
        avg_temperature,
        avg_humidity,
        avg_aqi,

        ROW_NUMBER() OVER (
            PARTITION BY sensor_id
            ORDER BY window_end DESC
        ) AS rn

    FROM lakehouse_ext.sensor_metrics
)

SELECT
    h.sensor_id,

    h.event_timestamp AS hot_event_timestamp,
    h.temperature AS hot_temperature,
    h.humidity AS hot_humidity,
    h.air_quality_index AS hot_aqi,

    i.window_start AS historical_window_start,
    i.window_end AS historical_window_end,
    i.event_count,

    i.avg_temperature AS historical_avg_temperature,
    i.avg_humidity AS historical_avg_humidity,
    i.avg_aqi AS historical_avg_aqi

FROM latest_hot h

JOIN latest_historical i
    ON h.sensor_id = i.sensor_id

WHERE h.rn = 1
  AND i.rn = 1

ORDER BY h.sensor_id;


-- =============================================================================
-- 12. MONITOREO DEL LAG DE STREAMING INGESTION
--
-- scanned_rows:
-- cantidad de registros procesados.
--
-- skipped_rows:
-- registros omitidos durante el scan.
--
-- ingestion_lag_seconds:
-- diferencia entre el timestamp del último registro del stream y
-- el momento en que Redshift realizó el scan.
--
-- Con refresh manual, este valor también refleja el intervalo entre refreshes.
-- =============================================================================

SELECT
    partition_id AS shard_id,
    scanned_rows,
    skipped_rows,

    stream_record_time_max AS last_stream_record,
    record_time AS redshift_scan_time,

    DATEDIFF(
        second,
        stream_record_time_max,
        record_time
    ) AS ingestion_lag_seconds

FROM SYS_STREAM_SCAN_STATES

WHERE external_schema_name = 'kinesis_stream'
  AND stream_name = '${KINESIS_STREAM_NAME}'
  AND mv_name = 'sensor_stream_raw'

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY partition_id
    ORDER BY record_time DESC
) = 1

ORDER BY shard_id;


-- =============================================================================
-- 13. SEGURIDAD DE LA CAPA ANALÍTICA
--
-- Rol de base de datos dedicado para consumidores analíticos.
--
-- El rol obtiene únicamente USAGE/SELECT sobre los objetos requeridos.
-- No recibe permisos de administración, CREATE ni modificación.
--
-- Ejecutar CREATE ROLE una única vez.
-- =============================================================================

CREATE ROLE analytics_reader;

GRANT USAGE ON SCHEMA public
TO ROLE analytics_reader;

GRANT SELECT ON sensor_stream_raw
TO ROLE analytics_reader;

GRANT SELECT ON sensor_stream_typed
TO ROLE analytics_reader;

GRANT SELECT ON sensor_stream_ready
TO ROLE analytics_reader;

GRANT USAGE ON SCHEMA lakehouse_ext
TO ROLE analytics_reader;

GRANT TEMP ON DATABASE ${REDSHIFT_DATABASE_NAME}
TO ROLE analytics_reader;