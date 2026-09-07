package com.dataops.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class SensorStreamingJob {

    private static final String ICEBERG_TABLE_NAME = "sensor_metrics";

    public static void main(String[] args) throws Exception {

        // ---------------------------------------------------------------------
        // 1. ENTORNO DE FLINK
        // ---------------------------------------------------------------------

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // El checkpointing de 60 segundos se configura desde Terraform
        // en Amazon Managed Service for Apache Flink.

        // ---------------------------------------------------------------------
        // 2. CONFIGURACION DE AWS
        // ---------------------------------------------------------------------

        String streamArn = getStreamArn();
        IcebergConfig icebergConfig = getIcebergConfig();

        // ---------------------------------------------------------------------
        // 3. SOURCE: AMAZON KINESIS
        // ---------------------------------------------------------------------

        Configuration sourceConfig = new Configuration();

        sourceConfig.set(
                KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
                KinesisSourceConfigOptions.InitialPosition.LATEST
        );

        KinesisStreamsSource<String> kinesisSource =
                KinesisStreamsSource.<String>builder()
                        .setStreamArn(streamArn)
                        .setSourceConfig(sourceConfig)
                        .setDeserializationSchema(new SimpleStringSchema())
                        .build();

        DataStream<String> rawEvents =
                env.fromSource(
                        kinesisSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kinesis Sensor Source",
                        Types.STRING
                );

        // ---------------------------------------------------------------------
        // 4. JSON -> SENSOR EVENT
        // ---------------------------------------------------------------------

        DataStream<SensorEvent> sensorEvents =
                rawEvents
                        .map(new JsonToSensorEvent())
                        .name("Deserialize Sensor JSON");

        // ---------------------------------------------------------------------
        // 5. EVENT TIME + WATERMARKS
        // ---------------------------------------------------------------------

        WatermarkStrategy<SensorEvent> watermarkStrategy =
                WatermarkStrategy
                        .<SensorEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(10)
                        )
                        .withTimestampAssigner(
                                (event, previousTimestamp) ->
                                        Instant.parse(event.timestamp).toEpochMilli()
                        )
                        .withIdleness(Duration.ofSeconds(30));

        DataStream<SensorEvent> eventsWithWatermarks =
                sensorEvents
                        .assignTimestampsAndWatermarks(watermarkStrategy)
                        .name("Event Time and Watermarks");

        // ---------------------------------------------------------------------
        // 6. VENTANAS DE 1 MINUTO
        // ---------------------------------------------------------------------

        DataStream<SensorWindowResult> windowResults =
                eventsWithWatermarks
                        .keyBy(event -> event.sensor_id)
                        .window(
                                TumblingEventTimeWindows.of(
                                        Time.minutes(1)
                                )
                        )
                        .process(new SensorWindowFunction())
                        .name("Sensor One Minute Aggregation");

        // ---------------------------------------------------------------------
        // 7. CONVERTIR RESULTADO A FILAS PARA ICEBERG
        // ---------------------------------------------------------------------

        DataStream<Row> icebergRows =
                windowResults
                        .map(new WindowResultToRow())
                        .returns(
                                Types.ROW_NAMED(
                                        new String[]{
                                                "sensor_id",
                                                "window_start",
                                                "window_end",
                                                "event_count",
                                                "avg_temperature",
                                                "avg_humidity",
                                                "avg_aqi"
                                        },
                                        Types.STRING,
                                        Types.INSTANT,
                                        Types.INSTANT,
                                        Types.INT,
                                        Types.DOUBLE,
                                        Types.DOUBLE,
                                        Types.DOUBLE
                                )
                        )
                        .name("Convert Window Result to Iceberg Row");

        // ---------------------------------------------------------------------
        // 8. ESQUEMA FLINK PARA EL ICEBERG SINK
        // ---------------------------------------------------------------------

        ResolvedSchema flinkSchema =
                ResolvedSchema.of(
                        Column.physical(
                                "sensor_id",
                                DataTypes.STRING().notNull()
                        ),
                        Column.physical(
                                "window_start",
                                DataTypes.TIMESTAMP_LTZ(3).notNull()
                        ),
                        Column.physical(
                                "window_end",
                                DataTypes.TIMESTAMP_LTZ(3).notNull()
                        ),
                        Column.physical(
                                "event_count",
                                DataTypes.INT().notNull()
                        ),
                        Column.physical(
                                "avg_temperature",
                                DataTypes.DOUBLE().notNull()
                        ),
                        Column.physical(
                                "avg_humidity",
                                DataTypes.DOUBLE().notNull()
                        ),
                        Column.physical(
                                "avg_aqi",
                                DataTypes.DOUBLE().notNull()
                        )
                );

        // ---------------------------------------------------------------------
        // 9. GLUE CATALOG + TABLA ICEBERG
        // ---------------------------------------------------------------------

        TableLoader tableLoader =
                createIcebergTableLoader(
                        icebergConfig.warehousePath,
                        icebergConfig.databaseName
                );

        // ---------------------------------------------------------------------
        // 10. ICEBERG SINK
        // ---------------------------------------------------------------------

        IcebergSink
                .forRow(icebergRows, flinkSchema)
                .tableLoader(tableLoader)
                .append();

        // ---------------------------------------------------------------------
        // 11. EJECUTAR PIPELINE
        // ---------------------------------------------------------------------

        env.execute("Urban Sensors Real-Time Lakehouse Pipeline");
    }

    // =========================================================================
    // CONFIGURACION DE KINESIS
    // =========================================================================

    private static String getStreamArn() throws Exception {

        Map<String, Properties> applicationProperties =
                KinesisAnalyticsRuntime.getApplicationProperties();

        Properties inputProperties =
                applicationProperties.get("InputStream");

        if (inputProperties == null) {
            throw new IllegalStateException(
                    "No se encontro el grupo de propiedades InputStream."
            );
        }

        String streamArn =
                inputProperties.getProperty("stream.arn");

        if (streamArn == null || streamArn.isBlank()) {
            throw new IllegalStateException(
                    "No se encontro la propiedad stream.arn."
            );
        }

        return streamArn;
    }

    // =========================================================================
    // CONFIGURACION DE ICEBERG
    // =========================================================================

    private static IcebergConfig getIcebergConfig() throws Exception {

        Map<String, Properties> applicationProperties =
                KinesisAnalyticsRuntime.getApplicationProperties();

        Properties icebergProperties =
                applicationProperties.get("IcebergCatalog");

        if (icebergProperties == null) {
            throw new IllegalStateException(
                    "No se encontro el grupo de propiedades IcebergCatalog."
            );
        }

        String warehousePath =
                icebergProperties.getProperty("warehouse.path");

        String databaseName =
                icebergProperties.getProperty("database.name");

        if (warehousePath == null || warehousePath.isBlank()) {
            throw new IllegalStateException(
                    "No se encontro warehouse.path."
            );
        }

        if (databaseName == null || databaseName.isBlank()) {
            throw new IllegalStateException(
                    "No se encontro database.name."
            );
        }

        return new IcebergConfig(
                warehousePath,
                databaseName
        );
    }

    // =========================================================================
    // CREACION / CARGA DE TABLA ICEBERG
    // =========================================================================

    private static TableLoader createIcebergTableLoader(
            String warehousePath,
            String databaseName) {

        Map<String, String> catalogProperties =
                new HashMap<>();

        catalogProperties.put(
                CatalogProperties.WAREHOUSE_LOCATION,
                warehousePath
        );

        catalogProperties.put(
                CatalogProperties.FILE_IO_IMPL,
                S3FileIO.class.getName()
        );

        // Recomendado para ingesta streaming con Glue.
        catalogProperties.put(
                "glue.skip-archive",
                "true"
        );

        CatalogLoader catalogLoader =
        CatalogLoader.custom(
                "glue_catalog",
                catalogProperties,
                new org.apache.hadoop.conf.Configuration(false),
                GlueCatalog.class.getName()
        );

        Catalog catalog =
                catalogLoader.loadCatalog();

        TableIdentifier tableIdentifier =
                TableIdentifier.of(
                        databaseName,
                        ICEBERG_TABLE_NAME
                );

        if (!catalog.tableExists(tableIdentifier)) {

            Schema icebergSchema =
                    new Schema(
                            org.apache.iceberg.types.Types.NestedField.required(
                                    1,
                                    "sensor_id",
                                    org.apache.iceberg.types.Types.StringType.get()
                            ),
                            org.apache.iceberg.types.Types.NestedField.required(
                                    2,
                                    "window_start",
                                    org.apache.iceberg.types.Types.TimestampType.withZone()
                            ),
                            org.apache.iceberg.types.Types.NestedField.required(
                                    3,
                                    "window_end",
                                    org.apache.iceberg.types.Types.TimestampType.withZone()
                            ),
                            org.apache.iceberg.types.Types.NestedField.required(
                                    4,
                                    "event_count",
                                    org.apache.iceberg.types.Types.IntegerType.get()
                            ),
                            org.apache.iceberg.types.Types.NestedField.required(
                                    5,
                                    "avg_temperature",
                                    org.apache.iceberg.types.Types.DoubleType.get()
                            ),
                            org.apache.iceberg.types.Types.NestedField.required(
                                    6,
                                    "avg_humidity",
                                    org.apache.iceberg.types.Types.DoubleType.get()
                            ),
                            org.apache.iceberg.types.Types.NestedField.required(
                                    7,
                                    "avg_aqi",
                                    org.apache.iceberg.types.Types.DoubleType.get()
                            )
                    );

            // Hidden partitioning por dia.
            PartitionSpec partitionSpec =
                    PartitionSpec
                            .builderFor(icebergSchema)
                            .day("window_start")
                            .build();

            Map<String, String> tableProperties =
                    new HashMap<>();

            tableProperties.put(
                    TableProperties.FORMAT_VERSION,
                    "2"
            );

            catalog.createTable(
                    tableIdentifier,
                    icebergSchema,
                    partitionSpec,
                    tableProperties
            );
        }

        return TableLoader.fromCatalog(
                catalogLoader,
                tableIdentifier
        );
    }

    // =========================================================================
    // MODELOS
    // =========================================================================

    public static class SensorEvent {

        public String sensor_id;
        public double temperature;
        public double humidity;
        public int air_quality_index;
        public String timestamp;

        public SensorEvent() {
        }
    }

    public static class SensorWindowResult {

        public String sensor_id;
        public long window_start;
        public long window_end;
        public int event_count;
        public double avg_temperature;
        public double avg_humidity;
        public double avg_aqi;

        public SensorWindowResult() {
        }

        public SensorWindowResult(
                String sensorId,
                long windowStart,
                long windowEnd,
                int eventCount,
                double avgTemperature,
                double avgHumidity,
                double avgAqi) {

            this.sensor_id = sensorId;
            this.window_start = windowStart;
            this.window_end = windowEnd;
            this.event_count = eventCount;
            this.avg_temperature = avgTemperature;
            this.avg_humidity = avgHumidity;
            this.avg_aqi = avgAqi;
        }
    }

    public static class IcebergConfig {

        public final String warehousePath;
        public final String databaseName;

        public IcebergConfig(
                String warehousePath,
                String databaseName) {

            this.warehousePath = warehousePath;
            this.databaseName = databaseName;
        }
    }

    // =========================================================================
    // DESERIALIZACION
    // =========================================================================

    public static class JsonToSensorEvent
            implements MapFunction<String, SensorEvent> {

        private static final ObjectMapper mapper =
                new ObjectMapper();

        @Override
        public SensorEvent map(String json) throws Exception {
            return mapper.readValue(
                    json,
                    SensorEvent.class
            );
        }
    }

    // =========================================================================
    // AGREGACION DE VENTANA
    // =========================================================================

    public static class SensorWindowFunction
            extends ProcessWindowFunction<
                    SensorEvent,
                    SensorWindowResult,
                    String,
                    TimeWindow> {

        private static final Logger LOG =
                LoggerFactory.getLogger(
                        SensorWindowFunction.class
                );

        @Override
        public void process(
                String sensorId,
                Context context,
                Iterable<SensorEvent> events,
                Collector<SensorWindowResult> out) {

            int count = 0;

            double temperatureSum = 0;
            double humiditySum = 0;
            double airQualitySum = 0;

            for (SensorEvent event : events) {

                count++;

                temperatureSum += event.temperature;
                humiditySum += event.humidity;
                airQualitySum += event.air_quality_index;
            }

            if (count == 0) {
                return;
            }

            double avgTemperature =
                    temperatureSum / count;

            double avgHumidity =
                    humiditySum / count;

            double avgAirQuality =
                    airQualitySum / count;

            SensorWindowResult result =
                    new SensorWindowResult(
                            sensorId,
                            context.window().getStart(),
                            context.window().getEnd(),
                            count,
                            avgTemperature,
                            avgHumidity,
                            avgAirQuality
                    );

            LOG.info(
                    String.format(
                            Locale.US,
                            "WINDOW_RESULT sensor=%s | window_start=%s | window_end=%s | events=%d | avg_temperature=%.2f | avg_humidity=%.2f | avg_aqi=%.2f",
                            result.sensor_id,
                            Instant.ofEpochMilli(result.window_start),
                            Instant.ofEpochMilli(result.window_end),
                            result.event_count,
                            result.avg_temperature,
                            result.avg_humidity,
                            result.avg_aqi
                    )
            );

            out.collect(result);
        }
    }

    // =========================================================================
    // CONVERSION A ROW PARA ICEBERG
    // =========================================================================

    public static class WindowResultToRow
            implements MapFunction<SensorWindowResult, Row> {

        @Override
        public Row map(
                SensorWindowResult result) {

            return Row.of(
                    result.sensor_id,
                    Instant.ofEpochMilli(result.window_start),
                    Instant.ofEpochMilli(result.window_end),
                    result.event_count,
                    result.avg_temperature,
                    result.avg_humidity,
                    result.avg_aqi
            );
        }
    }
}