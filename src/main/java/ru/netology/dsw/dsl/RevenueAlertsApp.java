package ru.netology.dsw.dsl;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.KeyValue;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RevenueAlertsApp {
    public static final String PRODUCT_TOPIC_NAME = "products";
    public static final String PURCHASE_TOPIC_NAME = "purchases";
    public static final String RESULT_TOPIC = "product_revenue_alerts-dsl";
    private static final double MAX_REVENUE_PER_MINUTE = 3000.0;

    public static void main(String[] args) throws InterruptedException {
        String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8090");
        var client = new CachedSchemaRegistryClient(schemaRegistryUrl, 16);
        var serDeProps = Map.of(
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        );

        Topology topology = buildTopology(client, serDeProps);

        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getStreamsConfig());
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        try {
            kafkaStreams.start();
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    public static Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "RevenueAlertsAppDSL");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.STATE_DIR_CONFIG, "states");
        return props;
    }

    public static Topology buildTopology(SchemaRegistryClient client, Map<String, String> serDeConfig) {
        var builder = new StreamsBuilder();
        var avroSerde = new GenericAvroSerde(client);
        avroSerde.configure(serDeConfig, false);

        var purchasesStream = builder.stream(
                PURCHASE_TOPIC_NAME,
                Consumed.with(new Serdes.StringSerde(), avroSerde)
        );

        var productsTable = builder.globalTable(
                PRODUCT_TOPIC_NAME,
                Consumed.with(new Serdes.StringSerde(), avroSerde)
        );

        var purchaseWithPrice = purchasesStream.leftJoin(
                productsTable,
                (key, val) -> val.get("productid").toString(),
                RevenueAlertsApp::joinProduct
        );

        Duration oneMinute = Duration.ofMinutes(1);

        purchaseWithPrice
                .filter((key, val) -> val.success)
                .mapValues(val -> val.result)
                .selectKey((key, val) -> val.get("product_id").toString())
                .mapValues(val -> (Double) val.get("amount"))
                .groupByKey(Grouped.with(new Serdes.StringSerde(), new Serdes.DoubleSerde()))
                .windowedBy(TimeWindows.of(oneMinute).advanceBy(oneMinute))
                .aggregate(
                        () -> 0.0,
                        (key, val, agg) -> agg + val,
                        Materialized.with(new Serdes.StringSerde(), new Serdes.DoubleSerde())
                )
                .filter((key, val) -> val > MAX_REVENUE_PER_MINUTE)
                .toStream()
                .map((key, val) -> {
                    Schema schema = SchemaBuilder.record("RevenueAlert").fields()
                            .name("window_start")
                            .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
                            .noDefault()
                            .requiredLong("product_id")
                            .requiredDouble("total_revenue")
                            .endRecord();
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("window_start", key.window().start());
                    record.put("product_id", Long.parseLong(key.key()));
                    record.put("total_revenue", val);
                    return KeyValue.pair(key.key(), record);
                })
                .to(RESULT_TOPIC, Produced.with(new Serdes.StringSerde(), avroSerde));

        return builder.build();
    }

    private static JoinResult joinProduct(GenericRecord purchase, GenericRecord product) {
        try {
            Schema schema = SchemaBuilder.record("PurchaseWithPrice").fields()
                    .requiredLong("purchase_id")
                    .requiredLong("product_id")
                    .requiredDouble("amount")
                    .endRecord();
            GenericRecord result = new GenericData.Record(schema);
            long quantity = (Long) purchase.get("quantity");
            double price = (Double) product.get("price");
            result.put("purchase_id", purchase.get("id"));
            result.put("product_id", purchase.get("productid"));
            result.put("amount", quantity * price);
            return new JoinResult(true, result);
        } catch (Exception e) {
            return new JoinResult(false, purchase);
        }
    }

    private static record JoinResult(boolean success, GenericRecord result) {}
}
