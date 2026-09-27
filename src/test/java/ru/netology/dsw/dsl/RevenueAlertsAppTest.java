package ru.netology.dsw.dsl;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.netology.dsw.TestUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RevenueAlertsAppTest {
    private final MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    private final StringSerializer stringSerializer = new StringSerializer();
    private final StringDeserializer stringDeserializer = new StringDeserializer();

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Object> purchasesTopic;
    private TestInputTopic<String, Object> productsTopic;
    private TestOutputTopic<String, Object> resultTopic;

    @BeforeEach
    void setUp() {
        var serDeProps = Map.of(
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8090"
        );
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistry, serDeProps);
        KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, serDeProps);
        testDriver = new TopologyTestDriver(RevenueAlertsApp.buildTopology(schemaRegistry, serDeProps), RevenueAlertsApp.getStreamsConfig());
        purchasesTopic = testDriver.createInputTopic(RevenueAlertsApp.PURCHASE_TOPIC_NAME, stringSerializer, avroSerializer);
        productsTopic = testDriver.createInputTopic(RevenueAlertsApp.PRODUCT_TOPIC_NAME, stringSerializer, avroSerializer);
        resultTopic = testDriver.createOutputTopic(RevenueAlertsApp.RESULT_TOPIC, stringDeserializer, avroDeserializer);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    public void shouldAlertWhenRevenueExceedsThreshold() {
        Instant twoMinutesAgo = Instant.now().minusSeconds(120);
        productsTopic.pipeInput("1", createProduct(1L, 60.0D));
        for (int i = 0; i < 6; i++) {
            purchasesTopic.pipeInput(
                    String.valueOf(i),
                    createPurchase(1L, 10L),
                    twoMinutesAgo
            );
        }

        var result = resultTopic.readKeyValue();
        assertThat(result.key, is("1"));
        assertThat(
                ((GenericRecord) result.value).get("window_start"),
                is(twoMinutesAgo.truncatedTo(ChronoUnit.MINUTES).toEpochMilli())
        );
        assertThat(((GenericRecord) result.value).get("product_id"), is(1L));
        assertThat(((GenericRecord) result.value).get("total_revenue"), is(3600.0D));
    }

    @Test
    public void shouldNotAlertWhenRevenueBelowThreshold() {
        Instant twoMinutesAgo = Instant.now().minusSeconds(120);
        productsTopic.pipeInput("1", createProduct(1L, 60.0D));
        for (int i = 0; i < 2; i++) {
            purchasesTopic.pipeInput(
                    String.valueOf(i),
                    createPurchase(1L, 10L),
                    twoMinutesAgo
            );
        }

        assertTrue(resultTopic.isEmpty());
    }

    @Test
    public void shouldIgnorePurchaseWithoutProduct() {
        Instant twoMinutesAgo = Instant.now().minusSeconds(120);
        purchasesTopic.pipeInput(
                "123",
                createPurchase(999L, 100L),
                twoMinutesAgo
        );

        assertTrue(resultTopic.isEmpty());
    }

    private GenericRecord createProduct(long id, double price) {
        Schema schema = SchemaBuilder.record("Product").fields()
                .requiredLong("id")
                .requiredString("name")
                .requiredString("description")
                .requiredDouble("price")
                .endRecord();
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("name", "TV");
        record.put("description", "TV set");
        record.put("price", price);
        return record;
    }

    private GenericRecord createPurchase(long productId, long quantity) {
        GenericRecord purchase = new GenericData.Record(TestUtils.createPurchaseSchema());
        purchase.put("id", new Random().nextLong());
        purchase.put("quantity", quantity);
        purchase.put("productid", productId);
        return purchase;
    }
}
