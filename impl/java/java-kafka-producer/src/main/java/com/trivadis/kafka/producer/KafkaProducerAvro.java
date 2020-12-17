package com.trivadis.kafka.producer;

import java.util.Properties;

import com.trivadis.ControlDataDO;
import com.trivadis.poc.controldata.v1.ControlData;
import org.apache.kafka.clients.producer.*;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class KafkaProducerAvro {

    private final static String TOPIC = "controldata-v1";
    private final static String BOOTSTRAP_SERVERS =
            "dataplatform:9092, dataplatform:9093, dataplatform:9094";
    private final static String SCHEMA_REGISTRY_URL = "http://dataplatform:8081";

    private Producer<String, ControlData> producer;

    private static Producer<String, ControlData> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);   // use constant for "schema.registry.url"

        return new KafkaProducer<>(props);
    }

    private String convertKey(ControlDataDO controlDataDO) {
        return controlDataDO.getB1() + ":" + controlDataDO.getB2() + ":" + controlDataDO.getB3() + ":" + controlDataDO.getElem() + ":" + controlDataDO.getInfo();
    }

    private ControlData convertValue(ControlDataDO controlDataDO) {
        ControlData controlData = ControlData.newBuilder()
                .setArchitimetext(controlDataDO.getArchitimetext())
                .setB1(controlDataDO.getB1())
                .setB2(controlDataDO.getB2())
                .setB3(controlDataDO.getB3())
                .setElem(controlDataDO.getElem())
                .setInfo(controlDataDO.getInfo())
                .setResolution(controlDataDO.getResolution())
                .setValue(controlDataDO.getValue())
                .setFlag(controlDataDO.getFlag())
                .build();
        return controlData;
    }

    public KafkaProducerAvro() {
        this.producer = createProducer();
    }

    public void produce(final ControlDataDO controlDataDO, final int totalMessagesToSend) throws Exception {
        long time = System.currentTimeMillis();
        String key = convertKey(controlDataDO);
        ControlData value = convertValue(controlDataDO);

        try {
            final ProducerRecord<String, ControlData> record =
                    new ProducerRecord<>(TOPIC, key, value);

            RecordMetadata metadata = producer.send(record).get();

            long elapsedTime = System.currentTimeMillis() - time;
            time = System.currentTimeMillis();
        } finally {
        }
    }

}
