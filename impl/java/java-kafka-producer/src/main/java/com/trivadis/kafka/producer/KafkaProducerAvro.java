package com.trivadis.kafka.producer;

import java.util.Properties;

import com.trivadis.ControlDataDO;
import com.trivadis.poc.controldata.v1.ControlData;
import org.apache.kafka.clients.producer.*;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class KafkaProducerAvro {

    private final static String TOPIC = "controldata-v1";

    private Producer<String, ControlData> producer;

    private static Producer<String, ControlData> createProducer(String bootstrapServers, String schemaRegistryUrl, Integer batchSize, Integer lingerMs, String compressionType, String acks) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        if (compressionType != null)
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

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

    public KafkaProducerAvro(String bootstrapServers, String schemaRegistryUrl, Integer batchSize, Integer lingerMs, String compressionType, String acks) {
        this.producer = createProducer(bootstrapServers, schemaRegistryUrl, batchSize, lingerMs, compressionType, acks);
    }

    public void produce(final ControlDataDO controlDataDO, boolean useAsync) throws Exception {
        String key = convertKey(controlDataDO);
        ControlData value = convertValue(controlDataDO);

        try {
            final ProducerRecord<String, ControlData> record =
                    new ProducerRecord<>(TOPIC, key, value);

            if (!useAsync) {
                RecordMetadata metadata = producer.send(record).get();
            } else {
                producer.send(record, (metadata, exception) -> {
                    if (metadata != null) {
                        //
                    } else {
                        exception.printStackTrace();
                    }
                });
            }
        } finally {
        }
    }

    public void close() {
        producer.flush();
        producer.close();
    }

}

