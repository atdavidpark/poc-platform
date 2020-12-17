package com.trivadis;

import com.opencsv.bean.CsvToBeanBuilder;
import com.trivadis.kafka.producer.KafkaProducerAvro;

import java.io.FileReader;
import java.util.Iterator;

public class DataSourceSimulator {

    static void runSimulator(final String fileName, final int sendMessageCount, final int speedUpFactor) throws Exception {
        FileReader filereader = new FileReader(fileName);

        // create csvReader object with parameter filereader and parser
        Iterator<ControlDataDO> iterator = new CsvToBeanBuilder(filereader)
                .withSeparator('\t')
                .withType(ControlDataDO.class)
                .build().iterator();

        KafkaProducerAvro producer = new KafkaProducerAvro();

        // we are going to read data line by line
        while (iterator.hasNext()) {
            ControlDataDO controlDataDO = iterator.next();

            producer.produce(controlDataDO, -1);
        }
    }

    public static void main(String... args) throws Exception {
        if (args.length == 0) {
            runSimulator("/Users/gus/workspace/git/trivadispf/poc-platform/impl/java/java-kafka-producer/src/main/resources/data/tng.csv",10,0);
        } else {
            runSimulator("data/test.csv", Integer.parseInt(args[0]),Integer.parseInt(args[1]));
        }
    }
}
