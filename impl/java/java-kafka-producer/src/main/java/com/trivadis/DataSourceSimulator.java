package com.trivadis;

import com.opencsv.bean.CsvToBeanBuilder;
import com.trivadis.kafka.producer.KafkaProducerAvro;

import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;

public class DataSourceSimulator {

    private void runSimulator(final String fileName, final int sendMessageCount, final int speedUpFactor) throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        Reader fileReader = new InputStreamReader(is);

        // create csvReader object with parameter filereader and parser
        Iterator<ControlDataDO> iterator = new CsvToBeanBuilder(fileReader)
                .withSeparator('\t')
                .withType(ControlDataDO.class)
                .build().iterator();

        KafkaProducerAvro producer = new KafkaProducerAvro();

        long startTime = System.currentTimeMillis();
        long totalRecords = 0;
        long totalRecordsPerSecond = 0;

        // we are going to read data line by line
        while (iterator.hasNext()) {
            ControlDataDO controlDataDO = iterator.next();

            producer.produce(controlDataDO, -1);
            totalRecords++;
            totalRecordsPerSecond++;

            if ((System.currentTimeMillis() - 1000) > startTime) {
                System.out.println("Total Records last second: " + totalRecordsPerSecond);
                totalRecordsPerSecond = 0;
                startTime = System.currentTimeMillis();
            }
        }

        System.out.println("Total Records sent: " + totalRecords);
        producer.close();

    }

    public static void main(String... args) throws Exception {
        DataSourceSimulator sim = new DataSourceSimulator();
        if (args.length == 0) {
            sim.runSimulator("data/tng.csv",10,0);
        } else {
            sim.runSimulator("data/tng.csv", Integer.parseInt(args[0]),Integer.parseInt(args[1]));
        }
    }
}
