package com.trivadis;

import com.opencsv.bean.CsvToBeanBuilder;
import com.trivadis.kafka.producer.KafkaProducerAvro;
import picocli.CommandLine;

import java.io.*;
import java.util.Iterator;

public class DataSourceSimulator implements Callable<Integer> {

    // final String fileName, final int sendMessageCount, final int speedUpFactor

    @CommandLine.Parameters(names = "-f", paramLabel = "INPUT_FILE", description = "the input file")
    File inputFile;

    public Integer call() throws Exception {
//        InputStream is = getClass().getClassLoader().getResourceAsStream(inputFile.);
//        Reader fileReader = new InputStreamReader(is);
        FileReader fileReader = new FileReader(inputFile);

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

            producer.produce(controlDataDO,true);

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
        int exitCode = new CommandLine(new DataSourceSimulator()).execute(args);
        System.exit(exitCode);
    }
}
