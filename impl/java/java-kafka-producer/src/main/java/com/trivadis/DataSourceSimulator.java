package com.trivadis;

import com.opencsv.bean.CsvToBeanBuilder;
import com.trivadis.kafka.producer.KafkaProducerAvro;
import picocli.CommandLine;

import java.io.*;
import java.util.Iterator;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "simulator", mixinStandardHelpOptions = true, version = "simulator 1.0",
        description = "Runs the simulator based on an input file")
public class DataSourceSimulator implements Callable<Integer> {

    private final static String BOOTSTRAP_SERVERS = "dataplatform:9092, dataplatform:9093, dataplatform:9094";

    // final String fileName, final int sendMessageCount, final int speedUpFactor

    @CommandLine.Option(names = {"-f", "--file"}, description = "the input file")
    File inputFile;

    @CommandLine.Option(names = {"-b", "--bootstrapServers"}, description = "bootstrap servers to use to connect to kafka")
    String bootstrapServers = BOOTSTRAP_SERVERS;

    @CommandLine.Option(names = {"-a", "--async"}, description = "produce asynchronously")
    boolean useAsync = false;

    @CommandLine.Option(names = {"-s", "--batch-size"}, description = "produce with this batch size (in bytes)")
    Integer batchSize = 16384;

    @CommandLine.Option(names = {"-l", "--linger-ms"}, description = "produce with this linger ms (in milliseconds)")
    Integer lingerMs = 0;

    @CommandLine.Option(names = {"-c", "--compression-type"}, description = "Compression Type to use, defaults to none")
    String compressionType = null;

    //    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, description = "display a help message")
//    private boolean helpRequested = false;

    public Integer call() throws Exception {
//        InputStream is = getClass().getClassLoader().getResourceAsStream(inputFile.);
//        Reader fileReader = new InputStreamReader(is);
        FileReader fileReader = new FileReader(inputFile);

        // create csvReader object with parameter filereader and parser
        Iterator<ControlDataDO> iterator = new CsvToBeanBuilder(fileReader)
                .withSeparator('\t')
                .withType(ControlDataDO.class)
                .build().iterator();

        KafkaProducerAvro producer = new KafkaProducerAvro(bootstrapServers, batchSize, lingerMs, compressionType);

        long startTime = System.currentTimeMillis();
        long totalRecords = 0;
        long totalRecordsPerSecond = 0;

        // we are going to read data line by line
        while (iterator.hasNext()) {
            ControlDataDO controlDataDO = iterator.next();

            producer.produce(controlDataDO,useAsync);

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

        return 0;
    }

    public static void main(String... args) throws Exception {
        int exitCode = new CommandLine(new DataSourceSimulator()).execute(args);
        System.exit(exitCode);
    }
}
