package com.aiven.test.producer;

import com.aiven.test.commons.ContextProvider;
import com.aiven.test.commons.service.KafkaService;
import com.aiven.test.producer.service.PerformanceMonitor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;


public class MetricsProducer {

    private static final Logger logger = Logger.getLogger(MetricsProducer.class.getName());


    public static void main(String[] args) throws IOException {

        KafkaService kafkaService = new KafkaService();
        ContextProvider.init();
        try {
            if (KafkaService.checkOrCreateTopic())
                new Thread(new PerformanceMonitor(kafkaService)).start();


        } catch (Exception ex) {
            logger.log(Level.SEVERE, ex.toString(), ex);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    Files.deleteIfExists(Paths.get(ContextProvider.contextProperties.getProperty("truststore.path")));
                    Files.deleteIfExists(Paths.get(ContextProvider.contextProperties.getProperty("keystore.path")));
                } catch (IOException e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }

            }
        });
    }

}
