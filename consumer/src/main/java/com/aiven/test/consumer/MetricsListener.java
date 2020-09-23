package com.aiven.test.consumer;

import com.aiven.test.commons.ContextProvider;
import com.aiven.test.commons.service.KafkaService;
import com.aiven.test.commons.service.PostgresService;
import com.aiven.test.consumer.converter.MetricConverter;
import com.aiven.test.consumer.service.MetricSaverService;

import java.util.logging.Level;
import java.util.logging.Logger;


public class MetricsListener {

    private static final Logger logger = Logger.getLogger(MetricsListener.class.getName());

    public static void main(String[] args) {
        try {
            ContextProvider.init();
            PostgresService postgresService = new PostgresService();

            MetricConverter converter = new MetricConverter();
            MetricSaverService metricsListener = new MetricSaverService(
                    postgresService,
                    KafkaService.getConsumer(),
                    converter);
            new Thread(metricsListener).start();
        } catch (Exception ex) {
            logger.log(Level.SEVERE, ex.toString(), ex);
        }

    }

}
