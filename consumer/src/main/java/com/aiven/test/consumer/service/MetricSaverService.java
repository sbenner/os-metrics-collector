package com.aiven.test.consumer.service;

import com.aiven.test.commons.ContextProvider;
import com.aiven.test.commons.model.Metric;
import com.aiven.test.commons.service.PostgresService;
import com.aiven.test.consumer.converter.MetricConverter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: sbenner
 * Date: 7/6/17
 * Time: 11:20 AM
 */
public class MetricSaverService implements Runnable {

    private static final Logger logger = Logger.getLogger(MetricSaverService.class.getName());
    private static final Semaphore semaphore = new Semaphore(5);
    private final PostgresService postgresService;
    private final MetricConverter converter;
    private final Consumer kafkaConsumer;

    public MetricSaverService(PostgresService postgresService,
                              Consumer<String, String> kafkaConsumer,
                              MetricConverter converter) {

        this.postgresService = postgresService;
        this.kafkaConsumer = kafkaConsumer;
        this.converter = converter;

    }

    @Override
    public void run() {

        try {

            while (!Thread.currentThread().isInterrupted()) {

                final ConsumerRecords<String, String> consumerRecords =
                        kafkaConsumer.poll(Duration.ofMillis(1000));

                if (consumerRecords.count() > 0)
                    consumerRecords.forEach(record -> {

                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        ContextProvider.getExecutor().submit(
                                new Thread(() -> {

                                    try {

                                        logger.info(String.format("Consumer Record:(%s, %s, %d, %d)",
                                                record.key(),
                                                record.value(),
                                                record.partition(), record.offset()));
                                        JSONObject object = new JSONObject(record.value());
                                        Metric m = converter.toObject(object);
                                        if (m != null)
                                            postgresService.insertMetric(m, semaphore);
                                    } catch (Exception e) {
                                        logger.log(Level.SEVERE, e.getMessage(), e);
                                    } finally {
                                        //semaphore.release();
                                    }


                                }));


                    });

                kafkaConsumer.commitAsync();

            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, ex.toString(), ex);
        }


    }

}


