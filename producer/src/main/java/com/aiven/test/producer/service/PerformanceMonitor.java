package com.aiven.test.producer.service;

/*
 *
 * Performance monitor - to track down the processor id, temperature and time
 *
 * */


import com.aiven.test.commons.ContextProvider;
import com.aiven.test.commons.service.KafkaService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;


public class PerformanceMonitor implements Runnable {

    private static final java.util.logging.Logger logger = Logger.getLogger(PerformanceMonitor.class.getName());
    private static int delay = 0;
    private KafkaService kafkaService;

    public PerformanceMonitor(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
        delay = Integer.valueOf(ContextProvider.contextProperties.getProperty("metrics.send.delay.ms"));
    }

    public String getSystemInfo() {
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();

        String processorSerialNumber = hal.getProcessor().getProcessorIdentifier().getProcessorID();

        double temperature =
                hal.getSensors().getCpuTemperature();
        JSONObject o =
                new JSONObject();
        o.put("systemId", processorSerialNumber);
        o.put("temperature", temperature);
        o.put("ts", System.currentTimeMillis());
        logger.log(Level.FINE, hal.getProcessor().toString());
        return o.toString();
    }

    @Override
    public void run() {

        while (!Thread.interrupted()) {

            try {
                ProducerRecord<String, String> record = new ProducerRecord<>(ContextProvider.contextProperties.getProperty("kafka.topic")
                        , UUID.randomUUID().toString(), getSystemInfo());
                logger.info(getSystemInfo());
                Future<RecordMetadata> m = kafkaService.getProducer().send(record);

                //we make sure it was sent and wait for :delay ms
                while (!m.isDone()) {
                    Thread.sleep(delay);
                }

            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage(), e);

            }

        }

    }
}
