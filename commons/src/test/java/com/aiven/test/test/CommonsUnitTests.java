package com.aiven.test.test;

import com.aiven.test.commons.ContextProvider;
import com.aiven.test.commons.model.Metric;
import com.aiven.test.commons.service.KafkaService;
import com.aiven.test.commons.service.PostgresService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.*;

@PrepareForTest(KafkaService.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})

public class CommonsUnitTests {

    @Mock
    PostgresService service;
    @Mock
    KafkaService kafkaService;
    @Mock
    private Connection mockConnection;
    @Mock
    private Statement mockStatement;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        ContextProvider.init();
    }

    @Test
    public void testPgService() throws SQLException {

        Mockito.when(mockConnection.createStatement()).thenReturn(mockStatement);
        Mockito.when(mockConnection.createStatement().executeUpdate(Mockito.any())).thenReturn(1);
        Metric m = new Metric();
        m.setId("test");
        m.setSystemId("test");
        m.setTemperature(new BigDecimal("32.32"));
        m.setTs(System.currentTimeMillis());
        Semaphore semaphore = new Semaphore(1);
        Mockito.when(service.insertMetric(m, semaphore)).thenReturn("test");

        String value = service.insertMetric(m, semaphore);
        assertEquals(value, "test");

    }

    @Test
    public void testKafkaService() throws IOException {

        MockProducer<String, String> mockProducer = new MockProducer<>(
                true, new StringSerializer(), new StringSerializer());

        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        PowerMockito.mockStatic(KafkaService.class);
        BDDMockito.when(KafkaService.getProducer()).thenReturn(mockProducer);
        BDDMockito.when(KafkaService.getConsumer()).thenReturn(consumer);

        String data = "{\"systemId\":\"AFC1FBFF009006EA\",\"temperature\":40,\"ts\":1600856440401}";
        ProducerRecord<String, String> record = new ProducerRecord("test", "test", "t1");
        Future recordMetadataFuture = ((MockProducer) KafkaService.getProducer()).send(record
        );
        assertTrue(recordMetadataFuture.isDone());
        assertTrue(((MockProducer) KafkaService.getProducer()).history().size() == 1);
        ProducerRecord res = (ProducerRecord) ((MockProducer) KafkaService.getProducer()).history().get(0);
        assertNotNull(res);
        assertEquals(res.value(), "t1");
        KafkaService.getConsumer().subscribe(Collections.singleton("test"));
        assertEquals(0, ((MockConsumer) KafkaService.getConsumer()).poll(Duration.ZERO).count());

        ((MockConsumer) KafkaService.getConsumer()).rebalance(Arrays.asList(new TopicPartition("test", 0), new TopicPartition("test", 1)));
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("test", 0), 0L);
        beginningOffsets.put(new TopicPartition("test", 1), 0L);
        ((MockConsumer) KafkaService.getConsumer()).updateBeginningOffsets(beginningOffsets);
        ((MockConsumer) KafkaService.getConsumer()).
                seek(new TopicPartition("test", 0), 0);
        ConsumerRecord<String, String> rec1 = new ConsumerRecord<>("test", 0,
                0, 0L, TimestampType.CREATE_TIME, 0L, 0,
                0, "k1", data);
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<>("test", 0,
                1, 0L, TimestampType.CREATE_TIME, 0L,
                0, 0, "k1", data);
        ((MockConsumer) KafkaService.getConsumer()).addRecord(rec1);
        ((MockConsumer) KafkaService.getConsumer()).addRecord(rec2);
        ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(100));
        Iterator<ConsumerRecord<String, String>> iter = recs.iterator();
        assertEquals(rec1, iter.next());
        assertEquals(rec2, iter.next());
    }

    @Test
    public void testContextProvider() throws IOException {
        ContextProvider.loadProperties();

        assertTrue(ContextProvider.loadProperties().size() > 0);
        assertNotNull(ContextProvider.contextProperties.getProperty("kafka.host"));
        assertNotNull(ContextProvider.contextProperties.getProperty("pgsql.url"));
    }
}
