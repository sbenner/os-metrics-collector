package com.aiven.test.commons.service;

import com.aiven.test.commons.ContextProvider;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaService {
    private static Producer _producer;

    private static Consumer _consumer;

    public static Consumer<String, String> getConsumer() throws IOException {
        if (_consumer == null) {
            _consumer = new KafkaConsumer<String, String>(getKafkaConsumerProperties());
            _consumer.subscribe(Collections.singletonList(ContextProvider.contextProperties.getProperty("kafka.topic")));
        }
        return _consumer;

    }

    public static boolean checkOrCreateTopic() throws IOException, ExecutionException, InterruptedException {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ContextProvider.contextProperties.getProperty("kafka.host"));
        setSSLProperties(config);

        AdminClient adminClient = AdminClient.create(config);
        ListTopicsResult listTopics = adminClient.listTopics();
        Set<String> names = listTopics.names().get();
        String topic = ContextProvider.contextProperties.getProperty("kafka.topic");
        boolean contains = names.contains(topic);
        if (!contains) {
            List<NewTopic> topicList = new ArrayList<NewTopic>();
            Map<String, String> configs = new HashMap<String, String>();
            int partitions = 1;
            List<Integer> brokers = adminClient.describeCluster().nodes().get().stream().map(Node::id).collect(Collectors.toList());

            NewTopic newTopic = new NewTopic(topic, partitions, (short) brokers.size()).configs(configs);
            topicList.add(newTopic);
            KafkaFuture f = adminClient.createTopics(topicList).values().get(topic);
            while (!f.isDone()) {
                Thread.sleep(1000);
            }

            contains = f.isDone();
        }
        return contains;

    }

    public static Producer getProducer() throws IOException {
        if (_producer == null) {
            _producer = new KafkaProducer<String, String>(getKafkaProducerProperties());
        }
        return _producer;

    }

    private static Properties getKafkaProducerProperties() throws IOException {
        Properties props = new Properties();
        setSSLProperties(props);
        //

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        props.put("client.id", InetAddress.getLocalHost().getHostName());
        props.put("bootstrap.servers", ContextProvider.contextProperties.getProperty("kafka.host"));
        return props;
    }


    private static String writeCertsToTemp(String file) throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResource(file).openStream();

        try {
            File somethingFile = File.createTempFile(file, ".crt");
            java.nio.file.Files.copy(
                    is, somethingFile.toPath(),
                    StandardCopyOption.REPLACE_EXISTING);

            return somethingFile.toPath().toAbsolutePath().toString();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            is.close();
        }
        return null;
    }

    private static void setSSLProperties(Properties props) throws IOException {
        if (ContextProvider.contextProperties.getProperty("truststore.path") == null) {
            String tsPath = writeCertsToTemp("truststore");
            if (tsPath == null && !Paths.get(tsPath).toFile().exists()) {
                throw new IOException(tsPath + " does not exist");
            }
            ContextProvider.contextProperties.put("truststore.path", tsPath);
        }
        if (ContextProvider.contextProperties.getProperty("keystore.path") == null) {
            String ksPath = writeCertsToTemp("keystore");
            if (ksPath == null && !Paths.get(ksPath).toFile().exists()) {
                throw new IOException(ksPath + " does not exist");
            }
            ContextProvider.contextProperties.put("keystore.path", ksPath);
        }
        //System.setProperty("javax.net.ssl.trustStore", filePath);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ContextProvider.contextProperties.getProperty("truststore.path"));
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ContextProvider.contextProperties.getProperty("truststore.pwd"));
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ContextProvider.contextProperties.getProperty("keystore.path"));
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ContextProvider.contextProperties.getProperty("keystore.pwd"));
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "123456");
    }

    private static Properties getKafkaConsumerProperties() throws IOException {
        Properties props = new Properties();

        setSSLProperties(props);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put("client.id", InetAddress.getLocalHost().getHostName());
        props.put("bootstrap.servers", ContextProvider.contextProperties.getProperty("kafka.host"));
        return props;
    }


}
