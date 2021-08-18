package Kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author niyaolanggeyo
 * @date 2021/7/7 16:48
 */
public class MyProducerCallBack {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "hadoop101:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("acks", "all");

        properties.put("retries", 3);

        properties.put("batch.size", 131072);

        properties.put("linger.ms", 1);

        properties.put("buffer.memory", 134217728);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("first", "Hello USST " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println(metadata);
                    }
                }
            });
            Thread.sleep(1);
        }
        producer.close();
    }
}
