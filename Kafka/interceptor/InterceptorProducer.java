package Kafka.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author niyaolanggeyo
 * @date 2021/7/7 16:46
 */
public class InterceptorProducer {
    public static void main(String[] args) {
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

        ArrayList<String> list = new ArrayList<String>();
        list.add("com.atguigu.interceptor.TimestampInterceptor");
        list.add("com.atguigu.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("first", "Hello Shanghai " + i));
        }

        producer.close();
    }
}
