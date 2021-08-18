package Kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author niyaolanggeyo
 * @date 2021/7/7 16:47
 */
public class TimestampInterceptor implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String value = record.value();
        String valueNew = System.currentTimeMillis() + value;

        ProducerRecord<String, String> record1 = new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), valueNew, record.headers());
        return record1;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
