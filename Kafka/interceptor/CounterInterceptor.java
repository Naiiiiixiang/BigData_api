package Kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author niyaolanggeyo
 * @date 2021/7/7 16:46
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private int success;
    private int error;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            success++;
        } else {
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("Send succeed: " + success + ", Send failed: " + error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
