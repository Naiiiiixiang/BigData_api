package Kafka.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * @author niyaolanggeyo
 * @date 2021/7/7 16:45
 */
public class MyInterceptorToKafka implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte b = event.getBody()[0];
        Map<String, String> headers = event.getHeaders();

        if (b >= '0' && b <= '9') {
            headers.put("topic", "second");
        }
        if ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')) {
            headers.put("topic", "first");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder {
        @Override
        public Interceptor build() {
            return new MyInterceptorToKafka();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
