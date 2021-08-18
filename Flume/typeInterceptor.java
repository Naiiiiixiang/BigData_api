package Flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author niyaolanggeyo
 * @date 2021/6/29 14:59
 */
public class typeInterceptor implements Interceptor {

    public List<Event> addEventList;

    @Override
    public void initialize() {
        addEventList = new ArrayList<>();
    }

    @Override
    // deal only one event
    public Event intercept(Event event) {
        // get event's head information
        Map<String, String> headers = event.getHeaders();
        // get event's body information
        String body = new String(event.getBody());
        // event whether contains "Hello" to determine add head information
        if (body.contains("Hello")) {
            // if contained "Hello", add "Hello"'s key-value pairs to head information
            headers.put("type", "Hello");
        } else {
            // if not contained "Hello", add "noHello"'s key-value pairs to head information
            headers.put("type", "noHello");
        }
        return event;
    }

    @Override
    // batch deal with event
    public List<Event> intercept(List<Event> events) {
        // clean all event set
        addEventList.clear();
        // add event into set use foreach
        for (Event event : events) {
            addEventList.add(intercept(event));
        }
        return addEventList;
    }

    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder {
        @Override
        // use myself Interceptor
        public Interceptor build() {
            return new typeInterceptor();
        }

        @Override
        // get or set configuration
        public void configure(Context context) {

        }
    }


}
