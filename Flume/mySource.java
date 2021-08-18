package Flume;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * @author niyaolanggeyo
 * @date 2021/6/29 15:23
 */
public class mySource extends AbstractSource implements Configurable, PollableSource {


    private String prefix;
    private String subfix;

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        try {
            for (int i = 0; i < 5; i++) {
                SimpleEvent event = new SimpleEvent();
                event.setBody((prefix + "--" + i + "--" + subfix).getBytes());
                getChannelProcessor().processEvent(event);
                Thread.sleep(1000);
            }
            status = Status.READY;
        } catch (InterruptedException e) {
            e.printStackTrace();
            status = Status.BACKOFF;
        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix");
        subfix = context.getString("subfix", "hahaha");
    }
}
