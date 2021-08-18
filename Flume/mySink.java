package Flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author niyaolanggeyo
 * @date 2021/6/29 16:15
 */
public class mySink extends AbstractSink implements Configurable {

    private String prefix;
    private String subfix;
    Logger logger = LoggerFactory.getLogger(mySink.class);

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        try {
            Event event = channel.take();
            if (event != null) {
                logger.info(prefix + "--" + new String(event.getBody()) + "--" + subfix);
            }
            status = Status.READY;
            transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            transaction.close();
        }

        return status;
    }

    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix");
        subfix = context.getString("subfix", "hahaha");
    }
}
