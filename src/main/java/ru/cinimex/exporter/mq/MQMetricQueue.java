package ru.cinimex.exporter.mq;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.constants.MQConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.cinimex.exporter.mq.pcf.PCFDataParser;

import java.util.List;

/**
 * Class represents all activities about reading and processing messages from single queue.
 */
public class MQMetricQueue extends Thread implements MQSubscriber {
    private static final Logger logger = LogManager.getLogger(MQMetricQueue.class);
    private List<MQTopicSubscriber> subscribers;
    private boolean isRunning;

    /**
     * MQMetricQueue constructor.
     *
     * @param subscribers - subscribers list. Each message, received from queue, contains custom metrics. They are passed to related subscriber, who knows how to handle them.
     */
    public MQMetricQueue(List<MQTopicSubscriber> subscribers) {
        this.subscribers = subscribers;
    }

    @Override
    public void run() {
        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_COMPLETE_MSG | MQConstants.MQGMO_FAIL_IF_QUIESCING;
        gmo.waitInterval = 16000;
        isRunning = true;
        while (isRunning) {
            MQMessage msg = new MQMessage();
            try {
                MQConnection.getQueue().get(msg, gmo);
                String name = msg.getStringProperty("mqps.Top");
                MQTopicSubscriber sub = subscribers.stream().filter(subscriber -> name.equals(subscriber.getTopicName())).findFirst().orElse(null);
                if (sub != null) {
                    sub.update(PCFDataParser.convertToPCF(msg));
                    logger.debug("Message from {} was successfully processed.", name);
                } else {
                    logger.warn("Subscriber for topic {} wasn't found", name);
                }
            } catch (MQException e1) {
                if (e1.getReason() == MQConstants.MQRC_CONNECTION_BROKEN) {
                    logger.error("Connection with queue manager was broken: ", e1);
                    System.exit(1);
                }
            } catch (Exception e) {
                logger.error("Error occurred during processing metrics: ", e);
            }
        }

    }

    @Override
    public void stopProcessing() {
        isRunning = false;
    }
}
