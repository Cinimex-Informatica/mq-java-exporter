package ru.cinimex.exporter.mq;

import com.ibm.mq.MQException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.cinimex.exporter.Config;
import ru.cinimex.exporter.mq.pcf.PCFElement;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class is used to manage work of all subscribers.
 */
public class MQSubscriberManager {
    private static final Logger logger = LogManager.getLogger(MQSubscriberManager.class);
    private String queueManagerName;
    private ArrayList<MQTopicSubscriber> subscribers;
    private ArrayList<MQPCFSubscriber> pcfSubscribers;
    private ScheduledExecutorService executor;
    private int timeout;
    private MQMetricQueue queue;

    /**
     * Constructor sets params for connecting to target queue manager.
     *
     * @param config - object containing different properties
     */
    public MQSubscriberManager(Config config) {
        queueManagerName = config.getQmgrName();
    }

    /**
     * Creates pool with subscribers and starts them.
     *
     * @param elements        - elements, received via MQ monitoring topics.
     * @param objects         - objects, retrieved from configuration file.
     * @param sendPCFCommands - this flag indicates, if we should send additional PCF commands (To get queues max depth, channels and listeners statuses).
     * @param usePCFWildcards - this flag indicates, if we should use wildcards (uses only 1 connection per MQObject type, but longer response processing).
     * @param interval        - interval in seconds, at which additional PCF commands are sent.
     * @param timeout         - timeout for MQGET operation (milliseconds).
     */
    public void runSubscribers(List<PCFElement> elements, List<MQObject> objects, boolean sendPCFCommands, boolean usePCFWildcards, int interval, int timeout) {
        logger.info("Launching subscribers...");
        subscribers = new ArrayList<>();
        pcfSubscribers = new ArrayList<>();
        this.timeout = timeout;
        addTopicSubscribers(elements, objects, timeout);
        if (sendPCFCommands) {
            if (usePCFWildcards) {
                EnumMap<MQObject.MQType, ArrayList<MQObject>> groups = groupMQObjects(objects);
                addPCFSubscribers(groups, interval);
            } else {
                addPCFSubscribers(objects, interval);
            }
        }
        for (MQPCFSubscriber subscriber : pcfSubscribers) {
            subscriber.start();
        }
        this.queue = new MQMetricQueue(subscribers);
        queue.start();
        if (!subscribers.isEmpty()) {
            logger.info("Successfully launched {} topic subscribers!", subscribers.size());
        } else {
            logger.warn("Didn't launch any topic subscriber. Exporter finishes it's work!");
            System.exit(1);
        }

    }

    /**
     * Stops all running subscribers in managed mode. All connections will be closed, all threads will be finished.
     *
     * @throws InterruptedException
     */
    public void stopSubscribers() throws InterruptedException {
        if (executor != null) {
            executor.shutdown();
        }

        for (MQPCFSubscriber subscriber : pcfSubscribers) {
            subscriber.stopProcessing();
        }
        for (MQPCFSubscriber subscriber : pcfSubscribers) {
            subscriber.join(timeout);
        }
        for(MQTopicSubscriber subscriber : subscribers){
            subscriber.stopProcessing();
        }

        if (queue != null) {
            queue.stopProcessing();
            queue.join(timeout);
        }
    }

    /**
     * Add topic subscribers for each MQ object being monitored.
     *
     * @param elements - list with PCFElements, received from MQ.
     * @param objects  - list with monitored MQ objects.
     * @param timeout  - timeout for MQGET operation (milliseconds).
     */
    private void addTopicSubscribers(List<PCFElement> elements, List<MQObject> objects, int timeout) {
        for (PCFElement element : elements) {
            if (element.requiresMQObject()) {
                for (MQObject object : objects) {
                    addTopicSubscriber(object, element);
                }
            } else {
                addTopicSubscriber(element);
            }
        }
    }

    /**
     * Adds topic subscriber for specific MQ object
     *
     * @param object  - monitored MQ object.
     * @param element - PCFElement, received from MQ.
     */
    private void addTopicSubscriber(MQObject object, PCFElement element) {
        if (object.getType().equals(MQObject.MQType.QUEUE)) {
            PCFElement objElement = new PCFElement(element.getTopicString(), element.getRows());
            objElement.formatTopicString(object.getName());
            subscribers.add(new MQTopicSubscriber(objElement, queueManagerName, object.getName()));
        }
    }

    /**
     * Adds topic subscriber
     *
     * @param element - PCFElement, received from MQ.
     */
    private void addTopicSubscriber(PCFElement element) {
        subscribers.add(new MQTopicSubscriber(element, queueManagerName));
    }

    /**
     * Adds PCF subscribers
     *
     * @param objects  - grouped map with objects, which require PCF subscribers
     * @param interval - interval between sending PCF commands.
     */
    private void addPCFSubscribers(Map<MQObject.MQType, ArrayList<MQObject>> objects, int interval) {
        int corePoolSize = MQObject.MQType.values().length;
        executor = Executors.newScheduledThreadPool(corePoolSize);
        for (Map.Entry<MQObject.MQType, ArrayList<MQObject>> entry : objects.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                MQPCFSubscriber subscriber = new MQPCFSubscriber(queueManagerName, entry.getValue());
                pcfSubscribers.add(subscriber);
                logger.debug("Starting subscriber for sending direct PCF commands to retrieve statistics about object with type {} and name {}.", entry.getKey().name());
                executor.scheduleAtFixedRate(subscriber, 0, interval, TimeUnit.SECONDS);
                logger.debug("Subscriber for sending direct PCF commands for objects with type {} successfully started.", entry.getKey().name());
            }
        }
    }

    /**
     * Adds PCF subscribers
     *
     * @param objects  - list with objects, which require PCF subscribers
     * @param interval - interval between sending PCF commands.
     */
    private void addPCFSubscribers(List<MQObject> objects, int interval) {
        int corePoolSize = objects.size();
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(corePoolSize);
        for (MQObject object : objects) {
            MQPCFSubscriber subscriber = new MQPCFSubscriber(object);
            pcfSubscribers.add(subscriber);
            logger.debug("Starting subscriber for sending direct PCF commands to retrieve statistics about object with type {} and name {}.", object.getType().name(), object.getName());
            scheduledExecutorService.scheduleAtFixedRate(subscriber, 0, interval, TimeUnit.SECONDS);
            logger.debug("Subscriber for sending direct PCF commands to retrieve statistics about object with type {} and name {} successfully started.", object.getType().name(), object.getName());
        }
    }

    /**
     * Method groups elements by type.
     *
     * @param objects - list with unsorted MQ objects.
     * @return - grouped map with objects.
     */
    private EnumMap<MQObject.MQType, ArrayList<MQObject>> groupMQObjects(List<MQObject> objects) {
        EnumMap<MQObject.MQType, ArrayList<MQObject>> groupedObjects = new EnumMap<>(MQObject.MQType.class);
        for (MQObject.MQType type : MQObject.MQType.values()) {
            groupedObjects.put(type, new ArrayList<>());
        }
        for (MQObject object : objects) {
            groupedObjects.get(object.getType()).add(object);
            logger.debug("{} {} was added for additional monitoring.", object.getType().name(), object.getName());
        }
        return groupedObjects;
    }
}
