package ru.cinimex.exporter;

import com.ibm.mq.*;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.pcf.PCFMessage;
import com.ibm.mq.pcf.PCFMessageAgent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.cinimex.exporter.mq.MQConnection;
import ru.cinimex.exporter.mq.MQObject;
import ru.cinimex.exporter.mq.MQSubscriberManager;
import ru.cinimex.exporter.mq.MQTopicSubscriber;
import ru.cinimex.exporter.mq.pcf.PCFClass;
import ru.cinimex.exporter.mq.pcf.PCFDataParser;
import ru.cinimex.exporter.mq.pcf.PCFElement;
import ru.cinimex.exporter.mq.pcf.PCFType;
import ru.cinimex.exporter.prometheus.HTTPServer;
import ru.cinimex.exporter.prometheus.Registry;
import ru.cinimex.exporter.prometheus.metrics.MetricsManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static ru.cinimex.exporter.mq.MQConnection.createMQConnectionParams;

/**
 * Main class of mq exporter tool. Parses config, scans topics, starts subscribers.
 */
public class ExporterLauncher {
    private static final Logger logger = LogManager.getLogger(ExporterLauncher.class);
    private static final String TOPIC_STRING = "$SYS/MQ/INFO/QMGR/%s/Monitor/METADATA/CLASSES";
    private static final int GMO = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_COMPLETE_MSG | MQConstants.MQGMO_SYNCPOINT;
    private static MQSubscriberManager manager;
    private static HTTPServer server;

    public static void main(String[] args) throws MQException, IOException {
        if (args.length == 0) {
            logger.error("It seems like you forgot to specify path to the config file.");
            System.exit(1);
        }
        Config config = new Config(args[0]);
        MQConnection.establish(config.getQmgrName(), createMQConnectionParams(config));

        createShutdownHook();
        ArrayList<PCFElement> elements = getAllPublishedMetrics(config);
        ArrayList<MQObject.MQType> monitoringTypes = new ArrayList<>();
        List<MQObject> objects = getMonitoringObjects(config);
        monitoringTypes.add(MQObject.MQType.QUEUE);
        monitoringTypes.add(MQObject.MQType.CHANNEL);
        monitoringTypes.add(MQObject.MQType.LISTENER);

        MetricsManager.initMetrics(elements, monitoringTypes);
        manager = new MQSubscriberManager(config);
        manager.runSubscribers(elements, objects, config.sendPCFCommands(), config.usePCFWildcards(),
                config.getScrapeInterval(), config.getConnTimeout());
        try {
            server = new HTTPServer(new InetSocketAddress("0.0.0.0", config.getEndpPort()), config.getEndpURL(), Registry.getRegistry(), false);
        } catch (IOException e) {
            logger.error("Error occurred during expanding endpoint for Prometheus: ", e);
        }
    }

    /**
     * Method goes through system topics structure and returns metrics headers, which are represented by PCFElement
     *
     * @param config - parsed config file.
     * @return - array, filled with metrics headers.
     */
    private static ArrayList<PCFElement> getAllPublishedMetrics(Config config) {
        List<MQTopic> topics = new ArrayList<>();
        MQTopic topic = null;
        ArrayList<PCFElement> elements = new ArrayList<>();
        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.options = GMO;
        gmo.waitInterval = 30000;
        try {
            String qmgrName = config.getQmgrName();
            topic = MQConnection.createTopic(String.format(TOPIC_STRING, qmgrName));
            topics.add(topic);
            MQMessage msg = getEmptyMessage();
            topic.get(msg, gmo);
            PCFMessage pcfResponse = new PCFMessage(msg);
            List<PCFClass> classes = PCFDataParser.getPCFClasses(pcfResponse);
            for (PCFClass pcfClass : classes) {
                topic = MQConnection.createTopic(pcfClass.getTopicString());
                topics.add(topic);
                msg = getEmptyMessage();
                topic.get(msg, gmo);
                pcfResponse = new PCFMessage(msg);
                List<PCFType> types = PCFDataParser.getPCFTypes(pcfResponse);
                for (PCFType type : types) {
                    topic = MQConnection.createTopic(type.getTopicString());
                    topics.add(topic);
                    msg = getEmptyMessage();
                    topic.get(msg, gmo);
                    pcfResponse = new PCFMessage(msg);
                    elements.addAll(PCFDataParser.getPCFElements(pcfResponse));
                }
            }
        } catch (MQException |
                IOException e) {
            logger.error("Failed!", e);
        } finally {
            try {
                logger.trace("Closing {} topics.", topics.size());
                for(MQTopic openedTopic : topics) {
                    if (openedTopic != null && openedTopic.isOpen()) {
                        openedTopic.close();
                        logger.trace("Topic {} successfully closed.", openedTopic.getName());
                    }
                }
            } catch (MQException e) {
                logger.error("Error occurred during disconnecting from topic {}. Error: ", "UNKNOWN", e);
            }
        }
        return elements;
    }

    private static List<MQObject> getMonitoringObjects(Config config) throws MQException, IOException {
        List<MQObject> objects = new ArrayList<>();

        for (MQObject.MQType type : MQObject.MQType.values()) {
            switch (type) {
                case QUEUE:
                    objects.addAll(inquireMQObjectsByPatterns(config.getQueues().get("include"), type, MQConstants.MQCACF_Q_NAMES));
                    objects.removeAll(inquireMQObjectsByPatterns(config.getQueues().get("exclude"), type, MQConstants.MQCACF_Q_NAMES));
                    break;
                case CHANNEL:
                    objects.addAll(inquireMQObjectsByPatterns(config.getChannels().get("include"), type, MQConstants.MQCACH_CHANNEL_NAMES));
                    objects.removeAll(inquireMQObjectsByPatterns(config.getChannels().get("exclude"), type, MQConstants.MQCACH_CHANNEL_NAMES));
                    break;
                case LISTENER:
                    objects.addAll(inquireMQObjectsByPatterns(config.getListeners().get("include"), type, MQConstants.MQCACH_LISTENER_NAME));
                    objects.removeAll(inquireMQObjectsByPatterns(config.getListeners().get("exclude"), type, MQConstants.MQCACH_LISTENER_NAME));
                    break;
            }
        }
        return objects;
    }

    private static List<MQObject> inquireMQObjectsByPatterns(List<String> rules, MQObject.MQType type, int getValueParam) throws MQException, IOException {

        List<MQObject> objects = new ArrayList<>();
        if (rules != null && !rules.isEmpty()) {
            PCFMessageAgent agent = new PCFMessageAgent();
            agent.connect(MQConnection.getQueueManager());
            for (String rule : rules) {
                PCFMessage pcfCommand = preparePCFCommand(type, rule);
                PCFMessage[] pcfResponse = agent.send(pcfCommand);
                if (!type.equals(MQObject.MQType.LISTENER)) {
                    String[] names = (String[]) pcfResponse[0].getParameterValue(getValueParam);

                    for (int index = 0; index < names.length; index++) {
                        String objName = names[index].trim();
                        if (!objName.startsWith("AMQ.")) {
                            objects.add(new MQObject(objName, type));
                        }
                    }
                } else {
                    for (int index = 0; index < pcfResponse.length; index++) {
                        objects.add(new MQObject(pcfResponse[index].getParameterValue(MQConstants.MQCACH_LISTENER_NAME).toString().trim(), type));
                    }
                }
            }
            agent.disconnect();
        }

        return objects;
    }

    private static PCFMessage preparePCFCommand(MQObject.MQType type, String name) {
        PCFMessage command;
        switch (type) {
            case QUEUE:
                command = new PCFMessage(MQConstants.MQCMD_INQUIRE_Q_NAMES);
                command.addParameter(MQConstants.MQCA_Q_NAME, name);
                command.addParameter(MQConstants.MQIA_Q_TYPE, MQConstants.MQQT_LOCAL);
                break;
            case CHANNEL:
                command = new PCFMessage(MQConstants.MQCMD_INQUIRE_CHANNEL_NAMES);
                command.addParameter(MQConstants.MQCACH_CHANNEL_NAME, name);
                break;
            case LISTENER:
                command = new PCFMessage(MQConstants.MQCMD_INQUIRE_LISTENER);
                command.addParameter(MQConstants.MQCACH_LISTENER_NAME, name);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        return command;
    }

    private static void createShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Exporter finishes all activities...");
            if (manager != null) {
                try {
                    logger.debug("Stopping subscribers... (it may take some time, please, be patient)");
                    manager.stopSubscribers();
                } catch (InterruptedException e) {
                    logger.error("Error occurred during stopping subscribers: ", e);
                }
            }

            if (server != null) {
                logger.debug("Stopping HTTP server...");
                server.stop();
            }
            MQConnection.close();
            logger.info("Goodbye!");
            LogManager.shutdown();
        }));
    }

    private static MQMessage getEmptyMessage() {
        MQMessage message = new MQMessage();
        message.messageId = null;
        message.encoding = 546;
        message.characterSet = 1208;
        return message;
    }
}
