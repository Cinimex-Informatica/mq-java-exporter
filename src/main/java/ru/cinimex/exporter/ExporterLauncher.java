package ru.cinimex.exporter;

import static com.ibm.mq.constants.MQConstants.*;
import static ru.cinimex.exporter.mq.MQConnection.createMQConnectionParams;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQTopic;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFMessage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.cinimex.exporter.mq.MQConnection;
import ru.cinimex.exporter.mq.MQSubscriberManager;
import ru.cinimex.exporter.mq.pcf.PCFDataParser;
import ru.cinimex.exporter.mq.pcf.model.PCFClass;
import ru.cinimex.exporter.mq.pcf.model.PCFElement;
import ru.cinimex.exporter.mq.pcf.model.PCFType;
import ru.cinimex.exporter.prometheus.HTTPServer;
import ru.cinimex.exporter.prometheus.Registry;
import ru.cinimex.exporter.prometheus.metrics.MetricsManager;

/**
 * Main class of mq exporter tool. Parses config, scans topics, starts subscribers.
 */
public class ExporterLauncher {
    private static final Logger logger = LogManager.getLogger(ExporterLauncher.class);
    private static final String TOPIC_STRING = "$SYS/MQ/INFO/QMGR/%s/Monitor/METADATA/CLASSES";
    private static final int GMO = MQGMO_WAIT | MQGMO_COMPLETE_MSG | MQGMO_SYNCPOINT;
    private static MQSubscriberManager manager;
    private static HTTPServer server;

    public static void main(String[] args) throws MQException {
        if (args.length == 0) {
            logger.error("It seems like you forgot to specify path to the config file.");
            System.exit(1);
        }
        Config config = new Config(args[0]);
        MQConnection.establish(config.getQmgrName(), createMQConnectionParams(config));

        createShutdownHook();
        ArrayList<PCFElement> elements = getAllPublishedMetrics(config);
        MetricsManager.initMetrics(elements);

        manager = new MQSubscriberManager(config);
        manager.runSubscribers(elements, config.sendPCFCommands(),
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
        MQTopic topic;
        List<MQTopic> topics = new ArrayList<>();
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

        } catch (MQException | IOException | MQDataException e) {
            logger.error("Failed!", e);
        } finally {
            try {
                logger.trace("Closing {} topics.", topics.size());
                for (MQTopic openedTopic : topics) {
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
