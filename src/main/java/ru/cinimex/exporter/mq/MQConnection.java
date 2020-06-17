package ru.cinimex.exporter.mq;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.MQTopic;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.MQConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.cinimex.exporter.Config;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * Class represents MQ connection.
 */
public class MQConnection {
    private static final Logger logger = LogManager.getLogger(MQConnection.class);
    private static MQQueueManager queueManager;
    private static MQQueue dynamicQueue;

    private MQConnection() {

    }

    /**
     * Method creates connection properties Hashtable from connection parameters.
     *
     * @param config - object containing different properties.
     * @return - returns prepared structure with all parameters transformed into queue manager's format.
     */
    public static Map<String, Object> createMQConnectionParams(Config config) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(MQConstants.TRANSPORT_PROPERTY, config.getQmgrHost() == null ? MQConstants.TRANSPORT_MQSERIES_BINDINGS : MQConstants.TRANSPORT_MQSERIES_CLIENT);
        if (config.getQmgrHost() != null) properties.put(MQConstants.HOST_NAME_PROPERTY, config.getQmgrHost());
        if (config.getQmgrPort() != 0) properties.put(MQConstants.PORT_PROPERTY, config.getQmgrPort());
        if (config.getQmgrChannel() != null) properties.put(MQConstants.CHANNEL_PROPERTY, config.getQmgrChannel());
        if (config.getUser() != null || config.getPassword() != null) {
            properties.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, config.useMqscp());
            if (config.getUser() != null) properties.put(MQConstants.USER_ID_PROPERTY, config.getUser());
            if (config.getPassword() != null) properties.put(MQConstants.PASSWORD_PROPERTY, config.getPassword());
        }
        MQSecurityProperties mqSecurityProperties = config.getMqSecurityProperties();
        if (mqSecurityProperties != null && mqSecurityProperties.isUseTLS()) {
            properties.put(MQConstants.SSL_CIPHER_SUITE_PROPERTY, mqSecurityProperties.getCipherSuite());
            properties.put(MQConstants.SSL_SOCKET_FACTORY_PROPERTY, getSslSocketFactory(mqSecurityProperties));
            System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings", "false");
        }
        return properties;
    }

    /**
     * Method creates SSLSocketFactory from connection parameters.
     *
     * @param mqSecurityProperties - object containing security properties.
     * @return - returns prepared SSLSocketFactory.
     */
    private static SSLSocketFactory getSslSocketFactory(MQSecurityProperties mqSecurityProperties) {
        KeyStore keyStore = getStore(mqSecurityProperties.getKeystorePath(), mqSecurityProperties.getKeystorePassword());
        KeyStore trustStore = getStore(mqSecurityProperties.getTruststorePath(), mqSecurityProperties.getTruststorePassword());
        SSLContext sslContext = null;
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            keyManagerFactory.init(keyStore, mqSecurityProperties.getKeystorePassword().toCharArray());
            sslContext = SSLContext.getInstance(mqSecurityProperties.getSslProtocol());
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
        } catch (KeyStoreException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyManagementException e1) {
            logger.error("Failed!", e1);
        }
        return sslContext.getSocketFactory();
    }

    private static KeyStore getStore(String storePath, String storePassword) {
        KeyStore keyStore = null;
        try (FileInputStream keyStoreInput = new FileInputStream(storePath)) {
            keyStore = KeyStore.getInstance("JKS");
            keyStore.load(keyStoreInput, storePassword.toCharArray());
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
            logger.error("Failed to get key or trust store: ", e);
        }
        return keyStore;
    }

    /**
     * Method establishes connection with queue manager.
     *
     * @param qmNqme               - queue manager's name.
     * @param connectionProperties - prepared structure with all parameters transformed into queue manager's format. See {@link #createMQConnectionParams(Config config)} for more info.
     */
    public static void establish(String qmNqme, Map<String, Object> connectionProperties) throws MQException {
        if (queueManager == null || !queueManager.isConnected()) {
            queueManager = new MQQueueManager(qmNqme, new Hashtable<>(connectionProperties));
        }
    }

    /**
     * Method closes connection.
     */
    public static void close() {
        try {
            if (queueManager != null && queueManager.isConnected()) {
                queueManager.disconnect();
            }
        } catch (MQException e) {
            logger.error("Failed!", e);
        }
    }

    /**
     * Returns MQTopic object, which was retrieved from queue manager.
     *
     * @param topic - topic string.
     * @return - MQTopic object, which is described @see <a href="https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.0.0/com.ibm.mq.javadoc.doc/WMQJavaClasses/com/ibm/mq/MQTopic.html">here</a>.
     * @throws MQException - MQ exception, which contains mqrc error code. More info <a href="https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.0.0/com.ibm.mq.javadoc.doc/WMQJMSClasses/com/ibm/mq/MQException.html">here</a>.
     */
    public static MQTopic createTopic(String topic) throws MQException {
        return queueManager.accessTopic(topic, "", CMQC.MQTOPIC_OPEN_AS_SUBSCRIPTION, CMQC.MQSO_CREATE | CMQC.MQSO_NON_DURABLE | CMQC.MQSO_MANAGED);
    }

    /**
     * Returns MQTopic object, which was retrieved from queue manager.
     *
     * @param topic - topic string
     * @return - MQTopic object, which is described @see <a href="https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.0.0/com.ibm.mq.javadoc.doc/WMQJavaClasses/com/ibm/mq/MQTopic.html">here</a>.
     * @throws MQException - MQ exception, which contains mqrc error code. More info <a href="https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.0.0/com.ibm.mq.javadoc.doc/WMQJMSClasses/com/ibm/mq/MQException.html">here</a>.
     */
    public static MQTopic createSpecificTopic(String topic) throws MQException {
        try {
           MQTopic topicObject =  queueManager.accessTopic(getQueue(), topic, "", CMQC.MQSO_CREATE | CMQC.MQSO_NON_DURABLE);
           topicObject.setCloseOptions(CMQC.MQCO_IMMEDIATE);
           return topicObject;
        } catch (MQException e) {
            if (e.getReason() == MQConstants.MQRC_HANDLE_NOT_AVAILABLE) {
                logger.error("The maximum number of open handles allowed for the current task has already been reached. Please, increase the MaxHandles queue manager attribute or reduce the number of queues to monitor: ", e);
                System.exit(1);
            }
            throw e;
        }
    }

    /**
     * Returns MQQueue object, which is used for collecting all metrics from queue manager.
     *
     * @return - MQQueue object, which is described @see <a href="https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.1.0/com.ibm.mq.javadoc.doc/WMQJavaClasses/com/ibm/mq/MQQueue.html">here</a>.
     * @throws MQException - MQ exception, which contains mqrc error code. More info <a href="https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.0.0/com.ibm.mq.javadoc.doc/WMQJMSClasses/com/ibm/mq/MQException.html">here</a>.
     */
    public static synchronized MQQueue getQueue() throws MQException {
        if (dynamicQueue == null) {
            createDynamicQueue();
        }
        return dynamicQueue;
    }

    private static void createDynamicQueue() throws MQException {
        dynamicQueue = queueManager.accessQueue("SYSTEM.NDURABLE.MODEL.QUEUE", CMQC.MQOO_INPUT_AS_Q_DEF | CMQC.MQOO_FAIL_IF_QUIESCING | CMQC.MQOO_INQUIRE | CMQC.MQOT_Q, null, "MQEXPORTER.*", null);
    }

    /**
     * Returns MQQueueManager object.
     *
     * @return - MQQueueManager object.
     */
    public static MQQueueManager getQueueManager() {
        return queueManager;
    }

}
