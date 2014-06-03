package at.ac.ait.ubicity.broker.apollo;

import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.log4j.Logger;
import org.fusesource.stomp.jms.StompJmsConnection;
import org.fusesource.stomp.jms.StompJmsConnectionFactory;

import at.ac.ait.ubicity.commons.broker.exceptions.UbicityBrokerException;
import at.ac.ait.ubicity.commons.util.PropertyLoader;

public abstract class AbstractBrokerClient {

	private StompJmsConnection connection;
	private Session session;

	private static final Logger logger = Logger
			.getLogger(AbstractBrokerClient.class);

	/**
	 * Sets up the Broker connection with configured username & password.
	 * 
	 * @throws BrokerException
	 */
	protected void init() throws UbicityBrokerException {
		PropertyLoader config = new PropertyLoader(
				Listener.class.getResource("/broker_client.cfg"));

		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI(config.getString("addon.apollo.client.host"));

		try {
			connection = (StompJmsConnection) factory.createConnection(
					config.getString("addon.apollo.client.pwd"),
					config.getString("addon.apollo.client.host"));
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		} catch (JMSException e) {
			throw new UbicityBrokerException(e);
		}
	}

	public void shutdown() {
		try {
			connection.close();
		} catch (JMSException e) {
			logger.warn("Close connection threw exception ", e);
		}
	}

	protected Session getSession() {
		return this.session;
	}

	protected StompJmsConnection getConnection() {
		return this.connection;
	}
}
