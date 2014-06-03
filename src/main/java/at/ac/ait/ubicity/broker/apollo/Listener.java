package at.ac.ait.ubicity.broker.apollo;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.log4j.Logger;
import org.fusesource.stomp.jms.StompJmsDestination;

import at.ac.ait.ubicity.commons.broker.exceptions.UbicityBrokerException;

public class Listener extends AbstractBrokerClient implements MessageListener {

	private static final Logger logger = Logger.getLogger(Listener.class);

	@Override
	public void init() throws UbicityBrokerException {

		super.init();

	}

	public void setConsumer(MessageListener listener, String dest)
			throws JMSException {

		Destination destination = StompJmsDestination.createDestination(
				getConnection(), dest);
		MessageConsumer consumer = getSession().createConsumer(destination);
		consumer.setMessageListener(listener);
	}

	public void consume() throws JMSException {
		// setConsumer(this, "/topic/test_topic");
		setConsumer(this, "/queue/test_queue");
	}

	@Override
	public void onMessage(Message message) {

		if (TextMessage.class.isInstance(message)) {
			TextMessage tMsg = (TextMessage) message;
			try {
				logger.info("Received: " + tMsg.getText());
			} catch (JMSException e) {

				logger.warn("Exc. caught onMessage", e);
			}
		}

	}

	public final static void main(String[] args) throws Exception,
			InterruptedException {
		Listener list = new Listener();
		list.init();
		list.consume();

		while (true) {
			Thread.sleep(100);
		}
	}
}
