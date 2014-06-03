package at.ac.ait.ubicity.broker.apollo;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;

import org.apache.log4j.Logger;
import org.fusesource.stomp.jms.StompJmsDestination;

import at.ac.ait.ubicity.commons.broker.exceptions.UbicityBrokerException;

public class Publisher extends AbstractBrokerClient {

	private static final Logger logger = Logger.getLogger(Publisher.class);

	@Override
	public void init() throws UbicityBrokerException {

		super.init();
	}

	public MessageProducer createProducer(String dest) throws JMSException {

		Destination destination = StompJmsDestination.createDestination(
				getConnection(), dest);

		MessageProducer producer = getSession().createProducer(destination);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		return producer;
	}

	public void produce() throws JMSException {

		// MessageProducer producer = createProducer("/topic/test_topic");
		MessageProducer producer = createProducer("/queue/test_queue");

		for (int msgCnt = 0; msgCnt < 20; msgCnt++) {
			TextMessage msg = getSession().createTextMessage(
					"Message body no " + msgCnt);
			msg.setIntProperty("id", msgCnt);

			logger.info(msg.toString());

			producer.send(msg);
		}

		producer.close();
	}
}
