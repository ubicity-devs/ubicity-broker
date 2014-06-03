package at.ac.ait.ubicity.broker.lmax;

import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import net.xeoh.plugins.base.annotations.PluginImplementation;
import net.xeoh.plugins.base.annotations.events.Init;

import org.apache.log4j.Logger;

import at.ac.ait.ubicity.commons.broker.BrokerConsumer;
import at.ac.ait.ubicity.commons.broker.UbicityBroker;
import at.ac.ait.ubicity.commons.broker.events.ConsumerPoison;
import at.ac.ait.ubicity.commons.broker.events.EventEntry;
import at.ac.ait.ubicity.commons.broker.events.Metadata;
import at.ac.ait.ubicity.commons.broker.exceptions.UbicityBrokerException;
import at.ac.ait.ubicity.commons.broker.exceptions.UbicityBrokerException.BrokerMsg;
import at.ac.ait.ubicity.commons.util.PropertyLoader;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Global instance for holding all UbicityBrokers and management of the
 * disruptor instances.
 * 
 * @author ruggenthalerc
 *
 */
@PluginImplementation
public class LMAXBroker implements UbicityBroker {
	private static HashMap<String, Disruptor<EventEntry>> brokers = new HashMap<String, Disruptor<EventEntry>>();

	private static Executor executor = Executors.newCachedThreadPool();

	private static int QUEUE_SIZE;

	private static final Logger logger = Logger.getLogger(LMAXBroker.class);

	private String name;

	@Override
	@Init
	public void init() {

		PropertyLoader config = new PropertyLoader(
				LMAXBroker.class.getResource("/lmax.cfg"));
		QUEUE_SIZE = config.getInt("core.lmax.queue_size");
		name = config.getString("core.lmax.name");

		logger.info("Started LMAX Broker");
	}

	/**
	 * Registers the consumer and creates an invidual broker instance for it.
	 * 
	 * @param consumer
	 */
	@Override
	@SuppressWarnings("unchecked")
	public synchronized void register(BrokerConsumer consumer) {

		if (!brokers.containsKey(consumer.getName())) {
			// Construct the Disruptor
			Disruptor<EventEntry> disruptor = new Disruptor<>(
					() -> new EventEntry(), QUEUE_SIZE, executor,
					ProducerType.SINGLE, new BlockingWaitStrategy());

			disruptor.handleEventsWith(consumer);
			disruptor.start();
			brokers.put(consumer.getName(), disruptor);

			logger.info("Registered consumer: " + consumer.getName());
		}
	}

	/**
	 * Deregisters the consumer and deletes the disruptor entry.
	 * 
	 * @param consumer
	 */
	@Override
	public synchronized void deRegister(BrokerConsumer consumer) {

		if (brokers.containsKey(consumer.getName())) {

			Disruptor<EventEntry> disruptor = brokers.get(consumer.getName());

			brokers.remove(consumer.getName());

			// Publish end message to signal shutdown
			publishToBroker(disruptor.getRingBuffer(), new ConsumerPoison());

			logger.info("Deregistered consumer: " + consumer.getName());

			// Stored entries are processed before the disruptor is shut down.
			disruptor.shutdown();
		}
	}

	/**
	 * Publishes the message to all specified consumers.
	 * 
	 * @param <T>
	 * 
	 * @param <T>
	 * 
	 * @param producer
	 */
	@Override
	public void publish(EventEntry event) throws UbicityBrokerException {

		// set to current sequence
		event.incSequence();

		for (int i = 0; i < event.getCurrentMetadata().size(); i++) {

			Metadata metadata = event.getCurrentMetadata().get(i);

			if (brokers.containsKey(metadata.getDestination())) {

				RingBuffer<EventEntry> buffer = brokers.get(
						metadata.getDestination()).getRingBuffer();

				publishToBroker(buffer, event);

			} else {
				logger.fatal("Failed to publish message '" + event.getId()
						+ "' to consumer '" + metadata.getDestination() + "'");

				throw new UbicityBrokerException(
						BrokerMsg.PRODUCER_NOT_EXISTENT);
			}
		}
	}

	private static void publishToBroker(RingBuffer<EventEntry> buffer,
			EventEntry event) {

		long sequence = buffer.next();
		try {
			EventEntry e = buffer.get(sequence);
			e.copy(event);
		} finally {
			buffer.publish(sequence);
		}
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void shutdown() {

		for (Disruptor<EventEntry> broker : brokers.values()) {
			// Publish end message to signal shutdown
			publishToBroker(broker.getRingBuffer(), new ConsumerPoison());
			// Stored entries are processed before the disruptor is shut down.
			broker.shutdown();
		}
	}
}
