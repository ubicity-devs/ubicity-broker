package at.ac.ait.ubicity.broker.apollo;

import net.xeoh.plugins.base.annotations.Thread;
import net.xeoh.plugins.base.annotations.events.Init;

import org.apache.activemq.apollo.broker.Broker;
import org.apache.activemq.apollo.dto.AcceptingConnectorDTO;
import org.apache.activemq.apollo.dto.BrokerDTO;
import org.apache.activemq.apollo.dto.VirtualHostDTO;
import org.apache.activemq.apollo.stomp.dto.StompDTO;
import org.apache.log4j.Logger;

import at.ac.ait.ubicity.commons.broker.BrokerConsumer;
import at.ac.ait.ubicity.commons.broker.UbicityBroker;
import at.ac.ait.ubicity.commons.broker.events.EventEntry;
import at.ac.ait.ubicity.commons.broker.exceptions.UbicityBrokerException;
import at.ac.ait.ubicity.commons.util.PropertyLoader;

//@PluginImplementation
public class ApolloBroker implements UbicityBroker {

	private static Broker broker = new Broker();

	private static final Logger logger = Logger.getLogger(ApolloBroker.class);

	private static boolean embedded = true;

	private String name;

	@Override
	@Init
	public void init() {

		PropertyLoader config = new PropertyLoader(
				Listener.class.getResource("/broker_server.cfg"));

		name = config.getString("addon.broker.name");
		embedded = config.getBoolean("addon.broker.server.embedded");

		if (embedded == false) {
			logger.info("Using external Apollo broker!");
			return;
		}

		logger.info(name + " loaded");
		createVirtualHosts(config);
	}

	@Thread
	public void run() {
		if (embedded) {
			broker.start(new Runnable() {
				@Override
				public void run() {
					logger.info("Embedded Apollo broker has been started.");
				}
			});
		}
	}

	private void createVirtualHosts(PropertyLoader config) {
		BrokerDTO brokerCfg = new BrokerDTO();

		// Brokers support multiple virtual hosts.
		VirtualHostDTO host = new VirtualHostDTO();
		host.id = config.getString("addon.apollo.server.vh.id");
		host.host_names.add(config.getString("addon.apollo.server.vh.name"));

		brokerCfg.virtual_hosts.add(host);

		// TCP transport
		AcceptingConnectorDTO tcp = new AcceptingConnectorDTO();
		tcp.id = config.getString("addon.apollo.server.tcp.connector.id");
		tcp.bind = config.getString("addon.apollo.server.tcp.connector.bind");
		brokerCfg.connectors.add(tcp);

		// Websocket transport
		final AcceptingConnectorDTO ws = new AcceptingConnectorDTO();
		ws.id = config.getString("addon.apollo.server.ws.connector.id");
		ws.bind = config.getString("addon.apollo.server.ws.connector.bind");
		ws.protocols.add(new StompDTO());
		brokerCfg.connectors.add(ws);

		broker.setConfig(brokerCfg);
	}

	@Override
	public void register(BrokerConsumer consumer) {
		logger.info("Registered consumer: " + consumer.getName());
	}

	@Override
	public void deRegister(BrokerConsumer consumer) {
		logger.info("Deregistered consumer: " + consumer.getName());
	}

	@Override
	public void publish(EventEntry event) throws UbicityBrokerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void shutdown() {
		if (embedded) {
			broker.stop(new Runnable() {
				@Override
				public void run() {
					logger.info("Embedded Apollo broker has been stopped.");
				}
			});
		}
	}

	public final static void main(String[] args) throws Exception {
		ApolloBroker broker = new ApolloBroker();
		broker.init();
		broker.run();

		while (true) {
			java.lang.Thread.sleep(100);
		}
	}

	@Override
	public String getName() {
		return name;
	}
}