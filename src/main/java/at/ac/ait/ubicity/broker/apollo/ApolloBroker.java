package at.ac.ait.ubicity.broker.apollo;

import net.xeoh.plugins.base.annotations.PluginImplementation;
import net.xeoh.plugins.base.annotations.Thread;
import net.xeoh.plugins.base.annotations.events.Init;
import net.xeoh.plugins.base.annotations.events.Shutdown;

import org.apache.activemq.apollo.broker.Broker;
import org.apache.activemq.apollo.dto.AcceptingConnectorDTO;
import org.apache.activemq.apollo.dto.BrokerDTO;
import org.apache.activemq.apollo.dto.VirtualHostDTO;
import org.apache.activemq.apollo.stomp.dto.StompDTO;
import org.apache.log4j.Logger;

import at.ac.ait.ubicity.commons.interfaces.UbicityPlugin;
import at.ac.ait.ubicity.commons.util.PropertyLoader;

@PluginImplementation
public class ApolloBroker implements UbicityPlugin {

	private static Broker broker = new Broker();

	private static final Logger logger = Logger.getLogger(ApolloBroker.class);

	private boolean embedded = true;

	private String name;

	@Override
	@Init
	public void init() {

		PropertyLoader config = new PropertyLoader(ApolloBroker.class.getResource("/broker_server.cfg"));

		name = config.getString("plugin.apollo.name");
		embedded = config.getBoolean("plugin.broker.server.embedded");

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
		host.id = config.getString("plugin.apollo.server.vh.id");
		host.host_names.add(config.getString("plugin.apollo.server.vh.name"));
		brokerCfg.virtual_hosts.add(host);

		// TCP transport
		AcceptingConnectorDTO tcp = new AcceptingConnectorDTO();
		tcp.id = config.getString("plugin.apollo.server.tcp.connector.id");
		tcp.bind = config.getString("plugin.apollo.server.tcp.connector.bind") + ":" + config.getString("env.apollo.broker.tcp.port");
		tcp.protocol = "stomp";
		tcp.protocols.add(new StompDTO());
		brokerCfg.connectors.add(tcp);

		// Websocket transport
		// final AcceptingConnectorDTO ws = new AcceptingConnectorDTO();
		// ws.id = config.getString("plugin.apollo.server.ws.connector.id");
		// ws.bind = config.getString("plugin.apollo.server.ws.connector.bind") + ":" + config.getString("env.apollo.broker.ws.port");
		// ws.protocol = "stomp";
		// ws.protocols.add(new StompDTO());
		// brokerCfg.connectors.add(ws);

		broker.setConfig(brokerCfg);
	}

	@Override
	@Shutdown
	public void shutdown() {
		if (embedded) {
			try {
				java.lang.Thread.sleep(500);
			} catch (InterruptedException e) {
			}
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