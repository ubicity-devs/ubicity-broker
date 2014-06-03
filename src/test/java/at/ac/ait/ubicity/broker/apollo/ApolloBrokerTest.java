package at.ac.ait.ubicity.broker.apollo;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ApolloBrokerTest {

	private ApolloBroker broker;
	private Publisher pub;
	private Listener cons;

	@Before
	public void setUp() throws Exception {
		// broker = new ApolloBroker();
		// broker.init();

		pub = new Publisher();
		pub.init();

		// cons = new Listener();
		// cons.init();

	}

	@Ignore
	@Test
	public void testBroker() throws Exception {
		// cons.consume();

		pub.produce();

	}

	@After
	public void tearDown() throws Exception {
		// broker.shutdown();
	}
}
