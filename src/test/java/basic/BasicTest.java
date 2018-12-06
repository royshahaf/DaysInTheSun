package basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.curator.test.TestingServer;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.Test;

public class BasicTest {

	@Test
	public void testStart() throws Exception {
		try (TestingServer server = new TestingServer();
				Basic basic = new Basic(getConfiguration("stupid1", server.getConnectString()))) {
			basic.start();
			Awaitility.await().untilAsserted(() -> assertEquals(1, basic.getLeadCount()));
		}
	}

	private Configuration getConfiguration(String name, String connectionString) {
		Configuration config = new HierarchicalConfiguration();
		config.setProperty("name", name);
		config.setProperty("connectionString", connectionString);
		config.setProperty("path", "/leader/election");
		return config;
	}

	@Test
	public void testStop() throws Exception {
		try (TestingServer server = new TestingServer();
				Basic basic = new Basic(getConfiguration("stupid1", server.getConnectString()))) {
			basic.start();
			Awaitility.await().untilAsserted(() -> assertEquals(1, basic.getLeadCount()));
			Awaitility.await().until(() -> !basic.getStopped());
			basic.stop();
			Awaitility.await().until(() -> basic.getStopped());
		}
	}

	@Test
	public void testClose() throws Exception {
		Basic sillyBasic;
		try (TestingServer server = new TestingServer();
				Basic basic = new Basic(getConfiguration("stupid1", server.getConnectString()))) {
			sillyBasic = basic;
			basic.start();
			Awaitility.await().untilAsserted(() -> assertEquals(1, basic.getLeadCount()));
			Awaitility.await().until(() -> !basic.getStopped());
		}
		assertNotNull(sillyBasic);
		Awaitility.await().until(() -> sillyBasic.getStopped());
	}
	
	@Test
	public void testSlaveDoesntBecomeMasterWhileMasterIsAlive() throws IOException, Exception {
		try (TestingServer server = new TestingServer();
				Basic basic = new Basic(getConfiguration("stupid1", server.getConnectString()))) {
			try (Basic basic2 = new Basic(getConfiguration("stupid2", server.getConnectString()))) {
				basic2.start();
				Awaitility.await().untilAsserted(() -> assertEquals(1, basic2.getLeadCount()));
				basic.start();
				Awaitility.await().pollDelay(Duration.ONE_SECOND).untilAsserted(() -> assertEquals(0, basic.getLeadCount()));
			}
			Awaitility.await().untilAsserted(() -> assertEquals(1, basic.getLeadCount()));
		}
	}

}
