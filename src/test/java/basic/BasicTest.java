package basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.curator.test.TestingServer;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.Test;

public class BasicTest {

	@Test
	public void testStart() throws Exception {
		try (TestingServer server = new TestingServer();
				Basic basic = new Basic(server.getConnectString(), "stupid1")) {
			basic.start();
			Awaitility.await().untilAsserted(() -> assertEquals(1, basic.getLeadCount()));
		}
	}

	@Test
	public void testStop() throws Exception {
		try (TestingServer server = new TestingServer();
				Basic basic = new Basic(server.getConnectString(), "stupid1")) {
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
				Basic basic = new Basic(server.getConnectString(), "stupid1")) {
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
				Basic basic = new Basic(server.getConnectString(), "stupid1")) {
			try (Basic basic2 = new Basic(server.getConnectString(), "stupid2")) {
				basic2.start();
				Awaitility.await().untilAsserted(() -> assertEquals(1, basic2.getLeadCount()));
				basic.start();
				Awaitility.await().pollDelay(Duration.ONE_SECOND).untilAsserted(() -> assertEquals(0, basic.getLeadCount()));
			}
			Awaitility.await().untilAsserted(() -> assertEquals(1, basic.getLeadCount()));
		}
	}

}
