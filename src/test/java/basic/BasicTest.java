package basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.curator.test.TestingServer;
import org.awaitility.Awaitility;
import org.junit.Test;

import basic.Basic;

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
			Awaitility.await().untilAsserted(() -> assertFalse(basic.getStopped()));
			basic.stop();
			Awaitility.await().untilAsserted(() -> assertTrue(basic.getStopped()));
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
			Awaitility.await().untilAsserted(() -> assertFalse(basic.getStopped()));
		}
		assertNotNull(sillyBasic);
		Awaitility.await().untilAsserted(() -> assertTrue(sillyBasic.getStopped()));
	}

}
