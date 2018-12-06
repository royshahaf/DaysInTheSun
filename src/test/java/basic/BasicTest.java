package basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.test.TestingServer;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.cfg4j.provider.ConfigurationProvider;
import org.cfg4j.provider.ConfigurationProviderBuilder;
import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.consul.ConsulConfigurationSourceBuilder;
import org.cfg4j.source.reload.strategy.PeriodicalReloadStrategy;
import org.junit.Test;

public class BasicTest {

	BasicConfiguration config = initConfiguration();

	private BasicConfiguration getConfiguration() {
		return config;
	}

	private BasicConfiguration initConfiguration() {
		ConfigurationSource source = new ConsulConfigurationSourceBuilder().withHost("192.168.99.100").withPort(8500)
				.build();
		ConfigurationProvider provider = new ConfigurationProviderBuilder().withConfigurationSource(source)
				.withReloadStrategy(new PeriodicalReloadStrategy(5, TimeUnit.SECONDS)).build();
		return provider.bind("reksio", BasicConfiguration.class);
	}

	@Test
	public void testStart() throws Exception {
		try (TestingServer server = new TestingServer();
				Basic basic = new Basic(getConfiguration(), server.getConnectString())) {
			basic.start();
			Awaitility.await().untilAsserted(() -> assertEquals(1, basic.getLeadCount()));
		}
	}

	@Test
	public void testStop() throws Exception {
		try (TestingServer server = new TestingServer();
				Basic basic = new Basic(getConfiguration(), server.getConnectString())) {
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
				Basic basic = new Basic(getConfiguration(), server.getConnectString())) {
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
				Basic basic = new Basic(getConfiguration(), server.getConnectString())) {
			try (Basic basic2 = new Basic(getConfiguration(), server.getConnectString())) {
				basic2.start();
				Awaitility.await().untilAsserted(() -> assertEquals(1, basic2.getLeadCount()));
				basic.start();
				Awaitility.await().pollDelay(Duration.ONE_SECOND)
						.untilAsserted(() -> assertEquals(0, basic.getLeadCount()));
			}
			Awaitility.await().untilAsserted(() -> assertEquals(1, basic.getLeadCount()));
		}
	}
	
	@Test
	public void testBasicConfigurationNameChange() throws IOException, Exception {
		setValue("a");
		Awaitility.await().untilAsserted(() -> assertEquals("a", getConfiguration().name()));
		setValue("b");
		Awaitility.await().untilAsserted(() -> assertEquals("b", getConfiguration().name()));
	}

	private void setValue(String value) throws UnsupportedEncodingException, IOException, ClientProtocolException {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpPut request = new HttpPut("http://192.168.99.100:8500/v1/kv/reksio/name");
			request.setEntity(new StringEntity(value));
			client.execute(request);
		}
	}

}
