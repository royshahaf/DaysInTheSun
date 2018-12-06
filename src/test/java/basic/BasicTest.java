package basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.test.TestingServer;
import org.apache.http.client.ClientProtocolException;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.cfg4j.provider.ConfigurationProvider;
import org.cfg4j.provider.ConfigurationProviderBuilder;
import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.context.environment.ImmutableEnvironment;
import org.cfg4j.source.context.filesprovider.ConfigFilesProvider;
import org.cfg4j.source.files.FilesConfigurationSource;
import org.cfg4j.source.reload.strategy.PeriodicalReloadStrategy;
import org.junit.Test;

public class BasicTest {

	BasicConfiguration config = initConfiguration();

	private BasicConfiguration getConfiguration() {
		return config;
	}

	private BasicConfiguration initConfiguration() {
		/*
		 * ConfigurationSource source = new
		 * ConsulConfigurationSourceBuilder().withHost(ip).withPort(port).build();
		 */
		ConfigFilesProvider configFilesProvider = () -> Arrays.asList(Paths.get("application.yaml"));
		ConfigurationSource source = new FilesConfigurationSource(configFilesProvider);
		ImmutableEnvironment environment = new ImmutableEnvironment(".");
		ConfigurationProvider provider = new ConfigurationProviderBuilder().withConfigurationSource(source)
				.withReloadStrategy(new PeriodicalReloadStrategy(5, TimeUnit.SECONDS)).withEnvironment(environment).build();
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
		/*
		 * try (CloseableHttpClient client = HttpClients.createDefault()) { HttpPut
		 * request = new HttpPut("http://ip:port/v1/kv/reksio/name");
		 * request.setEntity(new StringEntity(value)); client.execute(request); }
		 */
		List<String> before = Files.readAllLines(Paths.get("application.yaml"));
		List<String> after = new ArrayList<>();
		for (String line : before) {
			if (line.contains("name")) {
				after.add("  name: " + value);
			} else {
				after.add(line);
			}
		}
		Files.write(Paths.get("application.yaml"), after);
	}

}
