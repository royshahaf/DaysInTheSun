package basic;

import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Basic extends LeaderSelectorListenerAdapter implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(Basic.class);
	private final CuratorFramework client;
	private final LeaderSelector leaderSelector;
	private final AtomicBoolean stopped = new AtomicBoolean(false);
	private final AtomicInteger leadCount = new AtomicInteger(0);
	private final UUID id = UUID.randomUUID();
	private final BasicConfiguration configuration;

	public int getLeadCount() {
		return leadCount.get();
	}

	public Basic(BasicConfiguration configuration, String connectionString) {
		this.configuration = configuration;
		client = CuratorFrameworkFactory.newClient(connectionString, 5000, 5000, new ExponentialBackoffRetry(1000, 3));
		leaderSelector = new LeaderSelector(client, configuration.path(), this);
		leaderSelector.autoRequeue();
	}

	public void start() {
		client.start();
		leaderSelector.start();
	}

	public void stop() {
		stopped.set(true);
	}

	@Override
	public void takeLeadership(CuratorFramework client) throws Exception {
		leadCount.incrementAndGet();
		if (logger.isInfoEnabled()) {
			logger.info("master {}-{}", configuration.name(), id);
		}
		int localCounter = 0;
		while (!getStopped()) {
			if (localCounter++ % 2000 == 0) {
				if (logger.isInfoEnabled()) {
					logger.info("periodic print by {}-{}", configuration.name(), id);
				}
			}
			Thread.sleep(1);
		}
	}

	@Override
	public void close() {
		stop();
		leaderSelector.close();
	}

	public boolean getStopped() {
		return stopped.get();
	}

}
