package basic;

import java.io.Closeable;
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
	private static final String PATH = "/examples/leader";
	private static final Logger logger = LoggerFactory.getLogger(Basic.class);
	private final LeaderSelector leaderSelector;
	private final CuratorFramework client;
	private String name;
	private final AtomicBoolean stopped = new AtomicBoolean(false);
	private final AtomicInteger leadCount = new AtomicInteger(0);

	public int getLeadCount() {
		return leadCount.get();
	}

	public Basic(String connectionString, String name) {
		client = CuratorFrameworkFactory.newClient(connectionString, 5000, 5000, new ExponentialBackoffRetry(1000, 3));
		this.name = name;
		leaderSelector = new LeaderSelector(client, PATH, this);
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
		while (!getStopped()) {
			logger.info("master {}", name);
			Thread.sleep(1000);
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
