import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Worker {
	static String jPath = "/Jobs";
	static String ePath = "/Executions";
	static String wPath = "/Workers";
	static String rPath = "/Results";

	public static void main(String[] args) {

		if (args.length != 2) {
			System.out
			.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:clientPort numOfWorkers");
			return;
		}
		String zkServerHostIp = args[0];
		int numOfWorkers = Integer.parseInt(args[1]);

		ZkConnector zkc = new ZkConnector();
		try {
			zkc.connect(zkServerHostIp);
		} catch (Exception e) {
			System.out.println("Zookeeper connect " + e.getMessage());
		}

		ZooKeeper zk = zkc.getZooKeeper();
		ZkUtil.createAllPersistentNodes(zk);

		// create workers
		for (int i = 0; i < numOfWorkers; i++) {
			String wId = "w" + i;
			try {
				zk.create(wPath + "/" + wId, null, Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL);
			} catch (KeeperException e) {
				System.out.println(e.code());
			} catch (Exception e) {
				System.out.println("Make node:" + e.getMessage());
			}
			System.out.println("created worker: " + wId);
			// hack to not burst pass handlers of other components
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
