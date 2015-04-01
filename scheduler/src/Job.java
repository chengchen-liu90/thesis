import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Job {
	static String JOBS_PATH = "/Jobs";
	static String EXE_PATH = "/Executions";
	static String WORKERS_PATH = "/Workers";
	static String RESULTS_PATH = "/Results";

	public static void main(String[] args) {

		if (args.length != 2) {
			System.out
			.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Job zkServer:clientPort numQ");
			return;
		}
		String zkServerHostIp = args[0];
		int numQ = Integer.parseInt(args[1]);

		ZkConnector zkc = new ZkConnector();
		try {
			zkc.connect(zkServerHostIp);
		} catch (Exception e) {
			System.out.println("Zookeeper connect " + e.getMessage());
		}

		ZooKeeper zk = zkc.getZooKeeper();
		ZkUtil.createAllPersistentNodes(zk);

		// create jobs - AmWorks :)
		for (int q = 0; q < numQ; q++) {
			String jId = "j" + q;
			try {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				DataOutputStream dout = new DataOutputStream(out);
				dout.writeInt(q);
				zk.create(JOBS_PATH + "/" + jId, out.toByteArray(),
						Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				System.out.println(e.code());
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (Exception e) {
				System.out.println("Make node:" + e.getMessage());
			}
			System.out.println("created job: " + jId);
			// hack to not burst pass handlers of other components
			try {
				Thread.sleep(500);
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
