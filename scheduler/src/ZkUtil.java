import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class ZkUtil {
	static String JOBS_PATH = "/Jobs";
	static String WORKERS_PATH = "/Workers";
	static String RESULTS_PATH = "/Results";
	static String EXE_PATH = "/Executions";

	static String[] persistentPaths = new String[] { JOBS_PATH, WORKERS_PATH,
			RESULTS_PATH, EXE_PATH };

	public static void createAllPersistentNodes(ZooKeeper zk) {
		for (String p : persistentPaths) {
			createIfNotExistPersistentNode(zk, p);
		}
	}

	private static void createIfNotExistPersistentNode(ZooKeeper zk, String path) {
		try {
			Stat stat = zk.exists(path, null);
			if (stat == null) {
				System.out.println("Creating Persistent node: " + path);

				zk.create(path, null, Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			System.out.println(e.code());
		} catch (Exception e) {
			System.out.println("Make node:" + e.getMessage());
		}

	}
}
