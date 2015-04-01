import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;


public class ExecutionThreadHandler implements Runnable {

	static String JOBS_PATH = "/Jobs";
	static String EXE_PATH = "/Executions";
	static String WORKERS_PATH = "/Workers";
	static String RESULTS_PATH = "/Results";

	String exeId;
	int qVal;
	ZkConnector zkc;
	// time
	long start, end, exeTime;

	public ExecutionThreadHandler(String id, int q, ZkConnector zkc) {
		this.exeId = id;
		this.qVal = q;
		this.zkc = zkc;
	}

	@Override
	public void run() {
		System.out.println("Starting job: " + this.exeId + " for Q: "
				+ this.qVal);
		this.start = System.currentTimeMillis();

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		this.end = System.currentTimeMillis();
		this.exeTime = this.end - this.start;
		this.executionComplete(this.exeId, this.start, this.end,
				this.exeTime);
	}

	private void executionComplete(String exeId, long start,
			long end, long exeTime) {
		String ePath = EXE_PATH + "/" + exeId;
		String rPath = RESULTS_PATH + "/" + exeId;
		Code remove = this.zkc.delete(ePath);
		if (remove == KeeperException.Code.OK) {
			System.out.println("Deleted successful execution: " + ePath);
		} else {
			System.err.println(remove);
			System.err.println("Could not delete execution: " + ePath);
		}
		String data = "" + start + " " + end + " " + exeTime;
		Code create = this.zkc.create(rPath, data, CreateMode.PERSISTENT);
		if (create == KeeperException.Code.OK) {
			System.out.println("Created result : " + rPath);
		} else {
			System.err.println(create);
			System.err.println("Could not create result: " + rPath);
		}

	}

}
