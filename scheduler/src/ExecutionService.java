import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

public class ExecutionService {
	static String JOBS_PATH = "/Jobs";
	static String EXE_PATH = "/Executions";
	static String WORKERS_PATH = "/Workers";
	static String RESULTS_PATH = "/Results";

	Watcher jWatcher; // Jobs watcher
	Watcher wWatcher; // Workers watcher
	Watcher eWatcher; // Executions watcher

	ZooKeeper zk = null;
	ZkConnector zkc = null;

	public ExecutionService(String zkServerHostIp) {
		this.zkc = new ZkConnector();
		try {
			this.zkc.connect(zkServerHostIp);
		} catch (Exception e) {
			System.out.println("Zookeeper connect " + e.getMessage());
		}
		System.out.println("Exec Service connected to: " + zkServerHostIp);
		this.zk = this.zkc.getZooKeeper();
		this.jWatcher = new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				EventType type = event.getType();
				if (type == EventType.NodeChildrenChanged) {
					System.out.println("handling jobs change");
					ExecutionService.this.handleJobsChange();
				}
			}
		};
		this.wWatcher = new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				EventType type = event.getType();
				if (type == EventType.NodeChildrenChanged) {
					System.out.println("handling workers change");
					ExecutionService.this.handleWorkersChange();
				}
			}
		};
		this.eWatcher = new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				EventType type = event.getType();
				if (type == EventType.NodeChildrenChanged) {
					System.out.println("handling executions change");
					ExecutionService.this.handleExecutionsChange();
				}
			}
		};
	}

	private boolean stringInList(String s, List<String> list) {
		boolean res = true;
		for (String l: list) {
			if (l.contains(s)) {
				res = false;
			}
		}
		return res;
	}

	protected synchronized void handleExecutionsChange() {
		List<String> children = this.zkc.getChildren(EXE_PATH, this.eWatcher);
		if (children == null) {
			System.out.println("ERR: " + EXE_PATH + " does not exist ");
		}
	}

	protected synchronized void handleWorkersChange() {
		List<String> jobList = this.zkc.getChildren(JOBS_PATH);
		int jobsGiven = 0;
		System.out.println("jobs " + jobList.size());
		if (!jobList.isEmpty()) {
			// can assign new worker a job
			List<String> workerList = this.zkc.getChildren(WORKERS_PATH);
			System.out.println("workers " + workerList.size());
			List<String> exeList = this.zkc.getChildren(EXE_PATH);
			for (String w : workerList) {
				boolean assignWork = this.stringInList(w, exeList);
				if (assignWork) {
					String jobId = jobList.get(jobsGiven);
					String jPath = JOBS_PATH + "/" + jobId;
					Code remove = this.zkc.delete(jPath);
					if (remove == KeeperException.Code.OK) {
						// successfully removed one job
						String ePath = EXE_PATH + "/" + w + jobId;
						Code add = this.zkc.create(ePath, null,
								CreateMode.PERSISTENT);
						if (add != KeeperException.Code.OK) {
							System.err.println("failed to create " + ePath);
						}
						// successfully created one execution
						jobsGiven++;
					}
				}
				if (jobsGiven == jobList.size()) {
					break; // all jobs given
				}
			}
		}
		// reset watcher
		List<String> children = this.zkc.getChildren(WORKERS_PATH, this.wWatcher);
		if (children == null) {
			System.out.println("ERR: " + WORKERS_PATH + " does not exist ");
		}
	}

	protected synchronized void handleJobsChange() {

		// TODO Auto-generated method stub
		List<String> children = this.zkc.getChildren(JOBS_PATH, this.jWatcher);
		if (children == null) {
			System.out.println("ERR: " + JOBS_PATH + " does not exist ");
		}
	}

	public void startService() {
		ZkUtil.createAllPersistentNodes(this.zk);
		// setting watchers for the first time
		this.handleExecutionsChange();
		this.handleWorkersChange();
		this.handleJobsChange();
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out
			.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:clientPort");
			return;
		}
		String zkServerHostIp = args[0];
		ExecutionService es = new ExecutionService(zkServerHostIp);
		es.startService();

		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}
}
