all: Job Worker ExecutionService

Job:
	javac -Xlint:unchecked -classpath ../../zookeeper-3.4.6.jar:../../lib/log4j-1.2.16.jar ZkConnector.java ZkUtil.java Job.java
Worker:
	javac -Xlint:unchecked -classpath ../../zookeeper-3.4.6.jar:../../lib/log4j-1.2.16.jar ZkConnector.java ZkUtil.java Worker.java
ExecutionService:
	javac -Xlint:unchecked -classpath ../../zookeeper-3.4.6.jar:../../lib/log4j-1.2.16.jar ZkConnector.java ZkUtil.java ExecutionThreadHandler.java ExecutionService.java
clean:
	rm -f *.class *.*~

