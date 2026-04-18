package netty.common.util.concurrent;

public interface ThreadProperties {
	
	Thread.State state();
	
	int priority();
	
	boolean isInteruppted();
	
	boolean isDaemon();
	
	String name();
	
	long id();
	
	StackTraceElement[] stackTrace();
	
	boolean isAlive();
}
