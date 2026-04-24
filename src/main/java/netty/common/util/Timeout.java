package netty.common.util;

public interface Timeout {
	
	Timer timer();
	
	TimerTask task();
	
	boolean isExpired();
	
	boolean isCancelled();
	
	boolean cancel();
}
