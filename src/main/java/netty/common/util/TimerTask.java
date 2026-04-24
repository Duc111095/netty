package netty.common.util;

public interface TimerTask {
	
	void run(Timeout timeout) throws Exception;
	
	default void cancelled(Timeout timeout) {
		
	}
}
