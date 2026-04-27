package netty.common.util.concurrent;

public interface RejectedExecutionHandler {
	
	void rejected(Runnable task, SingleThreadEventExecutor executor);
}
