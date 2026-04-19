package netty.common.util.concurrent;

public abstract class CompleteFuture<V> extends AbstractFuture<V> {
	
	private final EventExecutor executor;

	protected CompleteFuture(EventExecutor executor) {
		this.executor = executor;
	}
	
	protected EventExecutor executor() {
		return executor;
	}
	
	
}
