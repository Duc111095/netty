package netty.common.util.concurrent;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;

public final class FailedFuture<V> extends CompleteFuture<V> {
	private final Throwable cause;
	
	public FailedFuture(EventExecutor executor, Throwable cause) {
		super(executor);
		this.cause = ObjectUtil.checkNotNull(cause, "cause");
	}
	
	@Override
	public Throwable cause() {
		return cause;
	}
	
	@Override
	public boolean isSuccess() {
		return false;
	}
	
	@Override
	public Future<V> sync() {
		PlatformDependent.throwException(cause);
		return this;
	}
	
	@Override
	public Future<V> syncUninterruptibly() {
		PlatformDependent.throwException(cause);
		return this;
	}
	
	@Override
	public V getNow() {
		return null;
	}
}
