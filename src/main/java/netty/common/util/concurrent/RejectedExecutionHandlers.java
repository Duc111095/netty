package netty.common.util.concurrent;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import netty.common.util.internal.ObjectUtil;

public final class RejectedExecutionHandlers {

	private static final RejectedExecutionHandler REJECT = new RejectedExecutionHandler() {
		@Override
		public void rejected(Runnable task, SingleThreadEventExecutor executor) {
			throw new RejectedExecutionException();
		}
	};
	
	private RejectedExecutionHandlers() { }
	
	public static RejectedExecutionHandler reject() {
		return REJECT;
	}
	
	public static RejectedExecutionHandler backoff(final int retries, long backoffTimeout, TimeUnit unit) {
		ObjectUtil.checkPositive(retries, "retries");
		final long backOffNanos = unit.toNanos(backoffTimeout);
		return new RejectedExecutionHandler() {
			@Override
			public void rejected(Runnable task, SingleThreadEventExecutor executor) {
				if (!executor.inEventLoop()) {
					for (int i = 0; i < retries; i++) {
						executor.wakeup(false);
						
						LockSupport.parkNanos(backOffNanos);
						if (executor.offerTask(task)) {
							return;
						}
					}
				}
				
				throw new RejectedExecutionException();
			}
		};
	}
}
