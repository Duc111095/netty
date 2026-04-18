package netty.common.util.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

import netty.common.util.internal.ObjectUtil;

public final class ThreadPerTaskExecutor implements Executor {
	
	private final ThreadFactory threadFactory;
	
	public ThreadPerTaskExecutor(ThreadFactory threadFactory) {
		this.threadFactory = ObjectUtil.checkNotNull(threadFactory, "threadFactory");
	}
	
	@Override
	public void execute(Runnable command) {
		threadFactory.newThread(command).start();
	}
}
