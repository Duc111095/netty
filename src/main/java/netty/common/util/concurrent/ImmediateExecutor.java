package netty.common.util.concurrent;

import java.util.concurrent.Executor;

import netty.common.util.internal.ObjectUtil;

public final class ImmediateExecutor implements Executor {
	public static final ImmediateExecutor INSTANCE = new ImmediateExecutor();
	
	private ImmediateExecutor() {
		
	}
	
	@Override
	public void execute(Runnable command) {
		ObjectUtil.checkNotNull(command, "command").run();
	}
}
