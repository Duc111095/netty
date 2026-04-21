package netty.common.util.concurrent;

import java.util.concurrent.Executor;

public interface ThreadAwareExecutor extends Executor {
	boolean isExecutorThread(Thread thread);
}
