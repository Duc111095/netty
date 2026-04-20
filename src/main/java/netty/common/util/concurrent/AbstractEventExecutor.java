package netty.common.util.concurrent;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.AbstractExecutorService;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public abstract class AbstractEventExecutor extends AbstractExecutorService implements EventExecutor {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(AbstractEventExecutor.class);
	
	static final long DEFAULT_SHUTDOWN_QUIET_PERIOD = 2;
	static final long DEFAULT_SHUTDOWN_TIMEOUT = 15;
	
	private final EventExecutorGroup parent;
	private final Collection<EventExecutor> selfCollection = Collections.<EventExecutor>singleton(this);
	
	protected AbstractEventExecutor() {
		this(null);
	}
	
	protected AbstractEventExecutor(EventExecutorGroup parent) {
		this.parent = parent;
	}
	
	@Override
	public EventExecutorGroup parent() {
		return parent;
	}
}
