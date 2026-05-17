package netty.channel;

import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import netty.common.util.concurrent.AbstractEventExecutorGroup;
import netty.common.util.concurrent.DefaultPromise;
import netty.common.util.concurrent.DefaultThreadFactory;
import netty.common.util.concurrent.EventExecutor;
import netty.common.util.concurrent.Future;
import netty.common.util.concurrent.FutureListener;
import netty.common.util.concurrent.GlobalEventExecutor;
import netty.common.util.concurrent.Promise;
import netty.common.util.concurrent.ThreadPerTaskExecutor;
import netty.common.util.internal.EmptyArrays;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.ReadOnlyIterator;

public class ThreadPerChannelEventLoopGroup extends AbstractEventExecutorGroup implements EventLoopGroup {
	
	private final Object[] childArgs;
	private final int maxChannels;
	final Executor executor;
	final Set<EventLoop> activeChildren = ConcurrentHashMap.newKeySet();
	final Queue<EventLoop> idleChildren = new ConcurrentLinkedQueue<>();
	private final ChannelException tooManyChannels;
	
	private volatile boolean shuttingDown;
	private final Promise<?> terminationFuture = new DefaultPromise<Void>(GlobalEventExecutor.INSTANCE);
	private final FutureListener<Object> childTerminationListener = new FutureListener<Object>() {
		@Override
		public void operationComplete(Future<Object> future) throws Exception {
			if (isTerminated()) {
				terminationFuture.trySuccess(null);
			}
		}
	};
	
	protected ThreadPerChannelEventLoopGroup() {
		this(0);
	}
	
	protected ThreadPerChannelEventLoopGroup(int maxChannels) {
		this(maxChannels, (ThreadFactory) null);
	}
	
	protected ThreadPerChannelEventLoopGroup(int maxChannels, ThreadFactory threadFactory, Object... args) {
		this(maxChannels, threadFactory == null ? null : new ThreadPerTaskExecutor(threadFactory), args);
	}
	
	protected ThreadPerChannelEventLoopGroup(int maxChannels, Executor executor, Object... args) {
		ObjectUtil.checkPositiveOrZero(maxChannels, "maxChannels");
		if (executor == null) {
			executor = new ThreadPerTaskExecutor(new DefaultThreadFactory(getClass()));
		}
		if (args == null) {
			childArgs = EmptyArrays.EMPTY_OBJECTS;
		} else {
			childArgs = args.clone();
		}
		
		this.maxChannels = maxChannels;
		this.executor = executor;
		
		tooManyChannels =
				ChannelException.newStatic("too many channels (max: " + maxChannels + ')', 
				ThreadPerChannelEventLoopGroup.class, "nextChild()");
	}
	
	protected EventLoop newChild(Object... args) throws Exception {
		return new ThreadPerChannelEventLoop(this);
	}
	
	@Override
	public Iterator<EventExecutor> iterator() {
		return new ReadOnlyIterator<EventExecutor>(activeChildren.iterator());
	}
	
	@Override
	public EventLoop next() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
		shuttingDown = true;
		
		for (EventLoop l : activeChildren) {
			l.shutdownGracefully(quietPeriod, timeout, unit);
		}
		for (EventLoop l : idleChildren) {
			l.shutdownGracefully(quietPeriod, timeout, unit);
		}
		if (isTerminated()) {
			terminationFuture.trySuccess(null);
		}
		return terminationFuture();
	}
	
	@Override
	public Future<?> terminationFuture() {
		return terminationFuture;
	}
	
	@Override
	public void shutdown() {
		shuttingDown = true;
		
		for (EventLoop l : activeChildren) {
			l.shutdown();
		}
		for (EventLoop l : idleChildren) {
			l.shutdown();
		}
		
		if (isTerminated()) {
			terminationFuture.trySuccess(null);
		}
	}
	
	@Override
	public boolean isShuttingDown() {
		for (EventLoop l : activeChildren) {
			if (!l.isShuttingDown()) {
				return false;
			}
		}
		for (EventLoop l : idleChildren) {
			if (!l.isShuttingDown()) {
				return false;
			}
		}
		return true;
	}
	
	@Override
    public boolean isShutdown() {
        for (EventLoop l: activeChildren) {
            if (!l.isShutdown()) {
                return false;
            }
        }
        for (EventLoop l: idleChildren) {
            if (!l.isShutdown()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isTerminated() {
        for (EventLoop l: activeChildren) {
            if (!l.isTerminated()) {
                return false;
            }
        }
        for (EventLoop l: idleChildren) {
            if (!l.isTerminated()) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) 
    		throws InterruptedException {
    	long deadline = System.nanoTime() + unit.toNanos(timeout);
    	for (EventLoop l : activeChildren) {
    		for (;;) {
    			long timeLeft = deadline - System.nanoTime();
    			if (timeLeft <= 0) {
    				return isTerminated();
    			}
    			if (l.awaitTermination(timeLeft, TimeUnit.NANOSECONDS)) {
    				break;
    			}
    		}
    	}
    	for (EventLoop l : idleChildren) {
    		for (;;) {
    			long timeLeft = deadline - System.nanoTime();
    			if (timeLeft <= 0) {
    				return isTerminated();
    			}
    			if (l.awaitTermination(timeout, TimeUnit.NANOSECONDS)) {
    				break;
    			}
    		}
    	}
    	return isTerminated();
    }
    
    @Override
    public ChannelFuture register(Channel channel) {
    	ObjectUtil.checkNotNull(channel, "channel");
    	try {
    		EventLoop l = nextChild();
    		return l.register(new DefaultChannelPromise(channel, l));
    	} catch (Throwable t) {
    		return new FailedChannelFuture(channel, GlobalEventExecutor.INSTANCE, t);
    	}
    }
    
    @Override
    public ChannelFuture register(ChannelPromise promise) {
    	try {
    		return nextChild().register(promise);
    	} catch (Throwable t) {
    		promise.setFailure(t);
    		return promise;
    	}
    }
    
    @Override
    public ChannelFuture register(Channel channel, ChannelPromise promise) {
    	ObjectUtil.checkNotNull(channel, "channel");
    	try {
    		return nextChild().register(channel, promise);
    	} catch (Throwable t) {
    		promise.setFailure(t);
    		return promise;
    	}
    }
    
    private EventLoop nextChild() throws Exception {
    	if (shuttingDown) {
    		throw new RejectedExecutionException("shuttingDown");
    	}
    	
    	EventLoop loop = idleChildren.poll();
    	if (loop == null) {
    		if (maxChannels > 0 && activeChildren.size() >= maxChannels) {
    			throw tooManyChannels;
    		}
    		loop = newChild(childArgs);
    		loop.terminationFuture().addListener(childTerminationListener);
    	}
    	activeChildren.add(loop);
    	return loop;
    }
}
