package netty.channel;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

import netty.common.util.NettyRuntime;
import netty.common.util.concurrent.DefaultThreadFactory;
import netty.common.util.concurrent.EventExecutorChooserFactory;
import netty.common.util.concurrent.MultithreadEventExecutorGroup;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public abstract class MultithreadEventLoopGroup extends MultithreadEventExecutorGroup implements EventLoopGroup{
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(MultithreadEventLoopGroup.class);
	
	private static final int DEFAULT_EVENT_LOOP_THREADS;
	
	static {
		DEFAULT_EVENT_LOOP_THREADS = Math.max(1, SystemPropertyUtil.getInt(
				"io.netty.eventLoopThreads", NettyRuntime.availableProcessors() * 2));
		if (logger.isDebugEnabled()) {
			logger.debug("-Dio.netty.eventLoopThreads: {}", DEFAULT_EVENT_LOOP_THREADS);
		}
	}
	
	protected MultithreadEventLoopGroup(int nThreads, Executor executor, Object... args) {
		super(nThreads == 0 ? DEFAULT_EVENT_LOOP_THREADS : nThreads, executor, args);
	}
	
	protected MultithreadEventLoopGroup(int nThreads, ThreadFactory threadFactory, Object... args) {
		super(nThreads == 0 ? DEFAULT_EVENT_LOOP_THREADS : nThreads, threadFactory, args);
	}
	
	protected MultithreadEventLoopGroup(int nThreads, Executor executor, EventExecutorChooserFactory chooserFactory,
			Object... args) {
		super(nThreads == 0 ? DEFAULT_EVENT_LOOP_THREADS : nThreads, executor, chooserFactory, args);
	}
	
	@Override
	protected ThreadFactory newDefaultThreadFactory() {
		return new DefaultThreadFactory(getClass(), Thread.MAX_PRIORITY);
	}
	
	@Override
	public EventLoop next() {
		return (EventLoop) super.next();
	}
	
	@Override
	protected abstract EventLoop newChild(Executor executor, Object... args) throws Exception;
	
	@Override
	public ChannelFuture register(Channel channel) {
		return next().register(channel);
	}
	
	@Override
	public ChannelFuture register(ChannelPromise promise) {
		return next().register(promise);
	}
	
	@Override
	public ChannelFuture register(Channel channel, ChannelPromise promise) {
		return next().register(channel, promise);
	}
}
