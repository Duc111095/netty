package netty.channel;

import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public final class PendingWriteQueue {

	private static final InternalLogger logger = InternalLoggerFactory.getInstance(PendingWriteQueue.class);
	private static final int PENDING_WRITE_OVERHEAD = 
			SystemPropertyUtil.getInt("io.netty.transport.pendingWriteSizeOverhead", 64);
	
	private final ChannelOutboundInvoker invoker;
	private final EventExecutor executor;
	private final PendingBytesTracker tracker;
	
}
