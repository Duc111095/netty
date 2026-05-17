package netty.channel;

import netty.buffer.AbstractReferenceCountedByteBuf;
import netty.common.util.Recycler;
import netty.common.util.ReferenceCountUtil;
import netty.common.util.concurrent.EventExecutor;
import netty.common.util.concurrent.PromiseCombiner;
import netty.common.util.internal.ObjectPool;
import netty.common.util.internal.ObjectUtil;
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
	
	private PendingWrite head;
	private PendingWrite tail;
	private int size;
	private long bytes;
	
	public PendingWriteQueue(ChannelHandlerContext ctx) {
		tracker = PendingBytesTracker.newTracker(ctx.channel());
		this.invoker = ctx;
		this.executor = ctx.executor();
	}
	
	public PendingWriteQueue(Channel channel) {
		tracker = PendingBytesTracker.newTracker(channel);
		this.invoker = channel;
		this.executor = channel.eventLoop();
	}
	
	public boolean isEmpty() {
		assert executor.inEventLoop();
		return head == null;
	}
	
	public int size() {
		assert executor.inEventLoop();
		return size;
	}
	
	public long bytes() {
		assert executor.inEventLoop();
		return bytes;
	}
	
	private int size(Object msg) {
		int messageSize = tracker.size(msg);
		if (messageSize < 0) {
			messageSize = 0;
		}
		return messageSize + PENDING_WRITE_OVERHEAD;
	}
	
	public void add(Object msg, ChannelPromise promise) {
		assert executor.inEventLoop();
		ObjectUtil.checkNotNull(msg, "msg");
		ObjectUtil.checkNotNull(promise, "promise");
		int messageSize = size(msg);
		
		PendingWrite write = PendingWrite.newInstance(msg, messageSize, promise);
		PendingWrite currentTail = tail;
		if (currentTail == null) {
			tail = head = write;
		} else {
			currentTail.next = write;
			tail = write;
		}
		size++;
		bytes += messageSize;
		tracker.incrementPendingOutboundBytes(write.size);
		
		if (msg instanceof AbstractReferenceCountedByteBuf) {
			((AbstractReferenceCountedByteBuf) msg).touch();
		} else {
			ReferenceCountUtil.touch(msg);
		}
	}
	
	public ChannelFuture removeAndWriteAll() {
		assert executor.inEventLoop();
		
		if (isEmpty()) {
			return null;
		}
		
		ChannelPromise p = invoker.newPromise();
		PromiseCombiner combiner = new PromiseCombiner(executor);
		try {
			for (PendingWrite write = head; write != null; write = head) {
				head = tail = null;
				size = 0;
				bytes = 0;
				
				while (write != null) {
					PendingWrite next = write.next;
					Object msg = write.msg;
					ChannelPromise promise  = write.promise;
					recycle(write, false);
					if (!(promise instanceof VoidChannelPromise)) {
						combiner.add(promise);
					}
					invoker.write(msg, promise);
					write = next;
				}
			}
			combiner.finish(p);
		} catch (Throwable cause) {
			p.setFailure(cause);
		}
		assertEmpty();
		return p;
	}
	
	public void removeAndFailAll(Throwable cause) {
		assert executor.inEventLoop();
		ObjectUtil.checkNotNull(cause, "cause");
		for (PendingWrite write = head; write != null; write = head) {
			head = tail = null;
			size = 0;
			bytes = 0;
			while (write != null) {
				PendingWrite next = write.next;
				ReferenceCountUtil.safeRelease(write.msg);
				ChannelPromise promise = write.promise;
				recycle(write, false);
				safeFail(promise, cause);
				write = next;
			}
		}
		assertEmpty();
	}
	
	public void removeAndFail(Throwable cause) {
		assert executor.inEventLoop();
		ObjectUtil.checkNotNull(cause, "cause");
		
		PendingWrite write = head;
		if (write == null) {
			return;
		}
		ReferenceCountUtil.safeRelease(write.msg);
		ChannelPromise promise = write.promise;
		safeFail(promise, cause);
		recycle(write, true);
	}
	
	private void assertEmpty() {
		assert tail == null && head == null && size == 0;
	}
	
	public ChannelFuture removeAndWrite() {
		assert executor.inEventLoop();
		PendingWrite write = head;
		if (write == null) {
			return null;
		}
		Object msg = write.msg;
		ChannelPromise promise = write.promise;
		recycle(write, true);
		return invoker.write(msg, promise);
	}
	
	public ChannelPromise remove() {
		assert executor.inEventLoop();
		PendingWrite write = head;
		if (write == null) {
			return null;
		}
		ChannelPromise promise = write.promise;
		ReferenceCountUtil.safeRelease(write.msg);
		recycle(write, true);
		return promise;
	}
	
	public Object current() {
		assert executor.inEventLoop();
		PendingWrite write = head;
		if (write == null) {
			return null;
		}
		return write.msg;
	}
	
	private void recycle(PendingWrite write, boolean update) {
		final PendingWrite next = write.next;
		final long writeSize = write.size;
		
		if (update) {
			if (next == null) {
				head = tail = null;
				size = 0;
				bytes = 0;
			} else {
				head = next;
				size --;
				bytes -= writeSize;
				assert size > 0 && bytes >= 0;
			}
		}
		write.recycle();
		tracker.decrementPendingOutboundBytes(writeSize);
	}
	
	private static void safeFail(ChannelPromise promise, Throwable cause) {
		if (!(promise instanceof VoidChannelPromise) && !promise.tryFailure(cause)) {
			logger.warn("Failed to mark a promise as failure because it's done already: {}", promise, cause);
		}
	}
	
	static final class PendingWrite {
		private static final Recycler<PendingWrite> RECYCLER = 
				new Recycler<PendingWrite>() {
			@Override
			protected PendingWrite newObject(Handle<PendingWrite> handle) {
				return new PendingWrite(handle);
			}
		};
		
		private final ObjectPool.Handle<PendingWrite> handle;
		private PendingWrite next;
		private long size;
		private ChannelPromise promise;
		private Object msg;
		
		private PendingWrite(ObjectPool.Handle<PendingWrite> handle) {
			this.handle = handle;
		}
		
		static PendingWrite newInstance(Object msg, int size, ChannelPromise promise) {
			PendingWrite write = RECYCLER.get();
			write.size = size;
			write.msg = msg;
			write.promise = promise;
			return write;
		}
		
		private void recycle() {
			size = 0;
			next = null;
			msg = null;
			promise = null;
			handle.recycle(this);
		}
	}
}
