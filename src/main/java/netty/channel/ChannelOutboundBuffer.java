package netty.channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import netty.buffer.AbstractReferenceCountedByteBuf;
import netty.buffer.ByteBuf;
import netty.buffer.ByteBufHolder;
import netty.buffer.Unpooled;
import netty.common.util.Recycler;
import netty.common.util.Recycler.EnhancedHandle;
import netty.common.util.Recycler.Handle;
import netty.common.util.ReferenceCountUtil;
import netty.common.util.concurrent.FastThreadLocal;
import netty.common.util.internal.InternalThreadLocalMap;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public final class ChannelOutboundBuffer {	
	static final int CHANNEL_OUTBOUND_BUFFER_ENTRY_OVERHEAD =
			SystemPropertyUtil.getInt("io.netty.transport.outboundBufferEntrySizeOverhead", 96);
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChannelOutboundBuffer.class);
	
	private static final FastThreadLocal<ByteBuffer[]> NIO_BUFFERS = new FastThreadLocal<ByteBuffer[]>() {
		@Override
		protected ByteBuffer[] initialValue() throws IOException {
			return new ByteBuffer[1024];
		}
	};
	
	private final Channel channel;

	private Entry flushedEntry;
	private Entry unflushedEntry;
	private Entry tailEntry;
	private int flushed;
	
	private int nioBufferCount;
	private long nioBufferSize;
	
	private boolean inFail;
	
	private static final AtomicLongFieldUpdater<ChannelOutboundBuffer> TOTAL_PENDING_SIZE_UPDATER =
			AtomicLongFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "totalPendingSize");
	
	private volatile long totalPendingSize;
	
	private static final AtomicIntegerFieldUpdater<ChannelOutboundBuffer> UNWRITABLE_UPDATER = 
			AtomicIntegerFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "unwritable");
	private volatile int unwritable;
	
	private volatile Runnable fireChannelWritabilityChangedTask;
	
	ChannelOutboundBuffer(AbstractChannel channel) {
		this.channel = channel;
	}
	
	public void addMessage(Object msg, int size, ChannelPromise promise) {
		Entry entry = Entry.newInstance(msg, size, total(msg), promise);
		if (tailEntry == null) {
			flushedEntry = null;
		} else {
			Entry tail = tailEntry;
			tail.next = entry;
		}
		tailEntry = entry;
		if (unflushedEntry == null) {
			unflushedEntry = entry;
		}
		
		if (msg instanceof AbstractReferenceCountedByteBuf) {
			((AbstractReferenceCountedByteBuf) msg).touch();
		} else {
			ReferenceCountUtil.touch(msg);
		}
		
		incrementPendingOutboundBytes(entry.pendingSize, false);
	}
	
	public void addFlush() {
		Entry entry = unflushedEntry;
		if (entry != null) {
			if (flushedEntry == null) {
				flushedEntry = entry;
			}
			do {
				flushed ++;
				if (!entry.promise.setUncancellable()) {
					int pending = entry.cancel();
					decrementPendingOutboundBytes(pending, false, true);
				}
				entry = entry.next;
			} while (entry != null);
			unflushedEntry = null;
		}
	}
	
	void incrementPendingOutboundBytes(long size) {
		incrementPendingOutboundBytes(size, true);
	}
	
	private void incrementPendingOutboundBytes(long size, boolean invokeLater) {
		if (size == 0) {
			return;
		}
		long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, size);
		if (newWriteBufferSize > channel.config().getWriteBufferHighWaterMark()) {
			setUnwritable(invokeLater);
		}
	}
	
	void decrementPendingOutboundBytes(long size) {
		decrementPendingOutboundBytes(size, true, true);
	}
	
	private void decrementPendingOutboundBytes(long size, boolean invokeLater, boolean nofityWritability) {
		if (size == 0) {
			return;
		}
		long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, -size);
		if (nofityWritability && newWriteBufferSize < channel.config().getWriteBufferHighWaterMark()) {
			setUnwritable(invokeLater);
		}
	}
	
	private static long total(Object msg) {
		if (msg instanceof ByteBuf) {
			return ((ByteBuf) msg).readableBytes();
		}
		if (msg instanceof FileRegion) {
			return ((FileRegion) msg).count();
		}
		if (msg instanceof ByteBufHolder) {
			return((ByteBufHolder) msg).content().readableBytes();
		}
		return -1;
	}
	
	public Object current() {
		Entry entry = flushedEntry;
		if (entry == null) {
			return null;
		}
		return entry.msg;
	}
	
	public long currentProgress() {
		Entry entry = flushedEntry;
		if (entry == null) {
			return 0;
		}
		return entry.progress;
	}
	
	public void progress(long amount) {
		Entry e = flushedEntry;
		assert e != null;
		ChannelPromise p = e.promise;
		long progress = e.progress + amount;
		e.progress = progress;
		assert p != null;
		final Class<?> promiseClass = p.getClass();
		if (promiseClass == VoidChannelPromise.class || promiseClass == DefaultChannelPromise.class) {
			return;
		}
		if (p instanceof DefaultChannelProgressivePromise) {
			((DefaultChannelProgressivePromise) p).tryProgress(progress, e.total);
		} else if (p instanceof ChannelProgressivePromise) {
			((ChannelProgressivePromise) p).tryProgress(progress, e.total);
		}
	}
	
	public boolean remove() {
		Entry e = flushedEntry;
		if (e == null) {
			clearNioBuffers();
			return false;
		}
		Object msg = e.msg;
		
		ChannelPromise promise = e.promise;
		int size = e.pendingSize;
		
		removeEntry(e);
		
		if (!e.cancelled) {
			if (msg instanceof AbstractReferenceCountedByteBuf) {
				try {
					((AbstractReferenceCountedByteBuf) msg).release();
				} catch (Throwable t) {
					logger.warn("Failed to release a ByteBuf: {}", msg, t);
				}
			} else {
				ReferenceCountUtil.safeRelease(msg);
			}
			safeSuccess(promise);
			decrementPendingOutboundBytes(size, false, true);
		}
		e.unguardedRecycle();
		return true;
	}
	
	public boolean remove(Throwable cause) {
		return remove0(cause, true);
	}
	
	private boolean remove0(Throwable cause, boolean notifyWritability) {
		Entry e = flushedEntry;
		if (e == null) {
			clearNioBuffers();
			return false;
		}
		
		Object msg = e.msg;
		ChannelPromise promise = e.promise;
		int size = e.pendingSize;
		removeEntry(e);
		
		if (!e.cancelled) {
			ReferenceCountUtil.safeRelease(msg);
			
			safeFail(promise, cause);
			decrementPendingOutboundBytes(size, false, notifyWritability);
		}
		
		e.unguardedRecycle();
		return true;
	}
	
	private void removeEntry(Entry e) {
		if (--flushed == 0) {
			flushedEntry = null;
			if (e == tailEntry) {
				tailEntry = null;
				unflushedEntry = null;
			}
		} else {
			flushedEntry = e.next;
		}
	}
	
	public void removeBytes(long writtenBytes) {
		for (;;) {
			Object msg = current();
			if (!(msg instanceof ByteBuf)) {
				assert writtenBytes == 0;
				break;
			}
			
			final ByteBuf buf = (ByteBuf) msg;
			final int readerIndex = buf.readerIndex();
			final int readableBytes = buf.writerIndex() - readerIndex;
			
			if (readableBytes <= writtenBytes) {
				if (writtenBytes != 0) {
					progress(readableBytes);
					writtenBytes -= readableBytes;
				}
				remove();
			} else {
				if (writtenBytes != 0) {
					buf.readerIndex(readerIndex + (int) writtenBytes);
					progress(writtenBytes);
				}
				break;
			}
		}
		clearNioBuffers();
	}
	
	private void clearNioBuffers() {
		int count = nioBufferCount;
		if (count > 0) {
			nioBufferCount = 0;
			Arrays.fill(NIO_BUFFERS.get(), 0, count, null);
		}
	}
	
	public ByteBuffer[] nioBuffers() {
		return nioBuffers(Integer.MAX_VALUE, Integer.MAX_VALUE);
	}
	
	public ByteBuffer[] nioBuffers(int maxCount, long maxBytes) {
		assert maxCount > 0;
		assert maxBytes > 0;
		long nioBufferSize = 0;
		int nioBufferCount = 0;
		final InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
		ByteBuffer[] nioBuffers = NIO_BUFFERS.get(threadLocalMap);
		Entry entry = flushedEntry;
		while (isFlushedEntry(entry) && entry.msg instanceof ByteBuf) {
			if (!entry.cancelled) {
				ByteBuf buf = (ByteBuf) entry.msg;
				final int readerIndex = buf.readerIndex();
				final int readableBytes = buf.writerIndex() - readerIndex;
				
				if (readableBytes > 0) {
					if (maxBytes - readableBytes < nioBufferSize && nioBufferCount != 0) {
						break;
					}
					nioBufferSize += readableBytes;
					int count = entry.count;
					if (count == -1) {
						entry.count = count = buf.nioBufferCount();
					}
					int neededSpace = Math.min(maxCount, nioBufferCount + count);
					if (neededSpace > nioBuffers.length) {
						nioBuffers = expandNioBufferArray(nioBuffers, neededSpace, nioBufferCount);
						NIO_BUFFERS.set(threadLocalMap, nioBuffers);
					}
					if (count == 1) {
						ByteBuffer nioBuf = entry.buf;
						if (nioBuf == null) {
							entry.buf = nioBuf = buf.internalNioBuffer(readerIndex, readableBytes);
						}
						nioBuffers[nioBufferCount++] = nioBuf;
					} else {
						nioBufferCount = nioBuffers(entry, buf, nioBuffers, nioBufferCount, maxCount);
					}
					if (nioBufferCount >= maxCount) {
						break;
					}
				}
			}
			entry = entry.next;
		}
		this.nioBufferCount = nioBufferCount;
		this.nioBufferSize = nioBufferSize;
		
		return nioBuffers;
	}
	
	private static int nioBuffers(Entry entry, ByteBuf buf, ByteBuffer[] nioBuffers, int nioBufferCount, int maxCount) {
		ByteBuffer[] nioBufs = entry.bufs;
		if (nioBufs == null) {
			entry.bufs = nioBufs = buf.nioBuffers();
		}
		for (int i = 0; i < nioBufs.length && nioBufferCount < maxCount; ++i) {
			ByteBuffer nioBuf = nioBufs[i];
			if (nioBuf == null) {
				break;
			} else if (!nioBuf.hasRemaining()) {
				continue;
			}
			nioBuffers[nioBufferCount++] = nioBuf;
		}
		return nioBufferCount;
	}
	
	private static ByteBuffer[] expandNioBufferArray(ByteBuffer[] array, int neededSpace, int size) {
		int newCapacity = array.length;
		do {
			newCapacity <<= 1;
			if (newCapacity < 0) {
				throw new IllegalStateException();
			}
		} while (neededSpace > newCapacity);
		
		ByteBuffer[] newArray = new ByteBuffer[newCapacity];
		System.arraycopy(array, 0, newArray, 0, size);
		return newArray;
	}
	
	public int nioBufferCount() {
		return nioBufferCount;
	}
	
	public long nioBufferSize() {
		return nioBufferSize;
	}
	
	public boolean isWritable() {
		return unwritable == 0;
	}
	
	public boolean getUserDefinedWritability(int index) {
		return (unwritable & writabilityMask(index)) == 0;
	}
	
	public void setUsedDefinedWritability(int index, boolean writable) {
		if (writable) {
			setUserDefinedWritability(index);
		} else {
			clearUserDefinedWritability(index);
		}
	}
	
	private void setUserDefinedWritability(int index) {
		final int mask = ~writabilityMask(index);
		for (;;) {
			final int oldValue = unwritable;
			final int newValue = oldValue & mask;
			if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
				if (oldValue != 0 && newValue == 0) {
					fireChannelWritablityChanged(true);
				}
				break;
			}
		}
	}
	
	private void clearUserDefinedWritability(int index) {
		final int mask = writabilityMask(index);
		for (;;) {
			final int oldValue = unwritable;
			final int newValue = oldValue | mask;
			if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
				if (oldValue == 0 && newValue != 0) {
					fireChannelWritablityChanged(true);
				}
				break;
			}
		}
	}
	
	private static int writabilityMask(int index) {
		if (index < 1 || index > 31) {
			throw new IllegalArgumentException("index: " + index + " (expected: 1~31)");
		}
		return 1 << index;
	}
	
	private void setWritable(boolean invokeLater) {
		for (;;) {
			final int oldValue = unwritable;
			final int newValue = oldValue & ~1;
			if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
				if (oldValue != 0 && newValue == 0) {
					fireChannelWritabilityChanged(invokeLater);
				}
				break;
			}
		}
	}
	
	private void setUnwritable(boolean invokeLater) {
		for (;;) {
			final int oldValue = unwritable;
			final int newValue = oldValue | 1;
			if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
				if (oldValue == 0) {
					fireChannelWritabilityChanged(invokeLater);
				}
				break;
			}
		}
	}
	
	private void fireChannelWritabilityChanged(boolean invokeLater) {
		final ChannelPipeline pipeline = channel.pipeline();
		if (invokeLater) {
			Runnable task = fireChannelWritabilityChangedTask;
			if (task == null) {
				fireChannelWritabilityChangedTask = task = new Runnable() {
					@Override
					public void run() {
						pipeline.fireChannelWritabilityChanged();
					}
				};
			}
			channel.eventLoop().execute(task);
		} else {
			pipeline.fireChannelWritabilityChanged();
		}
	}
	
	public int size() {
		return flushed;
	}
	
	public boolean isEmpty() {
		return flushed == 0;
	}
	
	void failFlushed(Throwable cause, boolean notify) {
		if (inFail) {
			return;
		}
		try {
			inFail = true;
			for (;;) {
				if (!remove0(cause, notify)) {
					break;
				}
			}
		} finally {
			inFail = false;
		}
	}
	
	static final class Entry {
		private static final Recycler<Entry> RECYCLER = new Recycler<Entry>() {
			@Override
			protected Entry newObject(Handle<Entry> handle) {
				return new Entry(handle);
			}
		};
		
		private final EnhancedHandle<Entry> handle;
		Entry next;
		Object msg;
		ByteBuffer[] bufs;
		ByteBuffer buf;
		ChannelPromise promise;
		long progress;
		long total;
		int pendingSize;
		int count = -1;
		boolean cancelled;
		
		private Entry(Handle<Entry> handle) {
			this.handle = (EnhancedHandle<Entry>) handle;
		}
		
		static Entry newInstance(Object msg, int size, long total, ChannelPromise promise) {
			Entry entry = RECYCLER.get();
			entry.msg = msg;
			entry.pendingSize = size + CHANNEL_OUTBOUND_BUFFER_ENTRY_OVERHEAD;
			entry.total = total;
			entry.promise = promise;
			return entry;
		}
		
		int cancel() {
			if (!cancelled) {
				cancelled = true;
				int pSize = pendingSize;
				
				ReferenceCountUtil.safeRelease(msg);
				msg = Unpooled.EMPTY_BUFFER;
				
				pendingSize = 0;
				total = 0;
				progress = 0;
				bufs = null;
				buf = null;
				return pSize;
			}
			return 0;
		}
		
		void unguardedRecycle() {
			next = null;
			bufs = null;
			buf = null;
			msg = null;
			promise = null;
			progress = 0;
			total = 0;
			pendingSize = 0;
			count = -1;
			cancelled = false;
			handle.unguardedRecycle(this);
		}
		
		Entry unguardedRecycleAndGetNext() {
			Entry next = this.next;
			unguardedRecycle();
			return next;
		}
	}
}
