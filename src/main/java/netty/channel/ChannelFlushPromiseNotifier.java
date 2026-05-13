package netty.channel;

import java.util.ArrayDeque;
import java.util.Queue;

import netty.common.util.internal.ObjectUtil;

public final class ChannelFlushPromiseNotifier {
	
	private long writeCounter;
	private final Queue<FlushCheckpoint> flushCheckpoints = new ArrayDeque<FlushCheckpoint>();
	private final boolean tryNotify;
	
	public ChannelFlushPromiseNotifier(boolean tryNotify) {
		this.tryNotify = tryNotify;
	}
	
	public ChannelFlushPromiseNotifier() {
		this(false);
	}
	
	public ChannelFlushPromiseNotifier add(ChannelPromise promise, int pendingDataSize) {
		return add(promise, (long) pendingDataSize);
	}
	
	public ChannelFlushPromiseNotifier add(ChannelPromise promise, long pendingDataSize) {
		ObjectUtil.checkNotNull(promise, "promise");
		ObjectUtil.checkPositiveOrZero(pendingDataSize, "pendingDataSize");
		long checkpoint = writeCounter + pendingDataSize;
		if (promise instanceof FlushCheckpoint) {
			FlushCheckpoint cp = (FlushCheckpoint) promise;
			cp.flushCheckpoint(checkpoint);
			flushCheckpoints.add(cp);
		} else {
			flushCheckpoints.add(new DefaultFlushCheckpoint(checkpoint, promise));
		}
		return this;
	}
	
	public ChannelFlushPromiseNotifier increaseWriteCounter(long delta) {
		ObjectUtil.checkPositiveOrZero(delta, "delta");
		writeCounter += delta;
		return this;
	}
	
	public long writeCounter() {
		return writeCounter;
	}
	
	public ChannelFlushPromiseNotifier notifyPromises() {
		notifyPromises0(null);
		return this;
	}
	
	public ChannelFlushPromiseNotifier notifyFlushFutures() {
		return notifyPromises();
	}
	
	public ChannelFlushPromiseNotifier notifyPromises(Throwable cause) {
		notifyPromises();
		for (;;) {
			FlushCheckpoint cp = flushCheckpoints.poll();
			if (cp == null) {
				break;
			}
			if (tryNotify) {
				cp.promise().tryFailure(cause);
			} else {
				cp.promise().setFailure(cause);
			}
		}
		return this;
	}
	
	public ChannelFlushPromiseNotifier notifyPromises(Throwable cause1, Throwable cause2) {
        notifyPromises0(cause1);
        for (;;) {
            FlushCheckpoint cp = flushCheckpoints.poll();
            if (cp == null) {
                break;
            }
            if (tryNotify) {
                cp.promise().tryFailure(cause2);
            } else {
                cp.promise().setFailure(cause2);
            }
        }
        return this;
    }

	public ChannelFlushPromiseNotifier notifyFlushFutures(Throwable cause1, Throwable cause2) {
        return notifyPromises(cause1, cause2);
    }
	
	private void notifyPromises0(Throwable cause) {
		if (flushCheckpoints.isEmpty()) {
			writeCounter = 0;
			return;
		}
		
		final long writeCounter = this.writeCounter;
		for (;;) {
			FlushCheckpoint cp = flushCheckpoints.peek();
			if (cp == null) {
				this.writeCounter = 0;
				break;
			}
			if (cp.flushCheckpoint() > writeCounter) {
				if (writeCounter > 0 && flushCheckpoints.size() == 1) {
					this.writeCounter = 0;
					cp.flushCheckpoint(cp.flushCheckpoint() - writeCounter);
				}
				break;
			}
			
			flushCheckpoints.remove();
			ChannelPromise promise = cp.promise();
			if (cause == null) {
				if (tryNotify) {
					promise.trySuccess();
				} else {
					promise.setSuccess();
				}
			} else {
				if (tryNotify) {
					promise.tryFailure(cause);
				} else {
					promise.setFailure(cause);
				}
			}
		}
		
		final long newWriterCounter = this.writeCounter;
		if (newWriterCounter >= 0x8000000000L) {
			this.writeCounter = 0;
			for (FlushCheckpoint cp : flushCheckpoints) {
				cp.flushCheckpoint(cp.flushCheckpoint() - newWriterCounter);
			}
		}
	}
	
	interface FlushCheckpoint {
		long flushCheckpoint();
		void flushCheckpoint(long checkpoint);
		ChannelPromise promise();
	}
	
	private static class DefaultFlushCheckpoint implements FlushCheckpoint {
		private long checkpoint;
		private final ChannelPromise future;
		
		DefaultFlushCheckpoint(long checkpoint, ChannelPromise future) {
			this.checkpoint = checkpoint;
			this.future = future;
		}
		
		@Override
		public long flushCheckpoint() {
			return checkpoint;
		}
		
		@Override
		public void flushCheckpoint(long checkpoint) {
			this.checkpoint = checkpoint;
		}
		
		@Override
		public ChannelPromise promise() {
			return future;
		}
	}
}
