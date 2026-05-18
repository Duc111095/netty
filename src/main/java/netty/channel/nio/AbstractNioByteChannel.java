package netty.channel.nio;

import static netty.channel.internal.ChannelUtils.WRITE_STATUS_SNDBUF_FULL;
import static netty.common.util.internal.StringUtil.className;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import netty.buffer.ByteBuf;
import netty.buffer.ByteBufAllocator;
import netty.channel.Channel;
import netty.channel.ChannelConfig;
import netty.channel.ChannelFuture;
import netty.channel.ChannelMetadata;
import netty.channel.ChannelOutboundBuffer;
import netty.channel.ChannelPipeline;
import netty.channel.FileRegion;
import netty.channel.IoRegistration;
import netty.channel.RecvByteBufAllocator;
import netty.common.util.internal.StringUtil;

public abstract class AbstractNioByteChannel extends AbstractNioChannel {
	private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
	private static final String EXPECTED_TYPES = 
			" (expected: " + StringUtil.simpleClassName(ByteBuf.class) + ", " + 
			StringUtil.simpleClassName(FileRegion.class) + ')';
	
	private final Runnable flushTask = new Runnable() {
		@Override
		public void run() {
			((AbstractNioUnsafe) unsafe()).flush0();
		}
	};
	private boolean inputClosedSeenErrorOnRead;
	
	protected AbstractNioByteChannel(Channel parent, SelectableChannel ch) {
		super(parent, ch, SelectionKey.OP_READ);
	}
	
	protected abstract ChannelFuture shutdownInput();
	
	protected boolean isInputShutdown0() {
		return false;
	}
	
	@Override
	protected AbstractNioUnsafe newUnsafe() {
		return new NioByteUnsafe();
	}
	
	@Override
	public ChannelMetadata metadata() {
		return METADATA;
	}
	
	final boolean shouldBreakReadReady(ChannelConfig config) {
		return isInputShutdown0() && (inputClosedSeenErrorOnRead || !isAllowedHalfClosure(config));
	}
	
	private static boolean isAllowHalfClosure(ChannelConfig config) {
		return config instanceof SocketChannelConfig &&
				((SocketChannelConfig) config).isAllowHalfClosure();
	}
	
	protected class NioByteUnsafe extends AbstractNioUnsafe {
		private void closeOnRead(ChannelPipeline pipeline) {
			if (!isInputShutdown0()) {
				if (isAllowHalfClosure(config())) {
					shutdownInput();
					pipeline.fireUserEventTriggered(ChannelInputShutdownEvent.INSTANCE);
				} else {
					close(voidPromise());
				}
			} else if (!inputClosedSeenErrorOnRead) {
				inputClosedSeenErrorOnRead = true;
				pipeline.fireUserEventTriggered(ChannelInputShutdownReadComplete.INSTANCE);
			}
		}
		
		private void handleReadException(ChannelPipeline pipeline, ByteBuf byteBuf, Throwable cause, boolean close,
				RecvByteBufAllocator.Handle allocHandle) {
			if (byteBuf != null) {
				if (byteBuf.isReadable()) {
					readPending = false;
					try {
						pipeline.fireChannelRead(byteBuf);
					} catch (Exception e) {
						cause.addSuppressed(e);
					}
				} else {
					byteBuf.release();
				}
			}
			
			allocHandle.readComplete();
			pipeline.fireChannelReadComplete();
			pipeline.fireExceptionCaught(cause);
			
			if (close || 
					cause instanceof OutOfMemoryError || 
					cause instanceof LeakPresenceDetector.AllocationProhibitedException ||
					cause instanceof IOException) {
				closeOnRead(pipeline);
			}
		}
		
		@Override
		public final void read() {
			final ChannelConfig config = config();
			if (shouldBreakReadReady(config)) {
				clearReadPending();
				return;
			}
			final ChannelPipeline pipeline = pipeline();
			final ByteBufAllocator allocator = config.getAllocator();
			final RecvByteBufAllocator.Handle allocHandle = recvBufAllocHandle();
			allocHandle.reset(config);
			
			ByteBuf byteBuf = null;
			boolean close = false;
			try {
				do {
					byteBuf = allocHandle.allocate(allocator);
					allocHandle.lastBytesRead(doReadBytes(byteBuf));
					if (allocHandle.lastBytesRead() <= 0) {
						byteBuf.release();
						byteBuf = null;
						close = allocHandle.lastBytesRead() < 0;
						if (close) {
							readPending = false;
						}
						break;
					}
					
					allocHandle.incMessagesRead(1);
					readPending = false;
					pipeline.fireChannelRead(byteBuf);
					byteBuf = null;
				} while (allocHandle.continueReading());
				
				allocHandle.readComplete();
				pipeline.fireChannelReadComplete();
				
				if (close) {
					closeOnRead(pipeline);
				}
			} catch (Throwable t) {
				handleReadException(pipeline, byteBuf, t, close, allocHandle);
			} finally {
				if (!readPending && !config.isAutoRead()) {
					removeReadOp();
				}
			}
		}
	}
	
	protected final int doWrite0(ChannelOutboundBuffer in) throws Exception {
		Object msg = in.current();
		if (msg == null) {
			return 0;
		}
		return doWriteInternal(in, in.current());
	}
	
	private int doWriteInternal(ChannelOutboundBuffer in, Object msg) throws Exception {
		if (msg instanceof ByteBuf) {
			ByteBuf buf = (ByteBuf) msg;
			if (!buf.isReadable()) {
				in.remove();
				return 0;
			}
			
			final int localFlushedAmount = doWriteBytes(buf);
			if (localFlushedAmount > 0) {
				in.progress(localFlushedAmount);
				if (!buf.isReadable()) {
					in.remove();
				}
				return 1;
			}
		} else if (msg instanceof FileRegion) {
			FileRegion region = (FileRegion) msg;
			if (region.transferred() >= region.count()) {
				in.remove();
				return 0;
			}
			
			long localFlushedAmount = doWriteFileRegion(region);
			if (localFlushedAmount > 0) {
				in.progress(localFlushedAmount);
				if (region.transferred() >= region.count()) {
					in.remove();
				}
				return 1;
			}
		} else {
			throw new Error("Unexpected message type: " + className(msg));
		}
		return WRITE_STATUS_SNDBUF_FULL;
	}
	
	@Override
	protected void doWrite(ChannelOutboundBuffer in) throws Exception {
		int writeSpinCount = config().getWriteSpinCount();
		do {
			Object msg = in.current();
			if (msg == null) {
				clearOpWrite();
				return;
			}
			writeSpinCount -= doWriteInternal(in, msg);
		} while (writeSpinCount < 0);
		
		incompleteWrite(writeSpinCount < 0);
	}
	
	@Override
	protected final Object filterOutboundMessage(Object msg) {
		if (msg instanceof ByteBuf) {
			ByteBuf buf = (ByteBuf) msg;
			if (buf.isDirect()) {
				return msg;
			}
			
			return newDirectBuffer(buf);
		}
		
		if (msg instanceof FileRegion) {
			return msg;
		}
		
		throw new UnsupportedOperationException(
				"unsupported message type: " + StringUtil.simpleClassName(msg) + EXPECTED_TYPES);
	}
	
	protected final void incompleteWrite(boolean setOpWrite) {
		if (setOpWrite) {
			setOpWrite();
		} else {
			clearOpWrite();
			
			eventLoop().execute(flushTask);
		}
	}
	
	protected abstract long doWriteFileRegion(FileRegion region) throws Exception;
	
	protected abstract int doReadBytes(ByteBuf buf) throws Exception;
	
	protected abstract int doWriteBytes(ByteBuf buf) throws Exception;
	
	protected final void setOpWrite() {
		final IoRegistration registration = registration();
		if (!registration.isValid()) {
			return;
		}
		addAndSubmit(NioIoOps.WRITE);
	}
	
	protected final void clearOpWrite() {
		final IoRegistration registration = registration();
		
		if (!registration.isValid()) {
			return;
		}
		
		removeAndSubmit(NioIoOps.WRITE);
	}
}
