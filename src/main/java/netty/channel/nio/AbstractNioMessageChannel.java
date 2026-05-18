package netty.channel.nio;

import java.io.IOException;
import java.net.PortUnreachableException;
import java.nio.channels.SelectableChannel;
import java.util.ArrayList;
import java.util.List;

import netty.channel.Channel;
import netty.channel.ChannelConfig;
import netty.channel.ChannelOutboundBuffer;
import netty.channel.ChannelPipeline;
import netty.channel.RecvByteBufAllocator;
import netty.channel.ServerChannel;

public abstract class AbstractNioMessageChannel extends AbstractNioChannel {
	boolean inputShutdown;
	
	protected AbstractNioMessageChannel(Channel parent, SelectableChannel ch, int readInterestOp) {
		super(parent, ch, readInterestOp);
	}
	
	protected AbstractNioMessageChannel(Channel parent, SelectableChannel ch, NioIoOps readOps) {
		super(parent, ch, readOps);
	}
	
	@Override
	protected AbstractNioUnsafe newUnsafe() {
		return new NioMessageUnsafe();
	}
	
	@Override
	protected void doBeginRead() throws Exception {
		if (inputShutdown) {
			return;
		}
		super.doBeginRead();
	}
	
	protected boolean continueReading(RecvByteBufAllocator.Handle allocHandle) {
		return allocHandle.continueReading();
	}
	
	private final class NioMessageUnsafe extends AbstractNioUnsafe {
		private final List<Object> readBuf = new ArrayList<Object>();
	
		@Override
		public void read() {
			assert eventLoop().inEventLoop();
			final ChannelConfig config = config();
			final ChannelPipeline pipeline = pipeline();
			final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
			allocHandle.reset(config);
			
			boolean closed = false;
			Throwable exception = null;
			try {
				try {
					do {
						int localRead = doReadMessage(readBuf);
						if (localRead == 0) {
							break;
						}
						if (localRead < 0 ) {
							closed = true;
							break;
						}
						
						allocHandle.incMessagesRead(localRead);
					} while (continueReading(allocHandle));
				} catch (Throwable t) {
					exception = t;
				}
				
				int size = readBuf.size();
				for (int i = 0; i < size; i++) {
					readPending = false;
					pipeline.fireChannelRead(readBuf.get(i));
				}
				readBuf.clear();
				allocHandle.readComplete();
				pipeline.fireChannelReadComplete();
				if (exception != null) {
					closed = closeOnReadError(exception);
					pipeline.fireExceptionCaught(exception);
				}
				
				if (closed) {
					inputShutdown = true;
					if (isOpen()) {
						close(voidPromise());
					}
				}
			} finally {
				if (!readPending && !config.isAutoRead()) {
					removeReadOp();
				}
			}
		}
	}
	
	@Override
	protected void doWrite(ChannelOutboundBuffer in) throws Exception {
		int maxMessagesPerWrite = maxMessagesPerWrite();
		while (maxMessagesPerWrite > 0) {
			Object msg = in.current();
			if (msg == null) {
				break;
			}
			try {
				boolean done = false;
				for (int i = config().getWriteSpinCount() -1; i >= 0; i--) {
					if (doWriteMessage(msg, in)) {
						done = true;
						break;
					}
				}
				
				if (done) {
					maxMessagesPerWrite--;
					in.remove();
				} else {
					break;
				}
			} catch (Exception e) {
				if (continueOnWriteError()) {
					maxMessagesPerWrite--;
					in.remove(e);
				} else {
					throw e;
				}
			}
		}
		if (in.isEmpty()) {
			removeAndSubmit(NioIoOps.WRITE);
		} else {
			addAndSubmit(NioIoOps.WRITE);
		}
	}
	
	protected boolean continueOnWriteError() {
		return false;
	}
	
	protected boolean closeOnReadError(Throwable cause) {
		if (!isActive()) {
			return true;
		}
		if (cause instanceof PortUnreachableException) {
			return false;
		}
		if (cause instanceof IOException) {
			return !(this instanceof ServerChannel);
		}
		return true;
	}
	
	protected abstract int doReadMessage(List<Object> buf ) throws Exception;
	
	protected abstract boolean doWriteMessage(Object msg, ChannelOutboundBuffer in) throws Exception;
}
