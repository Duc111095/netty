package netty.channel.socket.nio;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Map;
import java.util.concurrent.Executor;

import netty.buffer.ByteBuf;
import netty.channel.Channel;
import netty.channel.ChannelException;
import netty.channel.ChannelFuture;
import netty.channel.ChannelFutureListener;
import netty.channel.ChannelOption;
import netty.channel.ChannelOutboundBuffer;
import netty.channel.ChannelPromise;
import netty.channel.EventLoop;
import netty.channel.FileRegion;
import netty.channel.RecvByteBufAllocator;
import netty.channel.nio.AbstractNioByteChannel;
import netty.channel.nio.NioIoOps;
import netty.channel.socket.DefaultSocketChannelConfig;
import netty.channel.socket.ServerSocketChannel;
import netty.channel.socket.SocketChannelConfig;
import netty.channel.socket.SocketProtocolFamily;
import netty.common.util.concurrent.GlobalEventExecutor;
import netty.common.util.internal.SocketUtils;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static netty.channel.internal.ChannelUtils.MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD;

public class NioSocketChannel extends AbstractNioByteChannel implements netty.channel.socket.SocketChannel {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioSocketChannel.class);
	private static final SelectorProvider DEFAULT_SELECTOR_PROVIDER = SelectorProvider.provider();
	
	private static final Method OPEN_SOCKET_CHANNEL_WITH_FAMILY = 
			SelectorProviderUtil.findOpenMethod("openSocketChannel");
	private final SocketChannelConfig config;
	
	private static SocketChannel newChannel(SelectorProvider provider, SocketProtocolFamily family) {
		try {
			SocketChannel channel = SelectorProviderUtil.newChannel(OPEN_SOCKET_CHANNEL_WITH_FAMILY, provider, family);
			return channel == null ? provider.openSocketChannel() : channel;
		} catch (IOException e) {
			throw new ChannelException("Failed to open a socket.", e);
		}
	}
	
	public NioSocketChannel() {
		this(DEFAULT_SELECTOR_PROVIDER);
	}
	
	public NioSocketChannel(SelectorProvider provider) {
		this(provider, (SocketProtocolFamily) null);
	}
	
	public NioSocketChannel(SelectorProvider provider, SocketProtocolFamily family) {
		this(newChannel(provider, family));
	}
	
	public NioSocketChannel(SocketChannel socket) {
		this(null, socket);
	}
	
	public NioSocketChannel(Channel parent, SocketChannel socket) {
		super(parent, socket);
		config = new NioSocketChannelConfig(this, socket.socket());
	}
	
	@Override
	public ServerSocketChannel parent() {
		return (ServerSocketChannel) super.parent();
	}
	
	@Override
	public SocketChannelConfig config() {
		return config;
	}
	
	@Override
	protected SocketChannel javaChannel() {
		return (SocketChannel) super.javaChannel();
	}
	
	@Override
	public boolean isActive() {
		SocketChannel ch = javaChannel();
		return ch.isOpen() && ch.isConnected();
	}
	
	@Override
	public boolean isOutputShutdown() {
		return javaChannel().socket().isOutputShutdown() || !isActive();
	}
	
	@Override
	public boolean isInputShutdown() {
		return javaChannel().socket().isInputShutdown() || !isActive();
	}
	
	@Override
	public boolean isShutdown() {
		Socket socket = javaChannel().socket();
		return socket.isInputShutdown() && socket.isOutputShutdown() || !isActive();
	}
	
	@Override
	public InetSocketAddress localAddress() {
		return (InetSocketAddress) super.localAddress();
	}
	
	@Override
	public InetSocketAddress remoteAddress() {
		return (InetSocketAddress) super.remoteAddress();
	}
	
	@Override
	protected final void doShutdownOutput() throws Exception {
		javaChannel().shutdownOutput();
	}
	
	@Override
	public ChannelFuture shutdownOutput() {
		return shutdownOutput(newPromise());
	}
	
	@Override
	public ChannelFuture shutdownOutput(final ChannelPromise promise) {
		final EventLoop loop = eventLoop();
		if (loop.inEventLoop()) {
			((AbstractUnsafe) unsafe()).shutdownOutput(promise);
		} else {
			loop.execute(new Runnable() {
				@Override
				public void run() {
					((AbstractUnsafe) unsafe()).shutdownOutput(promise);
				}
			});
		}
		return promise;
	}
	
	@Override
	public ChannelFuture shutdownInput() {
		return shutdownInput(newPromise());
	}
	
	@Override
	protected boolean isInputShutdown0() {
		return isInputShutdown();
	}
	
	@Override
	public ChannelFuture shutdownInput(final ChannelPromise promise) {
		EventLoop loop = eventLoop();
		if (loop.inEventLoop()) {
			shutdownInput0(promise);
		} else {
			loop.execute(new Runnable() {
				@Override
				public void run() {
					shutdownInput0(promise);
				}
			});
		}
		return promise;
	}
	
	@Override
	public ChannelFuture shutdown() {
		return shutdown(newPromise());
	}
	
	@Override
	public ChannelFuture shutdown(final ChannelPromise promise) {
		ChannelFuture shutdownOutputFuture = shutdownOutput();
		if (shutdownOutputFuture.isDone()) {
			shutdownOutputDone(shutdownOutputFuture, promise);
		} else {
			shutdownOutputFuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(final ChannelFuture shutdownOutputFuture) throws Exception {
					shutdownOutputDone(shutdownOutputFuture, promise);
				}
			});
		}
		return promise;
	}
	
	private void shutdownOutputDone(final ChannelFuture shutdownOutputFuture, final ChannelPromise promise) {
		ChannelFuture shutdownInputFuture = shutdownInput();
		if (shutdownInputFuture.isDone()) {
			shutdownDone(shutdownOutputFuture, shutdownInputFuture, promise);
		} else {
			shutdownInputFuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture shutdownInputFuture) throws Exception {
					shutdownDone(shutdownOutputFuture, shutdownInputFuture, promise);
				}
			});
		}
	}
	
	private static void shutdownDone(ChannelFuture shutdownOutputFuture,
			ChannelFuture shutdownInputFuture,
			ChannelPromise promise) {
		Throwable shutdownOutputCause = shutdownOutputFuture.cause();
		Throwable shutdownInputCause = shutdownInputFuture.cause();
		if (shutdownOutputCause != null) {
			if (shutdownInputCause != null) {
				logger.debug("Exception suppressed because a previous exception occurred.",
						shutdownInputCause);
			}
			promise.setFailure(shutdownOutputCause);
		} else if (shutdownInputCause != null) {
			promise.setFailure(shutdownInputCause);
		} else {
			promise.setSuccess();
		}
	}
	
	private void shutdownInput0(final ChannelPromise promise) {
		try {
			shutdownInput0();
			promise.setSuccess();
		} catch (Throwable t) {
			promise.setFailure(t);
		}
	}
	
	private void shutdownInput0() throws Exception{
		javaChannel().shutdownInput();
	}
	
	@Override
	protected SocketAddress localAddress0() {
		return javaChannel().socket().getLocalSocketAddress();
	}
	
	@Override
	protected SocketAddress remoteAddress0() {
		return javaChannel().socket().getRemoteSocketAddress();
	}
	
	@Override
	protected void doBind(SocketAddress localAddress) throws Exception {
		doBind0(localAddress);
	}
	
	private void doBind0(SocketAddress localAddress) throws Exception {
		SocketUtils.bind(javaChannel(), localAddress);
	}
	
	@Override
	protected boolean doConnect(SocketAddress remoteAddress, SocketAddress localAddress) throws Exception {
		if (localAddress != null) {
			doBind0(localAddress);
		}
		
		boolean success = false;
		try {
			boolean connected = SocketUtils.connect(javaChannel(), remoteAddress);
			if (!connected) {
				addAndSubmit(NioIoOps.CONNECT);
			}
			success = true;
			return connected;
		} finally {
			if (!success) {
				doClose();
			}
		}
	}
	
	@Override
	protected void doFinishConnect() throws Exception {
		if (!javaChannel().finishConnect()) {
			throw new UnsupportedOperationException("finishConnect is not supported for " + getClass().getName());
		} 
	}
	
	@Override
	protected void doDisconnect() throws Exception {
		doClose();
	}
	
	@Override
	protected void doClose() throws Exception {
		super.doClose();
		javaChannel().close();
	}
	
	@Override
	protected int doReadBytes(ByteBuf byteBuf) throws Exception {
		final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
		allocHandle.attemptedBytesRead(byteBuf.writableBytes());
		return byteBuf.writeBytes(javaChannel(), allocHandle.attemptedBytesRead());
	}
	
	
	@Override
	protected int doWriteBytes(ByteBuf buf) throws Exception {
		final int expectedWrittenBytes = buf.readableBytes();
		return buf.readBytes(javaChannel(), expectedWrittenBytes);
	}
	
	@Override
	protected long doWriteFileRegion(FileRegion region) throws Exception {
		final long position = region.transferred();
		return region.transferTo(javaChannel(), position);
	}
	
	private void adjustMaxBytesPerGatheringWrite(int attempted, int written, int oldMaxBytesPerGatheringWrite) {
		if (attempted == written) {
			if (attempted << 1 > oldMaxBytesPerGatheringWrite) {
				((NioSocketChannelConfig) config).setMaxBytesPerGatheringWrite(attempted << 1); 
			} 
		} else if (attempted > MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD && written < attempted >>> 1) {
			((NioSocketChannelConfig) config).setMaxBytesPerGatheringWrite(attempted >>> 1); 
		}
	}
	
	@Override
	protected void doWrite(ChannelOutboundBuffer in) throws Exception {
		SocketChannel ch = javaChannel();
		int writeSpinCount = config().getWriteSpinCount();
		do {
			if (in.isEmpty()) {
				clearOpWrite();
				return;
			}
			
			int maxBytesPerGatheringWrite = ((NioSocketChannelConfig) config).getMaxBytesPerGatheringWrite();
			ByteBuffer[] nioBuffers = in.nioBuffers(1024, maxBytesPerGatheringWrite);
			int nioBufferCnt = in.nioBufferCount();
			
			switch (nioBufferCnt) {
			case 0:
				writeSpinCount -= doWrite0(in);
				break;
			case 1: {
				ByteBuffer buffer = nioBuffers[0];
				int attemptedBytes = buffer.remaining();
				final int localWrittenBytes = ch.write(buffer);
				if (localWrittenBytes <= 0) {
					incompleteWrite(true);
					return;
				}
				adjustMaxBytesPerGatheringWrite(attemptedBytes, localWrittenBytes, maxBytesPerGatheringWrite);
				in.removeBytes(localWrittenBytes);
				--writeSpinCount;
				break;
			}
			default: {
				long attemptedBytes = in.nioBufferSize();
				final long localWrittenBytes = ch.write(nioBuffers, 0, nioBufferCnt);
				if (localWrittenBytes <= 0) {
					incompleteWrite(true);
					return;
				}
				adjustMaxBytesPerGatheringWrite((int) attemptedBytes, (int) localWrittenBytes,
						maxBytesPerGatheringWrite);
				in.removeBytes(localWrittenBytes);
				--writeSpinCount;
				break;
			}
			}
		} while (writeSpinCount > 0);
		
		incompleteWrite(writeSpinCount < 0);
	}
	
	@Override
	protected AbstractNioUnsafe newUnsafe() {
		return new NioSocketChannelUnsafe();
	}
	
	private final class NioSocketChannelUnsafe extends NioByteUnsafe {
		@Override
		protected Executor prepareToClose() {
			try {
				if (javaChannel().isOpen() && config().getSoLinger() > 0) {
					doDeregister();
					return GlobalEventExecutor.INSTANCE;
				}
			} catch (Throwable ignore) {
				
			}
			return null;
		}
	}
	
	private final class NioSocketChannelConfig extends DefaultSocketChannelConfig {
		private volatile int maxBytesPerGatheringWrite = Integer.MAX_VALUE;
		private NioSocketChannelConfig(NioSocketChannel channel, Socket javaSocket) {
			super(channel, javaSocket);
			calculateMaxBytesPerGatheringWrite();
		}
		
		@Override
		protected void autoReadCleared() {
			clearReadPending();
		}
		
		@Override
		public NioSocketChannelConfig setSendBufferSize(int sendBufferSize) {
			super.setSendBufferSize(sendBufferSize);
			calculateMaxBytesPerGatheringWrite();
			return this;
		}
		
		@Override
		public <T> boolean setOption(ChannelOption<T> option, T value) {
			if (option instanceof NioChannelOption) {
				return NioChannelOption.setOption(jdkChannel(), (NioChannelOption<T>) option, value);
			}
			return super.setOption(option, value);
		}
		
		@Override
		public <T> T getOption(ChannelOption<T> option) {
			if (option instanceof NioChannelOption) {
				return NioChannelOption.getOption(jdkChannel(), (NioChannelOption<T>) option);
			}
			return super.getOption(option);
		}
		
		@Override
		public Map<ChannelOption<?>, Object> getOptions() {
			return getOptions(super.getOptions(), NioChannelOption.getOptions(jdkChannel()));
		}
		
		void setMaxBytesPerGatheringWrite(int maxBytesPerGatheringWrite) {
			this.maxBytesPerGatheringWrite = maxBytesPerGatheringWrite;
		}
		
		int getMaxBytesPerGatheringWrite() {
			return maxBytesPerGatheringWrite;
		}
		
		private void calculateMaxBytesPerGatheringWrite() {
			int newSendBufferSize = getSendBufferSize() << 1;
			if (newSendBufferSize > 0) {
				setMaxBytesPerGatheringWrite(newSendBufferSize);
			}
		}
		
		private SocketChannel jdkChannel() {
			return ((NioSocketChannel) channel).javaChannel();
		}
		
	}
}
