package netty.channel.nio;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.TimeUnit;

import netty.buffer.ByteBuf;
import netty.buffer.ByteBufAllocator;
import netty.buffer.ByteBufUtil;
import netty.buffer.Unpooled;
import netty.channel.AbstractChannel;
import netty.channel.Channel;
import netty.channel.ChannelException;
import netty.channel.ChannelFuture;
import netty.channel.ChannelPromise;
import netty.channel.ChannelFutureListener;
import netty.channel.ConnectTimeoutException;
import netty.channel.EventLoop;
import netty.channel.IoEvent;
import netty.channel.IoEventLoop;
import netty.channel.IoEventLoopGroup;
import netty.channel.IoRegistration;
import netty.common.util.ReferenceCountUtil;
import netty.common.util.ReferenceCounted;
import netty.common.util.concurrent.Future;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public abstract class AbstractNioChannel extends AbstractChannel {
	private static final InternalLogger logger = 
			InternalLoggerFactory.getInstance(AbstractNioChannel.class);
	private final SelectableChannel ch;
	protected final int readInterestOps;
	protected final NioIoOps readOps;
	volatile IoRegistration registration;
	boolean readPending;
	private final Runnable clearReadPendingRunnable = new Runnable() {
		@Override
		public void run() {
			clearReadPending0();
		}
	};
	
	private ChannelPromise connectPromise;
	private Future<?> connectTimeoutFuture;
	private SocketAddress requestedRemoteAddress;
	
	protected AbstractNioChannel(Channel parent, SelectableChannel ch, int readOps) {
		this(parent, ch, NioIoOps.valueOf(readOps));
	}
	
	protected AbstractNioChannel(Channel parent, SelectableChannel ch, NioIoOps readOps) {
		super(parent);
		this.ch = ch;
		this.readInterestOps = ObjectUtil.checkNotNull(readOps, "readOps").value;
		this.readOps = readOps;
		try {
			ch.configureBlocking(false);
		} catch (IOException e) {
			try {
				ch.close();
			} catch (IOException e2) {
				logger.warn("Failed to close a paritially initialized socket.", e2);
			}
			throw new ChannelException("Failed to enter non-blocking mode.", e);
		}
	}
	
	protected void addAndSubmit(NioIoOps addOps) {
		int interestOps = selectionKey().interestOps();
		if (!addOps.isIncludedIn(interestOps)) {
			try {
				registration().submit(NioIoOps.valueOf(interestOps).with(addOps));
			} catch (Exception e) {
				throw new ChannelException(e);
			}
		}
	}
	
	protected void removeAndSubmit(NioIoOps removeOps) {
		int interestOps = selectionKey().interestOps();
		if (removeOps.isIncludedIn(interestOps)) {
			try {
				registration().submit(NioIoOps.valueOf(interestOps).without(removeOps));
			} catch (Exception e) {
				throw new ChannelException(e);
			}
		}
	}
	
	@Override
	public boolean isOpen() {
		return ch.isOpen();
	}
	
	@Override
	public NioUnsafe unsafe() {
		return (NioUnsafe) super.unsafe();
	}
	
	protected SelectableChannel javaChannel() {
		return ch;
	}
	
	protected SelectionKey selectionKey() {
		return registration().attachment();
	}
	
	protected IoRegistration registration() {
		assert registration != null;
		return registration;
	}
	
	protected boolean isReadPending() {
		return readPending;
	}
	
	protected void setReadPending(final boolean readPending) {
		if (isRegistered()) {
			EventLoop eventLoop = eventLoop();
			if (eventLoop.inEventLoop()) {
				setReadPending0(readPending);
			} else {
				eventLoop.execute(new Runnable() {
					@Override
					public void run() {
						setReadPending0(readPending);
					}
				});
			}
		} else {
			this.readPending = readPending;
		}
	}
	
	protected final void clearReadPending() {
		if (isRegistered()) {
			EventLoop eventLoop = eventLoop();
			if (eventLoop.inEventLoop()) {
				clearReadPending0();
			} else {
				eventLoop.execute(clearReadPendingRunnable);
			}
		} else {
			readPending = false;
		}
	}
	
	private void setReadPending0(boolean readPending) {
		this.readPending = readPending;
		if (!readPending) {
			((AbstractNioUnsafe) unsafe()).removeReadOp();
		}
	}
	
	private void clearReadPending0() {
		readPending = false;
		((AbstractNioUnsafe) unsafe()).removeReadOp();
	}
	
	public interface NioUnsafe extends Unsafe {
		SelectableChannel ch();
		
		void finishConnect();
		
		void read();
		
		void forceFlush();
	}
	
	protected abstract class AbstractNioUnsafe extends AbstractUnsafe implements NioUnsafe, NioIoHandle {
		@Override
		public void close() {
			close(voidPromise());
		}
		
		@Override
		public SelectableChannel selectableChannel() {
			return ch;
		}
		
		Channel channel() {
			return AbstractNioChannel.this;
		}
		
		protected final void removeReadOp() {
			IoRegistration registration = registration();
			
			if (!registration.isValid()) {
				return;
			}
			removeAndSubmit(readOps);
		}
		
		@Override
		public final SelectableChannel ch() {
			return javaChannel();
		}
		
		@Override
		public final void connect(
				final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise promise) {
			if (promise.isDone() || !ensureOpen(promise)) {
				return;
			}
			
			try {
				if (connectPromise != null) {
					throw new ConnectionPendingException();
				}
				boolean wasActive = isActive();
				if (doConnect(remoteAddress, localAddress)) {
					fulfillConnectPromise(promise, wasActive);
				} else {
					connectPromise = promise;
					requestedRemoteAddress = remoteAddress;
					
					final int connectTimeoutMillis = config().getConnectTimeoutMillis();
					if (connectTimeoutMillis > 0) {
						connectTimeoutFuture = eventLoop().schedule(new Runnable() {
							@Override
							public void run() {
								ChannelPromise connectPromise = AbstractNioChannel.this.connectPromise;
								if (connectPromise != null && !connectPromise.isDone() 
										&& connectPromise.tryFailure(new ConnectTimeoutException(
												"connection timed out after " + connectTimeoutMillis + " ms: " + 
													remoteAddress))) {
									close(voidPromise());
								}
							}
						}, connectTimeoutMillis, TimeUnit.MILLISECONDS);
					}
					
					promise.addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) {
							if (future.isCancelled()) {
								if (connectTimeoutFuture != null) {
									connectTimeoutFuture.cancel(false);
								}
								connectPromise = null;
								close(voidPromise());
							}
						}
					});
				}
			} catch (Throwable t) {
				promise.tryFailure(annotateConnectException(t, remoteAddress));
				closeIfClosed();
			}
		}
		
		private void fulfillConnectPromise(ChannelPromise promise, boolean wasActive) {
			if (promise == null) {
				return;
			}
			boolean active = isActive();
			
			boolean promiseSet = promise.trySuccess();
			if (!wasActive && active) {
				pipeline().fireChannelActive();
			}
			
			if (!promiseSet) {
				close(voidPromise());
			}
		}
		
		private void fulfillConnectPromise(ChannelPromise promise, Throwable cause) {
			if (promise == null) {
				return;
			}
			
			promise.tryFailure(cause);
			closeIfClosed();
		}
		
		@Override
		public final void finishConnect() {
			assert eventLoop().inEventLoop();
			
			try {
				boolean wasActive = isActive();
				doFinishConnect();
				fulfillConnectPromise(connectPromise, wasActive);
			} catch (Throwable t) {
				fulfillConnectPromise(connectPromise, annotateConnectException(t, requestedRemoteAddress));
			} finally {
				if (connectTimeoutFuture != null) {
					connectTimeoutFuture.cancel(false);
				}
				connectPromise = null;
			}
		}
		
		@Override
		protected final void flush0() {
			if (!isFlushPending()) {
				super.flush0();
			}
		}
		
		@Override
		public final void forceFlush() {
			super.flush0();
		}
		
		private boolean isFlushPending() {
			IoRegistration registration = registration();
			return registration.isValid() && NioIoOps.WRITE.isIncludedIn((
					(SelectionKey) registration.attachment()).interestOps());
		}
		
		@Override
		public void handle(IoRegistration registration, IoEvent event ) {
			try {
				NioIoEvent nioEvent = (NioIoEvent) event;
				NioIoOps nioReadOps = nioEvent.ops();
				if (nioReadOps.contains(NioIoOps.CONNECT)) {
					removeAndSubmit(NioIoOps.CONNECT);
					
					unsafe().finishConnect();
				}
				
				if (nioReadOps.contains(NioIoOps.WRITE)) {
					forceFlush();
				}
				
				if (nioReadOps.contains(NioIoOps.READ_AND_ACCEPT) || nioReadOps.equals(NioIoOps.NONE)) {
					read();
				}
			} catch (CancelledKeyException ignored) {
				close(voidPromise());
			}
		}
	}
	
	@Override
	protected boolean isCompatible(EventLoop loop) {
		return loop instanceof IoEventLoop && ((IoEventLoopGroup) loop).isCompatible(AbstractNioUnsafe.class);
	}
	
	@Override
	protected void doRegister(ChannelPromise promise) {
		assert registration == null;
		((IoEventLoop) eventLoop()).register((AbstractNioUnsafe) unsafe()).addListener(f -> {
			if (f.isSuccess()) {
				registration = (IoRegistration) f.getNow();
				promise.setSuccess();
			} else {
				promise.setFailure(f.cause());
			}
 		});
	}
	
	@Override
	protected void doBeginRead() throws Exception {
		IoRegistration registration = this.registration;
		if (registration == null || !registration.isValid()) {
			return;
		}
		readPending = true;
		addAndSubmit(readOps);
	}
	
	protected abstract boolean doConnect(SocketAddress remoteAddress, SocketAddress localAddress) throws Exception;
	
	protected abstract void doFinishConnect() throws Exception;
	
	protected final ByteBuf newDirectBuffer(ByteBuf buf ) {
		final int readableBytes = buf.readableBytes();
		if (readableBytes == 0) {
			ReferenceCountUtil.safeRelease(buf);
			return Unpooled.EMPTY_BUFFER;
		}
		final ByteBufAllocator alloc = alloc();
		if (alloc.isDirectBufferPooled()) {
			ByteBuf directBuf = alloc.directBuffer(readableBytes);
			directBuf.writeBytes(buf, buf.readerIndex(), readableBytes);
			ReferenceCountUtil.safeRelease(buf);
			return directBuf;
		} 
		final ByteBuf directBuf = ByteBufUtil.threadLocalDirectBuffer();
		if (directBuf != null) {
			directBuf.writeBytes(buf, buf.readerIndex(), readableBytes);
			ReferenceCountUtil.safeRelease(buf);
			return directBuf;
		}
		
		return buf;
	}
	
	protected final ByteBuf newDirectBuffer(ReferenceCounted holder, ByteBuf buf) {
		final int readableBytes = buf.readableBytes();
		if (readableBytes == 0) {
			ReferenceCountUtil.safeRelease(buf);
			return Unpooled.EMPTY_BUFFER;
		}
		final ByteBufAllocator alloc = alloc();
		if (alloc.isDirectBufferPooled()) {
			ByteBuf directBuf = alloc.directBuffer(readableBytes);
			directBuf.writeBytes(buf, buf.readerIndex(), readableBytes);
			ReferenceCountUtil.safeRelease(buf);
			return directBuf;
		}
		
		final ByteBuf directBuf = ByteBufUtil.threadLocalDirectBuffer();
		if (directBuf != null) {
			directBuf.writeBytes(buf, buf.readerIndex(), readableBytes);
			ReferenceCountUtil.safeRelease(buf);
			return directBuf;
		}
		
		if (holder != buf) {
			buf.retain();
			ReferenceCountUtil.safeRelease(holder);
		}
		return buf;
	}
	
	@Override
	protected void doClose() throws Exception {
		ChannelPromise promise = connectPromise;
		if (promise != null) {
			promise.tryFailure(new ClosedChannelException());
			connectPromise = null;
		}
		
		Future<?> future = connectTimeoutFuture;
		if (future != null) {
			future.cancel(false);
			connectTimeoutFuture = null;
		}
	}
}
