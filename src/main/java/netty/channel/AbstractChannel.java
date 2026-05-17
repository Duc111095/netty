package netty.channel;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import netty.channel.socket.ChannelOutputShutdownEvent;
import netty.channel.socket.ChannelOutputShutdownException;
import netty.common.util.DefaultAttributeMap;
import netty.common.util.ReferenceCountUtil;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public abstract class AbstractChannel extends DefaultAttributeMap implements Channel {
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(AbstractChannel.class);
	
	private final Channel parent;
	private final ChannelId id;
	private final Unsafe unsafe;
	private final DefaultChannelPipeline pipeline;
	private final VoidChannelPromise unsafeVoidPromise = new VoidChannelPromise(this, false);
	private final CloseFuture closeFuture = new CloseFuture(this);
	
	private volatile SocketAddress localAddress;
	private volatile SocketAddress remoteAddress;
	private volatile EventLoop eventLoop;
	private volatile boolean registered;
	private boolean closeInitiated;
	private Throwable initialCloseCause;
	
	private boolean strValActive;
	private String strVal;
	
	protected AbstractChannel(Channel parent) {
		this.parent = parent;
		id = newId();
		unsafe = newUnsafe();
		pipeline = newChannelPipeline();
	}
	
	protected AbstractChannel(Channel parent, ChannelId id) {
		this.parent = parent;
		this.id = id;
		unsafe = newUnsafe();
		pipeline = newChannelPipeline();
	}
	
	protected final int maxMessagesPerWrite() {
		ChannelConfig config = config();
		if (config instanceof DefaultChannelConfig) {
			return ((DefaultChannelConfig) config).getMaxMessagesPerWrite();
		}
		Integer value = config.getOption(ChannelOption.MAX_MESSAGES_PER_WRITE);
		if (value == null) {
			return Integer.MAX_VALUE;
		}
		return value;
	}
	
	@Override
	public final ChannelId id() {
		return id;
	}
	
	protected ChannelId newId() {
		return DefaultChannelId.newInstance();
	}
	
	protected DefaultChannelPipeline newChannelPipeline() {
		return new DefaultChannelPipeline(this);
	}
	
	@Override
	public Channel parent() {
		return parent;
	}
	
	@Override
	public ChannelPipeline pipeline() {
		return pipeline;
	}
	
	@Override
	public EventLoop eventLoop() {
		EventLoop eventLoop = this.eventLoop;
		if (eventLoop == null) {
			throw new IllegalStateException("channel not registered to an event loop");
		}
		return eventLoop;
	}
	
	@Override
	public SocketAddress localAddress() {
		SocketAddress localAddress = this.localAddress;
		if (localAddress == null) {
			try {
				this.localAddress = localAddress = unsafe().localAddress();
			} catch (Error e) {
				throw e;
			} catch (Throwable t) {
				return null;
			}
		}
		return localAddress;
	}
	
	protected void invalidateLocalAddress() {
		localAddress = null;
	}
	
	@Override
	public SocketAddress remoteAddress() {
		SocketAddress remoteAddress = this.remoteAddress;
		if (remoteAddress == null) {
			try {
				this.remoteAddress = remoteAddress = unsafe().remoteAddress();
			} catch (Error e) {
				throw e;
			} catch (Throwable t) {
				return null;
			}
		}
		return remoteAddress;
	}
	
	protected void invalidateRemoteAddress() {
		remoteAddress = null;
	}
	
	@Override
	public boolean isRegistered() {
		return registered;
	}
	
	@Override
	public ChannelFuture closeFuture() {
		return closeFuture;
	}
	
	@Override
	public Unsafe unsafe() {
		return unsafe;
	}
	
	protected abstract AbstractUnsafe newUnsafe();
	
	@Override
	public final int hashCode() {
		return id.hashCode();
	}
	
	@Override
	public final boolean equals(Object o) {
		return this == o;
	}
	
	@Override
	public final int compareTo(Channel o) {
		if (this == o) {
			return 0;
		}
		return id().compareTo(o.id());
	}
	
	@Override
    public String toString() {
        boolean active = isActive();
        if (strValActive == active && strVal != null) {
            return strVal;
        }

        SocketAddress remoteAddr = remoteAddress();
        SocketAddress localAddr = localAddress();
        if (remoteAddr != null) {
            StringBuilder buf = new StringBuilder(96)
                .append("[id: 0x")
                .append(id.asShortText())
                .append(", L:")
                .append(localAddr)
                .append(active? " - " : " ! ")
                .append("R:")
                .append(remoteAddr)
                .append(']');
            strVal = buf.toString();
        } else if (localAddr != null) {
            StringBuilder buf = new StringBuilder(64)
                .append("[id: 0x")
                .append(id.asShortText())
                .append(", L:")
                .append(localAddr)
                .append(']');
            strVal = buf.toString();
        } else {
            StringBuilder buf = new StringBuilder(16)
                .append("[id: 0x")
                .append(id.asShortText())
                .append(']');
            strVal = buf.toString();
        }

        strValActive = active;
        return strVal;
    }
	
	@Override
	public final ChannelPromise voidPromise() {
		return pipeline.voidPromise();
	}
	
	protected abstract class AbstractUnsafe implements Unsafe {
		
		private volatile ChannelOutboundBuffer outboundBuffer = new ChannelOutboundBuffer(AbstractChannel.this);
		private RecvByteBufAllocator.Handle recvHandle;
		private boolean inFlush0;
		private boolean neverRegistered = true;
		
		private void assertEventLoop() {
			assert !registered || eventLoop.inEventLoop();
		}
		
		@Override
		public RecvByteBufAllocator.Handle recvBufAllocHandle() {
			if (recvHandle == null) {
				recvHandle = config().getRecvByteBufAllocator().newHandle();
			}
			return recvHandle;
		}
		
		@Override
		public final ChannelOutboundBuffer outboundBuffer() {
			return outboundBuffer;
		}
		
		@Override
		public final SocketAddress localAddress() {
			return localAddress0();
		}
		
		@Override
		public final SocketAddress remoteAddress() {
			return remoteAddress0();
		}
		
		@Override
		public final void register(EventLoop eventLoop, final ChannelPromise promise) {
			ObjectUtil.checkNotNull(eventLoop, "eventLoop");
			if (isRegistered()) {
				promise.setFailure(new IllegalStateException("registered to an event loop already"));
				return;
			}
			if (!isCompatible(eventLoop)) {
				promise.setFailure(
						new IllegalStateException("incompatible event loop type: " + eventLoop.getClass().getName()));
				return;
			}
			
			AbstractChannel.this.eventLoop = eventLoop;
			
			AbstractChannelHandlerContext context = pipeline.tail;
			do {
				context.contextExecutor = null;
				context = context.prev;
			} while (context != null);
			
			if (eventLoop.inEventLoop()) {
				register0(promise);
			} else {
				try {
					eventLoop.execute(new Runnable() {
						@Override
						public void run() {
							register0(promise);
						}
					});
				} catch (Throwable t) {
					logger.warn(
							"Force-closing a channel whose registration task was not accepted by an event loop: {}",
							AbstractChannel.this, t);
					closeForcibly();
					closeFuture.setClosed();
					safeSetFailure(promise, t);
				} 
			}
		}
		
		private void register0(ChannelPromise promise) {
			if (!promise.setUncancellable() || !ensureOpen(promise)) {
				return;
			}
			ChannelPromise registerPromise = newPromise();
			boolean firstRegistration = neverRegistered;
			registerPromise.addListener(future -> {
				if (future.isSuccess()) {
					neverRegistered = false;
					registered = true;
					
					pipeline.invokeHandlerAddedIfNeeded();
					
					safeSetSuccess(promise);
					pipeline.fireChannelRegistered();
					if (isActive()) {
						if (firstRegistration) {
							pipeline.fireChannelActive();
						} else if (config().isAutoRead()) {
							beginRead();
						}
					}
				} else {
					close(newPromise());
					closeFuture.setClosed();
					safeSetFailure(promise, future.cause());
				}
			});
			doRegister(registerPromise);
		}
		
		@Override
		public final void bind(final SocketAddress localAddress, final ChannelPromise promise) {
			assertEventLoop();
			
			if (!promise.setUncancellable() || !ensureOpen(promise)) {
				return;
			}
			
			if (Boolean.TRUE.equals(config().getOption(ChannelOption.SO_BROADCAST)) &&
					localAddress instanceof InetSocketAddress &&
					!((InetSocketAddress) localAddress).getAddress().isAnyLocalAddress() &&
					!PlatformDependent.isWindows() && !PlatformDependent.maybeSuperUser()) {
				logger.warn(
                        "A non-root user can't receive a broadcast packet if the socket " +
                        "is not bound to a wildcard address; binding to a non-wildcard " +
                        "address (" + localAddress + ") anyway as requested.");
			}
			
			boolean wasActive = isActive();
			try {
				doBind(localAddress);
			} catch (Throwable t) {
				safeSetFailure(promise, t);
				closeIfClosed();
				return;
			}
			
			if (!wasActive && isActive()) {
				invokeLater(new Runnable() {
					@Override
					public void run() {
						pipeline.fireChannelActive();
					}
				});
			}
			
			safeSetSuccess(promise);
		}
		
		@Override
		public final void disconnect(final ChannelPromise promise) {
			assertEventLoop();
			
			if (!promise.setUncancellable()) {
				return;
			}
			
			boolean wasActive = isActive();
			try {
				doDisconnect();
				remoteAddress = null;
				localAddress = null;
			} catch (Throwable t) {
				safeSetFailure(promise, t);
				closeIfClosed();
				return;
			}
			
			if (wasActive && !isActive()) {
				invokeLater(new Runnable() {
					@Override
					public void run() {
						pipeline.fireChannelInactive();
					}
				});
			}
			
			safeSetSuccess(promise);
			closeIfClosed();
		}
		
		@Override
		public void close(final ChannelPromise promise) {
			assertEventLoop();
			
			ClosedChannelException closedChannelException = 
					StacklessClosedChannelException.newInstance(AbstractChannel.class, "close(ChannelPromise)");
			close(promise, closedChannelException, closedChannelException);
		}
		
		public final void shutdownOutput(final ChannelPromise promise) {
			assertEventLoop();
			shutdownOutput(promise, null);
		}
		
		private void shutdownOutput(final ChannelPromise promise, Throwable cause) {
			if (!promise.setUncancellable()) {
				return;
			}
			
			final ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
			if (outboundBuffer == null) {
				promise.setFailure(new ClosedChannelException());
				return;
			}
			this.outboundBuffer = null;
			final Throwable shutdownCause = cause == null ?
					new ChannelOutputShutdownException("Channel output shutdown") :
					new ChannelOutputShutdownException("Channel output shutdown", cause);
			try {
				doShutdownOutput();
				promise.setSuccess();
			} catch (Throwable err) {
				promise.setFailure(err);
			} finally {
				closeOutboundBufferForShutdown(pipeline, outboundBuffer, shutdownCause);
			}
		}
		
		private void closeOutboundBufferForShutdown(
				ChannelPipeline pipeline, ChannelOutboundBuffer buffer, Throwable cause) {
			buffer.failFlushed(cause, false);
			buffer.close(cause, true);
			pipeline.fireUserEventTriggered(ChannelOutputShutdownEvent.INSTANCE);
		}
		
		protected void close(final ChannelPromise promise, final Throwable cause,
				final ClosedChannelException closeCause) {
			if (!promise.setUncancellable()) {
				return;
			}
			
			if (closeInitiated) {
				if (closeFuture.isDone()) {
					safeSetSuccess(promise);
				} else if (!(promise instanceof VoidChannelPromise)) {
					closeFuture.addListener(future -> promise.setSuccess());
				}
				return;
			}
			
			closeInitiated = true;
			
			final boolean wasActive = isActive();
			final ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
			this.outboundBuffer = null;
			Executor closeExecutor = prepareToClose();
			if (closeExecutor != null) {
				closeExecutor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							doClose0(promise);
						} finally {
							invokeLater(new Runnable() {
								@Override
								public void run() {
									if (outboundBuffer != null) {
										outboundBuffer.failFlushed(cause, false);
										outboundBuffer.close(closeCause);
									}
									fireChannelInactiveAndDeregister(wasActive);
								}
							});
						}
					}
				});
			} else {
				try {
					doClose0(promise);
				} finally {
					if (outboundBuffer != null) {
						outboundBuffer.failFlushed(cause, false);
						outboundBuffer.close(closeCause);
					}
				}
				if (inFlush0) {
					invokeLater(new Runnable() {
						@Override
						public void run() {
							fireChannelInactiveAndDeregister(wasActive);
						}
					});
				} else {
					fireChannelInactiveAndDeregister(wasActive);
				}
			}
		}
		
		private void doClose0(ChannelPromise promise) {
			try {
				doClose();
				closeFuture.setClosed();
				safeSetSuccess(promise);
			} catch (Throwable t) {
				closeFuture.setClosed();
				safeSetFailure(promise, t);
			}
		}
		
		private void fireChannelInactiveAndDeregister(final boolean wasActive) {
			deregister(voidPromise(), wasActive && !isActive());
		}
		
		@Override
		public final void closeForcibly() {
			assertEventLoop();
			
			try {
				doClose();
			} catch (Exception e) {
				logger.warn("Failed to close a channel.", e);
			}
		}
		
		@Override
		public void deregister(final ChannelPromise promise) {
			assertEventLoop();
			
			deregister(promise, false);
		}
		
		private void deregister(final ChannelPromise promise, final boolean fireChannelInactive) {
			if (!promise.setUncancellable()) {
				return;
			}
			
			if (!registered) {
				safeSetSuccess(promise);
				return;
			}
			
			invokeLater(new Runnable() {
				@Override
				public void run() {
					try {
						doDeregister();
					} catch (Throwable t) {
						logger.warn("Unexpected exception occurred while deregistering a channel.", t);
					} finally {
						if (fireChannelInactive) {
							pipeline.fireChannelInactive();
						}
						if (registered) {
							registered = false;
							pipeline.fireChannelUnregistered();
						}
						safeSetSuccess(promise);
					}
				}
			});
		}
		
		@Override
		public final void beginRead() {
			assertEventLoop();
			
			try {
				doBeginRead();
			} catch (final Exception e) {
				invokeLater(new Runnable() {
					@Override
					public void run() {
						pipeline.fireExceptionCaught(e);
					}
				});
				close(voidPromise());
			}
		}
		
		@Override
		public final void write(Object msg, ChannelPromise promise) {
			assertEventLoop();
			
			ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
			if (outboundBuffer == null) {
				try {
					ReferenceCountUtil.release(msg);
				} finally {
					safeSetFailure(promise,
							newClosedChannelException(initialCloseCause, "write(Object, ChannelPromise)"));
				}
				return;
			}
			
			int size;
			try {
				msg = filterOutboundMessage(msg);
				size = pipeline.estimatorHandle().size(msg);
				if (size < 0) {
					size = 0;
				}
			} catch (Throwable t) {
				try {
					ReferenceCountUtil.release(msg);
				} finally {
					safeSetFailure(promise, t);
				}
				return;
			}
			
			outboundBuffer.addMessage(msg, size, promise);
		}
		
		@Override
		public final void flush() {
			assertEventLoop();
			
			ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
			if (outboundBuffer == null) {
				return;
			}
			
			outboundBuffer.addFlush();
			flush0();
		}
		
		protected void flush0() {
			if (inFlush0) {
				return;
			}
			
			final ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
			if (outboundBuffer == null || outboundBuffer.isEmpty()) {
				return;
			}
			
			inFlush0 = true;
			
			if (!isActive()) {
				try {
					if (!outboundBuffer.isEmpty()) {
						if (isOpen()) {
							outboundBuffer.failFlushed(new NotYetConnectedException(), true);
						} else {
							outboundBuffer.failFlushed(newClosedChannelException(initialCloseCause, "flush0()"), false);
						}
					}
				} finally {
					inFlush0 = false;
				}
				return;
			}
			
			try {
				doWrite(outboundBuffer);
			} catch (Throwable t) {
				handleWriteError(t);
			} finally {
				inFlush0 = false;
			}
		}
		
		protected final void handleWriteError(Throwable t) {
			if (t instanceof IOException && config().isAutoClose()) {
				initialCloseCause = t;
				close(voidPromise(), t, newClosedChannelException(t, "flush0()"));
			} else {
				try {
					shutdownOutput(voidPromise(), t);
				} catch (Throwable t2) {
					initialCloseCause = t;
					close(voidPromise(), t2, newClosedChannelException(t, "flush0()"));
				}
			}
		}
		
		private ClosedChannelException newClosedChannelException(Throwable cause, String method) {
			ClosedChannelException exception = 
					StacklessClosedChannelException.newInstance(AbstractChannel.AbstractUnsafe.class, method);
			if (cause != null) {
				exception.initCause(cause);
			}
			return exception;
		}
		
		@Override
		public final ChannelPromise voidPromise() {
			assertEventLoop();
			
			return unsafeVoidPromise;
		}
		
		protected final boolean ensureOpen(ChannelPromise promise) {
			if (isOpen()) {
				return true;
			}
			
			safeSetFailure(promise, newClosedChannelException(initialCloseCause, "ensureOpen(ChannelPromise)"));
			return false;
		}
		
		protected final void safeSetSuccess(ChannelPromise promise) {
			if (!(promise instanceof VoidChannelPromise) && !promise.trySuccess()) {
				logger.warn("Failed to mark a promise as success because it is done already: {}", promise);
			}
		}
		
		protected final void safeSetFailure(ChannelPromise promise, Throwable cause) {
			if (!(promise instanceof VoidChannelPromise) && !promise.tryFailure(cause)) {
				logger.warn("Failed to mark a promise as failure because it's done already: {}", promise, cause);
			}
 		}
		
		protected final void closeIfClosed() {
			if (isOpen()) {
				return;
			}
			close(voidPromise());
		}
		
		private void invokeLater(Runnable task) {
			try {
				eventLoop().execute(task);
			} catch (RejectedExecutionException e) {
				logger.warn("Can't invoke task later as EventLoop rejected it", e);
			}
		}
		
		protected final Throwable annotateConnectException(Throwable cause, SocketAddress remoteAddress) {
			if (cause instanceof ConnectException) {
				return new AnnotatedConnectException((ConnectException) cause, remoteAddress);
			}
			if (cause instanceof NoRouteToHostException) {
                return new AnnotatedNoRouteToHostException((NoRouteToHostException) cause, remoteAddress);
            }
            if (cause instanceof SocketException) {
                return new AnnotatedSocketException((SocketException) cause, remoteAddress);
            }

            return cause;
		}
		
		protected Executor prepareToClose() {
			return null;
		}
	}
	
	protected abstract boolean isCompatible(EventLoop loop);
	
	protected abstract SocketAddress localAddress0();
	
	protected abstract SocketAddress remoteAddress0();
	
	protected void doRegister() throws Exception {
		
	}
	
	protected void doRegister(ChannelPromise promise) {
		try {
			doRegister();
		} catch (Throwable cause) {
			promise.setFailure(cause);
			return;
		}
		promise.setSuccess();
	}
	
	protected abstract void doBind(SocketAddress localAddress) throws Exception;
	
	protected abstract void doDisconnect() throws Exception;
	
	protected abstract void doClose() throws Exception;
	
	protected void doShutdownOutput() throws Exception {
		doClose();
	}
	
	protected void doDeregister() throws Exception {
		
	}
	
	protected abstract void doBeginRead() throws Exception;
	
	protected abstract void doWrite(ChannelOutboundBuffer in) throws Exception;
	
	protected Object filterOutboundMessage(Object msg) throws Exception {
		return msg;
	}
	
	protected void validateFileRegion(DefaultFileRegion region, long position) throws IOException {
		DefaultFileRegion.validate(region, position);
	}
 	
	static final class CloseFuture extends DefaultChannelPromise {
		
		CloseFuture(AbstractChannel ch) {
			super(ch);
		}
		
		@Override
		public ChannelPromise setSuccess() {
			throw new IllegalStateException();
		}
		
		@Override
		public ChannelPromise setFailure(Throwable cause) {
			throw new IllegalStateException();
		}
		
		@Override
		public boolean trySuccess() {
			throw new IllegalStateException();
		}
		
		@Override
		public boolean tryFailure(Throwable cause) {
			throw new IllegalStateException();
		}
		
		boolean setClosed() {
			return super.trySuccess();
		}
	} 
	
	private static final class AnnotatedConnectException extends ConnectException {
		
		private static final long serialVersionUID = 3901958112696433556L;
		
		AnnotatedConnectException(ConnectException exception, SocketAddress remoteAddress) {
			super(exception.getMessage() + ": " + remoteAddress);
			initCause(exception);
		}
		
		@Override
		public Throwable fillInStackTrace() {
			return this;
		}
	}
	
	private static final class AnnotatedNoRouteToHostException extends NoRouteToHostException {
		
		private static final long serialVersionUID = -6801433937592080623L;
		
		AnnotatedNoRouteToHostException(NoRouteToHostException exception, SocketAddress remoteAddress) {
			super(exception.getMessage() + ": " + remoteAddress);
			initCause(exception);
		}
		
		@Override
		public Throwable fillInStackTrace() {
			return this;
		}
	}
	
	private static final class AnnotatedSocketException extends SocketException {

        private static final long serialVersionUID = 3896743275010454039L;

        AnnotatedSocketException(SocketException exception, SocketAddress remoteAddress) {
            super(exception.getMessage() + ": " + remoteAddress);
            initCause(exception);
        }

        // Suppress a warning since this method doesn't need synchronization
        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }
}
