package netty.channel;

import netty.buffer.ByteBufAllocator;
import netty.common.util.Attribute;
import netty.common.util.AttributeKey;
import netty.common.util.Recycler;
import netty.common.util.ReferenceCountUtil;
import netty.common.util.Recycler.Handle;
import netty.common.util.ResourceLeakHint;
import netty.common.util.concurrent.AbstractEventExecutor;
import netty.common.util.concurrent.EventExecutor;
import netty.common.util.concurrent.OrderedEventExecutor;
import netty.common.util.concurrent.PromiseNotifier;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PromiseNotificationUtil;
import netty.common.util.internal.StringUtil;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static netty.channel.ChannelHandlerMask.*;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

abstract class AbstractChannelHandlerContext implements ChannelHandlerContext, ResourceLeakHint {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(AbstractChannelHandlerContext.class);
	volatile AbstractChannelHandlerContext next;
	volatile AbstractChannelHandlerContext prev;
	
	private static final AtomicIntegerFieldUpdater<AbstractChannelHandlerContext> HANDLER_STATE_UPDATER =
			AtomicIntegerFieldUpdater.newUpdater(AbstractChannelHandlerContext.class, "handlerState");
	
	private static final int ADD_PENDING = 1;
	private static final int ADD_COMPLETE = 2;
	private static final int REMOVE_COMPLETE = 3;
	private static final int INIT = 0;
	
	private final DefaultChannelPipeline pipeline;
	private final String name;
	private final boolean ordered;
	private final int executionMask;
	
	final EventExecutor childExecutor;
	EventExecutor contextExecutor;
	private ChannelFuture succeededFuture;
	
	private Tasks invokeTasks;
	private volatile int handlerState = INIT;
	
	AbstractChannelHandlerContext(DefaultChannelPipeline pipeline, EventExecutor executor,
			String name, Class<? extends ChannelHandler> handlerClass) {
		this.name = ObjectUtil.checkNotNull(name, "name");
		this.pipeline = pipeline;
		this.childExecutor = executor;
		executionMask = mask(handlerClass);
		ordered = executor == null || executor instanceof OrderedEventExecutor;
	}
	
	@Override
	public Channel channel() {
		return pipeline.channel();
	}
	
	@Override
	public ChannelPipeline pipeline() {
		return pipeline;
	}
	
	@Override
	public ByteBufAllocator alloc() {
		return channel().config().getAllocator();
	}
	
	@Override
	public EventExecutor executor() {
		EventExecutor ex = contextExecutor;
		if (ex == null) {
			contextExecutor = ex = childExecutor != null ? childExecutor : channel().eventLoop();
		}
		return ex;
	}
	
	@Override
	public String name() {
		return name;
	}

	@Override
	public ChannelHandlerContext fireChannelRegistered() {
		AbstractChannelHandlerContext next = findContextInbound(MASK_CHANNEL_REGISTERED);
		if (next.executor().inEventLoop()) {
			if (next.invokeHandler()) {
				try {
					final ChannelHandler handler = next.handler();
					final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
					if (handler == headContext) {
						headContext.channelRegistered(next);
					} else if (handler instanceof ChannelInboundHandlerAdapter) {
						((ChannelInboundHandlerAdapter) handler).channelRegistered(next);
					} else {
						((ChannelInboundHandler) handler).channelRegistered(next);
					}
				} catch (Throwable t) {
					next.invokeExceptionCaught(t);
				}
			} else {
				next.fireChannelRegistered();
			}
		} else {
			next.executor().execute(this::fireChannelRegistered);
		}
		return this;
	}
	
	@Override
	public ChannelHandlerContext fireChannelUnregistered() {
		final AbstractChannelHandlerContext next = findContextInbound(MASK_CHANNEL_UNREGISTERED);
		if (next.executor().inEventLoop()) {
			if (next.invokeHandler()) {
				try {
					final ChannelHandler handler = next.handler();
					final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
					if (handler == headContext) {
						headContext.channelUnregistered(next);
					} else if (handler instanceof ChannelInboundHandlerAdapter) {
						((ChannelInboundHandlerAdapter) handler).channelUnregistered(next);
					} else {
						((ChannelInboundHandler) handler).channelUnregistered(next);
					}
				} catch (Throwable t) {
					next.invokeExceptionCaught(t);
				}
			} else {
				next.fireChannelUnregistered();
			}
		} else {
			next.executor().execute(this::fireChannelUnregistered);
		}
		return this;
	}
	
	@Override
	public ChannelHandlerContext fireChannelActive() {
		AbstractChannelHandlerContext next = findContextInbound(MASK_CHANNEL_ACTIVE);
		if (next.executor().inEventLoop()) {
			if (next.invokeHandler()) {
				try {
					final ChannelHandler handler = next.handler();
					final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
					if (handler == headContext) {
						headContext.channelActive(next);
					} else if (handler instanceof ChannelInboundHandlerAdapter) {
						((ChannelInboundHandlerAdapter) handler).channelActive(next);
					} else {
						((ChannelInboundHandler) handler).channelActive(next);
					}
				} catch (Throwable t) {
					next.invokeExceptionCaught(t);
				}
			} else {
				next.fireChannelActive();
			}
		} else {
			next.executor().execute(this::fireChannelActive);
		}
		return this;
	}
	
	@Override
	public ChannelHandlerContext fireChannelInactive() {
		AbstractChannelHandlerContext next = findContextInbound(MASK_CHANNEL_INACTIVE);
		if (next.executor().inEventLoop()) {
			if (next.invokeHandler()) {
				try {
					final ChannelHandler handler = next.handler();
					final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
					if (handler == headContext) {
						headContext.channelInactive(next);
					} else if (handler instanceof ChannelInboundHandlerAdapter) {
						((ChannelInboundHandlerAdapter) handler).channelInactive(next);
					} else {
						((ChannelInboundHandler) handler).channelInactive(next);
					}
				} catch (Throwable t) {
					next.invokeExceptionCaught(t);
				}
			} else {
				next.fireChannelInactive();
			}
		} else {
			next.executor().execute(this::fireChannelInactive);
		}
		return this;
	}
	
	@Override
	public ChannelHandlerContext fireExceptionCaught(final Throwable cause) {
		AbstractChannelHandlerContext next = findContextInbound(MASK_EXCEPTION_CAUGHT);
		ObjectUtil.checkNotNull(cause, "cause");
		if (next.executor().inEventLoop()) {
			next.invokeExceptionCaught(cause);
		} else {
			try {
				next.executor().execute(() -> next.invokeExceptionCaught(cause));
			} catch (Throwable t) {
				if (logger.isWarnEnabled()) {
					logger.warn("Failed to submit an exceptionCaught() event", t);
					logger.warn("The exceptionCaught() event that was failed to submit was:", cause);
				}
			}
		}
		return this;
	}
	
	private void invokeExceptionCaught(final Throwable cause) {
		if (invokeHandler()) {
			try {
				handler().exceptionCaught(this, cause);
			} catch (Throwable error) {
				if (logger.isDebugEnabled()) {
					logger.debug(
		                "An exception " +
		                "was thrown by a user handler's exceptionCaught() " +
		                "method while handling the following exception:", cause);
				} else if (logger.isWarnEnabled()) {
	                logger.warn(
	                    "An exception '{}' [enable DEBUG level for full stacktrace] " +
	                    "was thrown by a user handler's exceptionCaught() " +
	                    "method while handling the following exception:", error, cause);
	            }
			}
		} else {
		    fireExceptionCaught(cause);
		}
	}
	
	@Override
	public ChannelHandlerContext fireUserEventTriggered(final Object event) {
		ObjectUtil.checkNotNull(event, "event");
		AbstractChannelHandlerContext next = findContextInbound(MASK_USER_EVENT_TRIGGERED);
		if (next.executor().inEventLoop()) {
			if (next.invokeHandler()) {
				try {
					final ChannelHandler handler = next.handler();
					final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
					if (handler == headContext) {
						headContext.userEventTriggered(next, event);
					} else if (handler instanceof ChannelInboundHandlerAdapter) {
						((ChannelInboundHandlerAdapter) handler).userEventTriggered(next, event);
					} else {
						((ChannelInboundHandler) handler).userEventTriggered(next, event);
					}
				} catch (Throwable t) {
					next.invokeExceptionCaught(t);
				}
			} else {
				next.fireUserEventTriggered(event);
			}
		} else {
			next.executor().execute(() -> fireUserEventTriggered(event));
		}
		return this;
	}
	
	@Override
	public ChannelHandlerContext fireChannelRead(final Object msg) {
		AbstractChannelHandlerContext next = findContextInbound(MASK_CHANNEL_READ);
		if (next.executor().inEventLoop()) {
			final Object m = pipeline.touch(msg, next);
			if (next.invokeHandler()) {
				try {
					final ChannelHandler handler = next.handler();
					final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
					if (handler == headContext) {
						headContext.channelRead(next, m);
					} else if (handler instanceof ChannelDuplexHandler) {
						((ChannelDuplexHandler) handler).channelRead(next, m);
					} else {
						((ChannelInboundHandler) handler).channelRead(next, m);
					}
				} catch (Throwable t) {
					next.invokeExceptionCaught(t);
				}
			} else {
				next.fireChannelRead(m);
			}
		} else {
			next.executor().execute(() -> fireChannelRead(msg));
		}
		return this;
	}
	
	@Override
	public ChannelHandlerContext fireChannelReadComplete() {
		AbstractChannelHandlerContext next = findContextInbound(MASK_CHANNEL_READ_COMPLETE);
		if (next.executor().inEventLoop()) {
			if (next.invokeHandler()) {
				try {
					final ChannelHandler handler = next.handler();
					final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
					if (handler == headContext) {
						headContext.channelReadComplete(next);
					} else if (handler instanceof ChannelDuplexHandler) {
						((ChannelDuplexHandler) handler).channelReadComplete(next);
					} else {
						((ChannelInboundHandler) handler).channelReadComplete(next);
					}
				} catch (Throwable t) {
					next.invokeExceptionCaught(t);
				}
			} else {
				next.fireChannelReadComplete();
			}
		} else {
			next.executor().execute(getInvokeTasks().fireChannelReadCompleteTask);
		}
		return this;
	}
	
	@Override
	public ChannelHandlerContext fireChannelWritabilityChanged() {
		AbstractChannelHandlerContext next = findContextInbound(MASK_CHANNEL_WRITABILITY_CHANGED);
		if (next.executor().inEventLoop()) {
			if (next.invokeHandler()) {
				try {
					final ChannelHandler handler = next.handler();
					final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
					if (handler == headContext) {
						headContext.channelWritabilityChanged(next);
					} else if (handler instanceof ChannelInboundHandlerAdapter) {
						((ChannelInboundHandlerAdapter) handler).channelWritabilityChanged(next);
					} else {
						((ChannelInboundHandler) handler).channelWritabilityChanged(next);
					}
				} catch (Throwable t) {
					next.invokeExceptionCaught(t);
				}
			} else {
				next.fireChannelWritabilityChanged();
			}
		} else {
			next.executor().execute(getInvokeTasks().fireChannelWritabilityChangedTask);
		}
		return this;
	}
	
	@Override
    public ChannelFuture bind(SocketAddress localAddress) {
        return bind(localAddress, newPromise());
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress) {
        return connect(remoteAddress, newPromise());
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return connect(remoteAddress, localAddress, newPromise());
    }

    @Override
    public ChannelFuture disconnect() {
        return disconnect(newPromise());
    }

    @Override
    public ChannelFuture close() {
        return close(newPromise());
    }

    @Override
    public ChannelFuture deregister() {
        return deregister(newPromise());
    }
    
    private ChannelPromise ensurePromiseUseCorrectExecutor(ChannelPromise promise) {
    	if (promise instanceof DefaultChannelPromise && !((DefaultChannelPromise) promise).executor().inEventLoop()) {
    		ChannelPromise newPromise = newPromise();
    		PromiseNotifier.cascade(newPromise, promise);
    		return newPromise;
    	}
    	return promise;
    }
    
    @Override
    public ChannelFuture bind(final SocketAddress localAddress, ChannelPromise promise) {
    	ObjectUtil.checkNotNull(localAddress, "localAddress");
    	if (isNotValidPromise(promise, false)) {
    		return promise;
    	}
    	final AbstractChannelHandlerContext next = findContextOutbound(MASK_BIND);
    	EventExecutor executor = next.executor();
    	if (executor.inEventLoop()) {
    		if (next.invokeHandler()) {
    			promise = ensurePromiseUseCorrectExecutor(promise);
    			try {
    				final ChannelHandler handler = next.handler();
    				final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
    				if (handler == headContext) {
    					headContext.bind(next, localAddress, promise);
    				} else if (handler instanceof ChannelDuplexHandler) {
    					((ChannelDuplexHandler) handler).bind(next, localAddress, promise);
    				} else if (handler instanceof ChannelOutboundHandlerAdapter) {
    					((ChannelOutboundHandlerAdapter) handler).bind(next, localAddress, promise);
    				} else {
    					((ChannelOutboundHandler) handler).bind(next, localAddress, promise);
    				}
    			} catch (Throwable t) {
    				notifyOutboundHandlerException(t, promise);
    			}
    		} else {
    			next.bind(localAddress, promise);
    		}
    	} else {
    		final ChannelPromise p = promise;
    		safeExecute(executor, () -> bind(localAddress, p), promise, null, false);
    	}
    	return promise;
    }
    
    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
    	return connect(remoteAddress, null, promise);
    }
    
    @Override
    public ChannelFuture connect(
    		final SocketAddress remoteAddress, final SocketAddress localAddress, ChannelPromise promise) {
    	ObjectUtil.checkNotNull(remoteAddress, "remoteAddress");
    	
    	if (isNotValidPromise(promise, false)) {
    		return promise;
    	}
    	final AbstractChannelHandlerContext next = findContextOutbound(MASK_CONNECT);
    	EventExecutor executor = next.executor();
    	if (executor.inEventLoop()) {
    		if (next.invokeHandler()) {
    			promise = ensurePromiseUseCorrectExecutor(promise);
    			try {
    				final ChannelHandler handler = next.handler();
    				final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
    				if (handler == headContext) {
    					headContext.connect(next, remoteAddress, localAddress, promise);
    				} else if (handler instanceof ChannelDuplexHandler) {
    					((ChannelDuplexHandler) handler).connect(next, remoteAddress, localAddress, promise);
    				} else if (handler instanceof ChannelOutboundHandlerAdapter) {
    					((ChannelOutboundHandlerAdapter) handler).connect(next, remoteAddress, localAddress, promise);
    				} else {
    					((ChannelOutboundHandler) handler).connect(next, remoteAddress, localAddress, promise);
    				}
    			} catch (Throwable t) {
    				notifyOutboundHandlerException(t, promise);
    			}
    		} else {
    			next.connect(remoteAddress, localAddress, promise);
    		}
    	} else {
    		final ChannelPromise p = promise;
    		safeExecute(executor, () -> connect(remoteAddress, localAddress, p), promise, null, false);
    	}
    	return promise;
    }
    
    @Override
    public ChannelFuture disconnect(ChannelPromise promise) {
    	if (!channel().metadata().hasDisconnect()) {
    		return close(promise);
    	}
    	if (isNotValidPromise(promise, false)) {
    		return promise;
    	}
    	final AbstractChannelHandlerContext next = findContextOutbound(MASK_DISCONNECT);
    	EventExecutor executor = next.executor();
    	if (executor.inEventLoop()) {
    		if (next.invokeHandler()) {
    			promise = ensurePromiseUseCorrectExecutor(promise);
    			try {
    				final ChannelHandler handler = next.handler();
    				final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
    				if (handler == headContext) {
    					headContext.disconnect(next, promise);
    				} else if (handler instanceof ChannelDuplexHandler) {
    					((ChannelDuplexHandler) handler).disconnect(next, promise);
    				} else if (handler instanceof ChannelOutboundHandlerAdapter) {
    					((ChannelOutboundHandlerAdapter) handler).disconnect(next, promise);
    				} else {
    					((ChannelOutboundHandler) handler).disconnect(next, promise);
    				}
    			} catch (Throwable t) {
    				notifyOutboundHandlerException(t, promise);
    			}
    		} else {
    			next.disconnect(promise);
    		}
    	} else {
    		final ChannelPromise p = promise;
    		safeExecute(executor, () -> disconnect(p), promise, null, false);
    	}
    	return promise;
    }
    
    @Override
    public ChannelFuture close(ChannelPromise promise) {
    	if (isNotValidPromise(promise, false)) {
    		return promise;
    	}
    	final AbstractChannelHandlerContext next = findContextOutbound(MASK_CLOSE);
    	EventExecutor executor = next.executor();
    	if (executor.inEventLoop()) {
    		if (next.invokeHandler()) {
    			promise = ensurePromiseUseCorrectExecutor(promise);
    			try {
    				final ChannelHandler handler = next.handler();
    				final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
    				if (handler == headContext) {
    					headContext.close(next, promise);
    				} else if (handler instanceof ChannelDuplexHandler) {
    					((ChannelDuplexHandler) handler).close(next, promise);
    				} else if (handler instanceof ChannelOutboundHandlerAdapter) {
    					((ChannelOutboundHandlerAdapter) handler).close(next, promise);
    				} else {
    					((ChannelOutboundHandler) handler).close(next, promise);
    				}
    			} catch (Throwable t) {
    				notifyOutboundHandlerException(t, promise);
    			}
    		} else {
    			next.close(promise);
    		}
     	} else {
     		final ChannelPromise p = promise;
     		safeExecute(executor, () -> close(p), promise, null, false);
     	}
    	return promise;
    }
    
    @Override
    public ChannelFuture deregister(ChannelPromise promise) {
    	if (isNotValidPromise(promise, false)) {
    		return promise;
    	}
    	final AbstractChannelHandlerContext next = findContextOutbound(MASK_DEREGISTER);
    	EventExecutor executor = next.executor();
    	if (executor.inEventLoop()) {
    		if (next.invokeHandler()) {
    			promise = ensurePromiseUseCorrectExecutor(promise);
    			try {
    				final ChannelHandler handler = next.handler();
    				final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
    				if (handler == headContext) {
    					headContext.deregister(next, promise);
    				} else if (handler instanceof ChannelDuplexHandler) {
    					((ChannelDuplexHandler) handler).deregister(next, promise);
    				} else if (handler instanceof ChannelOutboundHandlerAdapter) {
    					((ChannelOutboundHandlerAdapter) handler).deregister(next, promise);
    				} else {
    					((ChannelOutboundHandler) handler).deregister(next, promise);
    				}
    			} catch (Throwable t) {
    				notifyOutboundHandlerException(t, promise);
    			}
    		} else {
    			deregister(promise);
    		}
    	} else {
    		final ChannelPromise p = promise;
    		safeExecute(executor, () -> deregister(p), promise, null, false);
    	}
    	return promise;
    }
    
    @Override
    public ChannelHandlerContext read() {
    	final AbstractChannelHandlerContext next = findContextOutbound(MASK_READ);
    	if (next.executor().inEventLoop()) {
    		if (next.invokeHandler()) {
    			try {
    				final ChannelHandler handler = next.handler();
    				final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
    				if (handler == headContext) {
    					headContext.read(next);
    				} else if (handler instanceof ChannelDuplexHandler) {
    					((ChannelDuplexHandler) handler).read(next);
    				} else if (handler instanceof ChnanelOutboundHandlerAdapter) {
    					((ChannelOutboundHandlerAdapter) handler).read(next);
    				} else {
    					((ChannelOutboundHandler) handler).read(next);
    				}
    			} catch (Throwable t) {
    				next.invokeExceptionCaught(t);
    			}
    		} else {
    			next.read();
    		}
    	} else {
    		next.executor().execute(getInvokeTasks().readTask);
    	}
    	return this;
    }
    
    @Override
    public ChannelFuture write(Object msg) {
    	ChannelPromise promise = newPromise();
    	write(msg, false, promise);
    	return promise;
    }
    
    @Override
    public ChannelFuture write(final Object msg, final ChannelPromise promise) {
    	write(msg, false, promise);
    	return promise;
    }
    
    @Override
    public ChannelHandlerContext flush() {
    	final AbstractChannelHandlerContext next = findContextOutbound(MASK_FLUSH);
    	EventExecutor executor = next.executor();
    	if (executor.inEventLoop()) {
    		if (next.invokeHandler()) {
    			try {
    				final ChannelHandler handler = next.handler();
    				final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
    				if (handler == headContext) {
    					headContext.flush(next);
    				} else if (handler instanceof ChannelDuplexHandler) {
    					((ChannelDuplexHandler) handler).flush(next);
    				} else if (handler instanceof ChannelOutboundHandlerAdapter) {
    					((ChannelOutboundHandlerAdapter) handler).flush(next);
    				} else {
    					((ChannelOutboundHandler) handler).flush(next);
    				}
    			} catch (Throwable t) {
    				next.invokeExceptionCaught(t);
    			}
    		} else {
    			next.flush();
    		}
    	} else {
    		safeExecute(executor, getInvokeTasks().flushTask, channel().voidPromise(), null, false);
    	}
    	return this;
    }
	
    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
    	write(msg, true, promise);
    	return promise;
    }
    
    void write(Object msg, boolean flush, ChannelPromise promise) {
    	if (validateWrite(msg, promise)) {
    		final AbstractChannelHandlerContext next = findContextOutbound(flush ? 
    				MASK_WRITE | MASK_FLUSH : MASK_WRITE);
    		final Object m = pipeline.touch(msg, next);
    		EventExecutor executor = next.executor();
    		if (executor.inEventLoop()) {
    			if (next.invokeHandler()) {
    				promise = ensurePromiseUseCorrectExecutor(promise);
    				try {
    					final ChannelHandler handler = next.handler();
    					final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
    					if (handler == headContext) {
    						headContext.write(next, msg, promise);
    					} else if (handler instanceof ChannelDuplexPromise) {
    						((ChannelDuplexHandler) handler).write(next, msg, promise);
    					} else if (handler instanceof ChannelOutboundHandlerAdapter) {
    						((ChannelOutboundHandlerAdapter) handler).write(next, msg, promise);
    					} else {
    						((ChannelOutboundHandler) handler).write(next, msg, promise);
    					}
    				} catch (Throwable t) {
    					notifyOutboundHandlerException(t, promise);
    				}
    				if (flush) {
    					try {
    						final ChannelHandler handler = next.handler();
    						final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
    						if (handler == headContext) {
    							headContext.flush(next);
    						} else if (handler instanceof ChannelDuplexHandler) {
    							((ChannelDuplexHandler) handler).flush(next);
    						} else if (handler instanceof ChannelOutboundHandlerAdapter) {
    							((ChannelOutboundHandlerAdapter) handler).flush(next);
    						} else {
    							((ChannelOutboundHandler) handler).flush(next);
    						}
    					} catch (Throwable t) {
    						next.invokeExceptionCaught(t);
    					}
    				}
    			} else {
    				next.write(msg, flush, promise);
    			}
    		} else {
    			final WriteTask task = WriteTask.newInstance(this, m, promise, flush);
    			if (!safeExecute(executor, task, promise, m, !flush)) {
    				task.cancel();
    			}
    		}
    	}
    }
    
    private boolean validateWrite(Object msg, ChannelPromise promise) {
    	ObjectUtil.checkNotNull(msg, "msg");
    	try {
    		if (isNotValidPromise(promise, true)) {
    			ReferenceCountUtil.release(msg);
    			return false;
    		}
    	} catch (RuntimeException e) {
    		ReferenceCountUtil.release(msg);
    		throw e;
    	}
    	return true;
    }
    
    @Override
    public ChannelFuture writeAndFlush(Object msg) {
    	return writeAndFlush(msg, newPromise());
    }
    
    private static void notifyOutboundHandlerException(Throwable cause, ChannelPromise promise) {
    	PromiseNotificationUtil.tryFailure(promise, cause, promise instanceof VoidChannelPromise ? null : logger);
    }
    
    @Override
    public ChannelPromise newPromise() {
    	return new DefaultChannelPromise(channel(), executor());
    }
    
    @Override
    public ChannelProgressivePromise newProgressivePromise() {
    	return new DefaultChannelProgressivePromise(channel(), executor());
    }
    
    @Override
    public ChannelFuture newSucceededFuture() {
    	ChannelFuture succeededFuture = this.succeededFuture;
    	if (succeededFuture == null) {
    		this.succeededFuture = succeededFuture = new SucceededChannelFuture(channel(), executor());
    	}
    	return succeededFuture;
    }
    
    @Override
    public ChannelFuture newFailedFuture(Throwable cause) {
    	return new FailedChannelFuture(channel(), executor(), cause);
    }
    
    private boolean isNotValidPromise(ChannelPromise promise, boolean allowVoidPromise) {
    	ObjectUtil.checkNotNull(promise, "promise");
    	
    	if (promise.isDone()) {
    		if (promise.isCancelled()) {
    			return true;
    		}
    		throw new IllegalArgumentException("promise already done: " + promise);
    	}
    	
    	if (promise.channel() != channel()) {
    		throw new IllegalArgumentException(String.format(
    				"promise.channel does not match: %s (expected: %s)", promise.channel(), channel()));
    	}
    	
    	if (promise.getClass() == DefaultChannelPromise.class) {
    		return false;
    	}
    	
    	if (!allowVoidPromise && promise instanceof VoidChannelPromise) {
    		throw new IllegalArgumentException(
    				StringUtil.simpleClassName(VoidChannelPromise.class) + " not allowed for this operation");
    	}
    	
    	if (promise instanceof AbstractChannel.CloseFuture) {
    		throw new IllegalArgumentException(
    				StringUtil.simpleClassName(AbstractChannel.CloseFuture.class) + " not allowed in a pipeline");
    	}
    	return false;
    }
    
    private AbstractChannelHandlerContext findContextInbound(int mask) {
    	AbstractChannelHandlerContext ctx = this;
    	EventExecutor currentExecutor = executor();
    	do {
    		ctx = ctx.next;
    	} while (skipContext(ctx, currentExecutor, mask, MASK_ONLY_INBOUND));
    	return ctx;
    }
    
    private AbstractChannelHandlerContext findContextOutbound(int mask) {
    	AbstractChannelHandlerContext ctx = this;
    	EventExecutor currentExecutor = executor();
    	do {
    		ctx = ctx.prev;
    	} while (skipContext(ctx, currentExecutor, mask, MASK_ONLY_OUTBOUND));
    	return ctx;
    }
    
    private static boolean skipContext(
    		AbstractChannelHandlerContext ctx, EventExecutor currentExecutor, int mask, int onlyMask) {
    	return (ctx.executionMask & (onlyMask | mask)) == 0 ||
    			(ctx.executor() == currentExecutor && (ctx.executionMask & mask) == 0);
    }
    
    @Override
    public ChannelPromise voidPromise() {
    	return channel().voidPromise();
    }
    
    final void setRemoved() {
    	handlerState = REMOVE_COMPLETE;
    }
    
    final boolean setAddComplete() {
    	for (;;) {
    		int oldState = handlerState;
    		if (oldState == REMOVE_COMPLETE) {
    			return false;
    		}
    		if (HANDLER_STATE_UPDATER.compareAndSet(this, oldState, ADD_COMPLETE)) {
    			return true;
    		}
    	}
    }
    
    final void setAddPending() {
    	boolean updated = HANDLER_STATE_UPDATER.compareAndSet(this, INIT, ADD_PENDING);
    	assert updated;
    }
    
    final void callHandlerAdded() throws Exception {
    	if (setAddComplete()) {
    		handler().handlerAdded(this);
    	}
    }
    
    final void callHandlerRemoved() throws Exception {
    	try {
    		if (handlerState == ADD_COMPLETE) {
    			handler().handlerRemoved(this);
    		}
    	} finally {
    		setRemoved();
    	}
    }
    
    boolean invokeHandler() {
    	int handlerState = this.handlerState;
    	return handlerState == ADD_COMPLETE || (!ordered && handlerState == ADD_PENDING);
    }
    
    @Override
    public boolean isRemoved() {
    	return handlerState == REMOVE_COMPLETE;
    }
    
    @Override
    public <T> Attribute<T> attr(AttributeKey<T> key) {
    	return channel().attr(key);
    }
    
    @Override
    public <T> boolean hasAttr(AttributeKey<T> key) {
    	return channel().hasAttr(key);
    }
    
    private static boolean safeExecute(EventExecutor executor, Runnable runnable,
    		ChannelPromise promise, Object msg, boolean lazy) {
    	try {
    		if (lazy && executor instanceof AbstractEventExecutor) {
    			((AbstractEventExecutor) executor).lazyExecute(runnable);
    		} else {
    			executor.execute(runnable);
    		}
    		return true;
    	} catch (Throwable cause) {
    		try {
    			if (msg != null) {
    				ReferenceCountUtil.release(msg);
    			}
    		} finally {
    			promise.setFailure(cause);
    		}
    		return false;
    	}
    }
    
    @Override
    public String toHintString() {
    	return '\'' + name + "' will handle the message from this point.";
    }
    
    @Override
    public String toString() {
    	return StringUtil.simpleClassName(ChannelHandlerContext.class) + '(' + name + ", " + channel() + ')';
    }
    
    Tasks getInvokeTasks() {
    	Tasks tasks = invokeTasks;
    	if (tasks == null) {
    		invokeTasks = tasks = new Tasks(this);
    	}
    	return tasks;
    }
    
	static final class WriteTask implements Runnable {
		private static final Recycler<WriteTask> RECYCLER = new Recycler<WriteTask>() {
			@Override
			protected WriteTask newObject(Handle<WriteTask> handle) {
				return new WriteTask(handle);
			}
		};
		
		static WriteTask newInstance(AbstractChannelHandlerContext ctx, 
				Object msg, ChannelPromise promise, boolean flush) {  
			WriteTask task = RECYCLER.get();
			init(task, ctx, msg, promise, flush);
			return task;
		}
		
		private static final boolean ESTIMATE_TASK_SIZE_ON_SUBMIT = 
				SystemPropertyUtil.getBoolean("io.netty.transport.estimateSizeOnSubmit", true);
		
		private static final int WRITE_TASK_OVERHEAD =
				SystemPropertyUtil.getInt("io.netty.transport.writeTaskSizeOverhead", 32);
		
		private final Handle<WriteTask> handle;
		private AbstractChannelHandlerContext ctx;
		private Object msg;
		private ChannelPromise promise;
		private int size;
		
		private WriteTask(Handle<WriteTask> handle) {
			this.handle = handle;
		}
		
		static void init(WriteTask task, AbstractChannelHandlerContext ctx,
				Object msg, ChannelPromise promise, boolean flush) {
			task.ctx = ctx;
			task.msg = msg;
			task.promise = promise;
			if (ESTIMATE_TASK_SIZE_ON_SUBMIT) {
				task.size = ctx.pipeline.estimatorHandle().size(msg) + WRITE_TASK_OVERHEAD;
				ctx.pipeline.incrementPendingOutboundBytes(task.size);
			} else {
				task.size = 0;
			}
			if (flush) {
				task.size |= Integer.MIN_VALUE;
			}
		}
		
		@Override
		public void run() {
			try {
				decrementPendingOutboundBytes();
				ctx.write(msg, size < 0, promise);
			} finally {
				recycle();
			}
		}
		
		void cancel() {
			try {
				decrementPendingOutboundBytes();
			} finally {
				recycle();
			}
		}
		
		private void decrementPendingOutboundBytes() {
			if (ESTIMATE_TASK_SIZE_ON_SUBMIT) {
				ctx.pipeline.decrementPendingOutboundBytes(size & Integer.MAX_VALUE);
			}
		}
		
		private void recycle() {
			ctx = null;
			msg = null;
			promise = null;
			handle.recycle(this);
		}
	}

	static final class Tasks {
		final Runnable fireChannelReadCompleteTask;
		private final Runnable readTask;
		private final Runnable fireChannelWritabilityChangedTask;
		private final Runnable flushTask;
		
		Tasks(AbstractChannelHandlerContext ctx) {
			fireChannelReadCompleteTask = ctx::fireChannelReadComplete;
			readTask = ctx::read;
			fireChannelWritabilityChangedTask = ctx::fireChannelWritabilityChanged;
			flushTask = ctx::flush;
		}
	}
}
