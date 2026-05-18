package netty.channel;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.WeakHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import netty.channel.Channel.Unsafe;
import netty.common.util.ReferenceCountUtil;
import netty.common.util.ResourceLeakDetector;
import netty.common.util.concurrent.EventExecutor;
import netty.common.util.concurrent.EventExecutorGroup;
import netty.common.util.concurrent.FastThreadLocal;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.StringUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public class DefaultChannelPipeline implements ChannelPipeline {
	
	static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultChannelPipeline.class);
	private static final String HEAD_NAME = generateName0(HeadContext.class);
	private static final String TAIL_NAME = generateName0(TailContext.class);
	
	private static final FastThreadLocal<Map<Class<?>, String>> nameCaches = 
			new FastThreadLocal<Map<Class<?>, String>>() {
		@Override
		protected Map<Class<?>, String> initialValue() {
			return new WeakHashMap<Class<?>, String>();
		}
	};
	
	private static final AtomicReferenceFieldUpdater<DefaultChannelPipeline, MessageSizeEstimator.Handle> ESTIMATOR = 
			AtomicReferenceFieldUpdater.newUpdater(DefaultChannelPipeline.class, MessageSizeEstimator.Handle.class, "estimatorHandle");
	final HeadContext head;
	final TailContext tail;
	
	private final Channel channel;
	private final ChannelFuture succeededFuture;
	private final VoidChannelPromise voidPromise;
	private final boolean touch = ResourceLeakDetector.isEnabled();
	
	private Map<EventExecutorGroup, EventExecutor> childExecutors;
	private volatile MessageSizeEstimator.Handle estimatorHandle;
	private boolean firstRegistration = true;
	
	private PendingHandlerCallback pendingHandlerCallbackHead;
	
	private boolean registered;
	
	protected DefaultChannelPipeline(Channel channel) {
		this.channel = ObjectUtil.checkNotNull(channel, "channel");
		succeededFuture = new SucceededChannelFuture(channel, null);
		voidPromise = new VoidChannelPromise(channel, true);
		tail = new TailContext(this);
		head = new HeadContext(this);
		
		head.prev = tail;
		tail.next = head;
	}
	
	final MessageSizeEstimator.Handle estimatorHandle() {
		MessageSizeEstimator.Handle handle = estimatorHandle;
		if (handle == null) {
			handle = channel.config().getMessageSizeEstimator().newHandle();
			if (!ESTIMATOR.compareAndSet(this, null, handle)) {
				handle = estimatorHandle;
			}
		}
		return handle;
	}
	
	final Object touch(Object msg, AbstractChannelHandlerContext next) {
		return touch ? ReferenceCountUtil.touch(msg, next) : msg;
	}
	
	private AbstractChannelHandlerContext newContext(EventExecutorGroup group, String name, ChannelHandler handler) {
		return new DefaultChannelHandlerContext(this, childExecutor(group), name, handler);
	}
	
	private EventExecutor childExecutor(EventExecutorGroup group) {
		if (group == null) {
			return null;
		}
		Boolean pinEventExecutor = channel.config().getOption(ChannelOption.SINGLE_EVENTEXECUTOR_PER_GROUP);
		if (pinEventExecutor != null && !pinEventExecutor) {
			return group.next();
		}
		Map<EventExecutorGroup, EventExecutor> childExecutors = this.childExecutors;
		if (childExecutors == null ) {
			childExecutors = this.childExecutors = new IdentityHashMap<EventExecutorGroup, EventExecutor>(4);
		}
		EventExecutor childExecutor = childExecutors.get(group);
		if (childExecutor == null) {
			childExecutor = group.next();
			childExecutors.put(group, childExecutor);
		}
		return childExecutor;
	}
	
	@Override
	public final Channel channel() {
		return channel;
	}
	
	@Override
	public final ChannelPipeline addFirst(String name, ChannelHandler handler) {
		return addFirst(null, name, handler);
	}
	
	private enum AddStrategy {
		ADD_FIRST,
		ADD_LAST,
		ADD_BEFORE,
		ADD_AFTER;
	}
	
	private ChannelPipeline internalAdd(EventExecutorGroup group, String name,
			ChannelHandler handler, String baseName,
			AddStrategy addStrategy) {
		final AbstractChannelHandlerContext newCtx;
		synchronized (this) {
			checkMultiplicity(handler);
			name = filterName(name, handler);
			newCtx = newContext(group, name, handler);
			switch (addStrategy) {
			case ADD_FIRST:
				addFirst0(newCtx);
				break;
			case ADD_LAST:
				addLast0(newCtx);
				break;
			case ADD_BEFORE:
				addBefore0(getContextOrDie(baseName), newCtx);
				break;
			case ADD_AFTER:
				addAfter0(getContextOrDie(baseName), newCtx);
				break;
			default:
				throw new IllegalArgumentException("unknown add strategy: " + addStrategy);
			}
			
			if (!registered) {
				newCtx.setAddPending();
				callHandlerCallbackLater(newCtx, true);
				return this;
			}
			
			EventExecutor executor = newCtx.executor();
			if (!executor.inEventLoop()) {
				callHandlerAddedInEventLoop(newCtx, executor);
				return this;
			}
		}
		callHandlerAdded0(newCtx);
		return this;
	}
	
	@Override
	public final ChannelPipeline addFirst(EventExecutorGroup group, String name, ChannelHandler handler) {
		return internalAdd(group, name, handler, null, AddStrategy.ADD_FIRST);
	}
	
	private void addFirst0(AbstractChannelHandlerContext newCtx) {
		AbstractChannelHandlerContext nextCtx = head.next;
		newCtx.prev = head;
		newCtx.next = nextCtx;
		head.next = newCtx;
		nextCtx.prev = newCtx;
	}
	
	@Override
	public final ChannelPipeline addLast(String name, ChannelHandler handler) {
		return addLast(null, name, handler);
	}
	
	@Override
	public final ChannelPipeline addLast(EventExecutorGroup  group, String name, ChannelHandler handler) {
		return internalAdd(group, name, handler, null, AddStrategy.ADD_LAST);
	}
	
	private void addLast0(AbstractChannelHandlerContext newCtx) {
		AbstractChannelHandlerContext prev = tail.prev;
		newCtx.prev = prev;
		newCtx.next = tail;
		prev.next = newCtx;
		tail.prev = newCtx;
	}
	
	@Override
	public final ChannelPipeline addBefore(String baseName, String name, ChannelHandler handler) {
		return addBefore(null, baseName, name, handler);
	}
	
	@Override
	public final ChannelPipeline addBefore(
			EventExecutorGroup group, String baseName, String name, ChannelHandler handler) {
		return internalAdd(group, name, handler, baseName, AddStrategy.ADD_BEFORE);
	}
	
	private static void addBefore0(AbstractChannelHandlerContext ctx, AbstractChannelHandlerContext newCtx) {
		newCtx.prev = ctx.prev;
		newCtx.next = ctx;
		ctx.prev.next = newCtx;
		ctx.prev = newCtx;
	}
	
	private String filterName(String name, ChannelHandler handler) {
		if (name == null) {
			return generateName(handler);
		}
		checkDuplicateName(name);
		return name;
	}
	
	@Override
	public final ChannelPipeline addAfter(String baseName, String name, ChannelHandler handler) {
		return addAfter(null, baseName, name, handler);
	}
	
	@Override
	public final ChannelPipeline addAfter(
			EventExecutorGroup group, String baseName, String name, ChannelHandler handler) {
		return internalAdd(group, name, handler, baseName, AddStrategy.ADD_AFTER);
	}
	
	private static void addAfter0(AbstractChannelHandlerContext ctx, AbstractChannelHandlerContext newCtx) {
		newCtx.prev = ctx;
		newCtx.next = ctx.next;
		ctx.next.prev = newCtx;
		ctx.next = newCtx;
	}
	
	public final ChannelPipeline addFirst(ChannelHandler handler) {
		return addFirst(null, handler);
	}
	
	@Override
	public final ChannelPipeline addFirst(ChannelHandler... handlers) {
		return addFirst(null, handlers);
	}
	
	@Override
	public final ChannelPipeline addFirst(EventExecutorGroup executor, ChannelHandler... handlers) {
		ObjectUtil.checkNotNull(handlers, "handlers");
		if (handlers.length == 0 || handlers[0] == null) {
			return this;
		}
		int size;
		for (size = 1; size < handlers.length; size ++) {
			if (handlers[size] == null) {
				break;
			}
		}
		for (int i = size - 1; i >= 0; i--) {
			ChannelHandler h = handlers[i];
			addFirst(executor, null, h);
		}
		return this;
	}
	
	public final ChannelPipeline addLast(ChannelHandler handler) {
		return addLast(null, handler);
	}
	
	@Override
	public final ChannelPipeline addLast(ChannelHandler... handlers) {
		return addLast(null, handlers);
	}
	
	@Override
	public final ChannelPipeline addLast(EventExecutorGroup executor, ChannelHandler... handlers) {
		ObjectUtil.checkNotNull(handlers, "handlers");
		
		for (ChannelHandler h : handlers) {
			if (h == null) {
				break;
			}
			addLast(executor, null, h);
		}
		return this;
	}
	
	private String generateName(ChannelHandler handler) {
		Map<Class<?>, String> cache = nameCaches.get();
		Class<?> handlerType = handler.getClass();
		String name = cache.get(handlerType);
		if (name == null) {
			name = generateName0(handlerType);
			cache.put(handlerType, name);
		}
		
		if (context0(name) != null) {
			String baseName = name.substring(0, name.length() - 1);
			for (int i = 1; ; i++) {
				String newName = baseName + i;
				if (context0(newName) == null) {
					name = newName;
					break;
				}
			}
		}
		return name;
	}
	
	private static String generateName0(Class<?> handlerType) {
		return StringUtil.simpleClassName(handlerType) + "#0";
	}
	
	@Override
	public final ChannelPipeline remove(ChannelHandler handler) {
		remove(getContextOrDie(handler));
		return this;
	}
	
	@Override
	public final ChannelHandler remove(String name) {
		return remove(getContextOrDie(name)).handler();
	}
	
	@Override
	public final <T extends ChannelHandler> T remove(Class<T> handlerType) {
		return (T) remove(getContextOrDie(handlerType)).handler();
	}
	
	public final <T extends ChannelHandler> T removeIfExists(String name) {
		return removeIfExists(context(name));
	}
	
	public final <T extends ChannelHandler> T removeIfExists(Class<T> handlerType) {
		return removeIfExists(context(handlerType));
	}
	
	public final <T extends ChannelHandler> T removeIfExists(ChannelHandler handler) {
		return removeIfExists(context(handler));
	}
	
	@SuppressWarnings("unchecked")
	private <T extends ChannelHandler> T removeIfExists(ChannelHandlerContext ctx) {
		if (ctx == null) {
			return null;
		}
		return (T) remove((AbstractChannelHandlerContext) ctx).handler();
	}
	
	private AbstractChannelHandlerContext remove(final AbstractChannelHandlerContext ctx) {
		assert ctx != head && ctx != tail;
		
		synchronized (this) {
			atomicRemoveFromHandlerList(ctx);
			if (!registered) {
				callHandlerCallbackLater(ctx, false);
				return ctx;
			}
			EventExecutor executor = ctx.executor();
			if (!executor.inEventLoop()) {
				executor.execute(new Runnable() {
					@Override
					public void run() {
						callHandlerRemoved0(ctx);
					}
				});
				return ctx;
			}
		} 
		callHandlerRemoved0(ctx);
		return ctx;
	}
	
	private synchronized void atomicRemoveFromHandlerList(AbstractChannelHandlerContext ctx) {
		AbstractChannelHandlerContext prev = ctx.prev;
		AbstractChannelHandlerContext next = ctx.next;
		prev.next = next;
		next.prev = prev;
	}
	
	@Override
	public final ChannelHandler removeFirst() {
		if (head.next == tail) {
			throw new NoSuchElementException();
		}
		return remove(head.next).handler();
	}
	
	@Override
	public final ChannelHandler removeLast() {
		if (head.next == tail) {
			throw new NoSuchElementException();
		}
		return remove(tail.prev).handler();
	}
	
	@Override
	public final ChannelPipeline replace(ChannelHandler oldHandler, String newName, ChannelHandler newHandler) {
		replace(getContextOrDie(oldHandler), newName, newHandler);
		return this;
	}
	
	@Override
	public final ChannelHandler replace(String oldName, String newName, ChannelHandler newHandler) {
		return replace(getContextOrDie(oldName), newName, newHandler);
	}
	
	@Override
	public final <T extends ChannelHandler> T replace(
			Class<T> oldHandlerType, String newName, ChannelHandler newHandler) {
		return (T) replace(getContextOrDie(oldHandlerType), newName, newHandler);
	}
	
	private ChannelHandler replace(
			final AbstractChannelHandlerContext ctx, String newName, ChannelHandler newHandler) {
		assert ctx != head && ctx != tail;
		final AbstractChannelHandlerContext newCtx;
		synchronized (this) {
			checkMultiplicity(newHandler);
			if (newName == null) {
				newName = generateName(newHandler);
			} else {
				boolean sameName = ctx.name().equals(newName);
				if (!sameName) {
					checkDuplicateName(newName);
				}
			}
			
			newCtx = newContext(ctx.childExecutor, newName, newHandler);
			replace0(ctx, newCtx);
			
			if (!registered) {
				callHandlerCallbackLater(newCtx, true);
				callHandlerCallbackLater(ctx, false);
				return ctx.handler();
			}
			EventExecutor executor = ctx.executor();
			if (!executor.inEventLoop()) {
				executor.execute(new Runnable() {
					@Override
					public void run() {
						callHandlerAdded0(newCtx);
						callHandlerRemoved0(ctx);
					}
				});
				return ctx.handler();
			}
		}
		callHandlerAdded0(newCtx);
		callHandlerRemoved0(ctx);
		return ctx.handler();
	}
	
	private static void replace0(AbstractChannelHandlerContext oldCtx, AbstractChannelHandlerContext newCtx) {
		AbstractChannelHandlerContext prev = oldCtx.prev;
		AbstractChannelHandlerContext next = oldCtx.next;
		newCtx.prev = prev;
		newCtx.next = next;
		
		prev.next = newCtx;
		next.prev = newCtx;
		
		oldCtx.prev = newCtx;
		oldCtx.next = newCtx;
	}
	
	private static void checkMultiplicity(ChannelHandler handler) {
		if (handler instanceof ChannelHandlerAdapter) {
			ChannelHandlerAdapter h = (ChannelHandlerAdapter) handler;
			if (!h.isSharable() && h.added) {
				throw new ChannelPipelineException(
						h.getClass().getName() + 
						" is not a @Sharable handler, so can't be added or removed multiple times.");
			}
			h.added = true;
		}
	}
	
	private void callHandlerAdded0(final AbstractChannelHandlerContext ctx) {
		try {
			ctx.callHandlerAdded();
		} catch (Throwable t) {
			boolean removed = false;
			try {
				atomicRemoveFromHandlerList(ctx);
				ctx.callHandlerRemoved();
				removed = true;
			} catch (Throwable t2) {
				if (logger.isWarnEnabled()) {
					logger.warn("Failed to remove a handler: " + ctx.name(), t2);
				}
			}
			if (removed) {
				fireExceptionCaught(new ChannelPipelineException(
						ctx.handler().getClass().getName() + 
						".handlerAdded() has thrown an exception; removed.", t));
			} else {
				fireExceptionCaught(new ChannelPipelineException(
						ctx.handler().getClass().getName() + 
						".handlerAdded() has thrown an exception; also failed to remove.", t));
			}
		}
	}
	
	private void callHandlerRemoved0(final AbstractChannelHandlerContext ctx) {
		try {
			ctx.callHandlerRemoved();
		} catch (Throwable t) {
			fireExceptionCaught(new ChannelPipelineException(
					ctx.handler().getClass().getName() + ".handlerRemoved() has thrown an exception.", t));
		}
	}
	
	final void invokeHandlerAddedIfNeeded() {
		assert channel.eventLoop().inEventLoop();
		if (firstRegistration) {
			firstRegistration = false;
			callHandlerAddedForAllHandlers();
		}
	}
	
	@Override
	public final ChannelHandler first() {
		ChannelHandlerContext first = firstContext();
		if (first == null) {
			return null;
		}
		return first.handler();
	}
	
	@Override
	public final ChannelHandlerContext firstContext() {
		AbstractChannelHandlerContext first = head.next;
		if (first == tail) {
			return null;
		}
		return head.next;
	}
	
	@Override
	public final ChannelHandler last() {
		ChannelHandlerContext last = lastContext();
		if (last == null) {
			return null;
		}
		return last.handler();
	}
	
	@Override
	public final ChannelHandlerContext lastContext() {
		AbstractChannelHandlerContext last = tail.prev;
		if (last == head) {
			return null;
		}
		return tail.prev;
	}
	
	@Override
	public final ChannelHandler get(String name) {
		ChannelHandlerContext ctx = context(name);
		if (ctx == null) {
			return null;
		} else {
			return ctx.handler();
		}
	}
	
	@SuppressWarnings("unchecked")
	public final <T extends ChannelHandler> T get(Class<T> handlerType) {
		ChannelHandlerContext ctx = context(handlerType);
		if (ctx == null) {
			return null;
		} else {
			return (T) ctx.handler();
		}
	}
	
	@Override
	public final ChannelHandlerContext context(String name) {
		return context0(ObjectUtil.checkNotNull(name, "name"));
	}
	
	@Override
	public final ChannelHandlerContext context(ChannelHandler handler) {
		ObjectUtil.checkNotNull(handler, "handler");
		
		AbstractChannelHandlerContext ctx = head.next;
		for (;;) {
			if (ctx == null) {
				return null;
			}
			if (ctx.handler() == handler) {
				return ctx;
			}
			ctx = ctx.next;
		}
	}
	
	@Override
	public final ChannelHandlerContext context(Class<? extends ChannelHandler> handlerType) {
		ObjectUtil.checkNotNull(handlerType, "handlerType");
		
		AbstractChannelHandlerContext ctx = head.next;
		for (;;) {
			if (ctx == null) {
				return null;
			}
			if (handlerType.isAssignableFrom(ctx.handler().getClass())) {
				return ctx;
			}
			ctx = ctx.next;
		}
	}
	
	@Override
	public final List<String> names() {
		List<String> list = new ArrayList<String>();
		AbstractChannelHandlerContext ctx = head.next;
		for (;;) {
			if (ctx == null) {
				return list;
			}
			list.add(ctx.name());
			ctx = ctx.next;
		}
	}
	
	@Override
	public final Map<String, ChannelHandler> toMap() {
		Map<String, ChannelHandler> map = new LinkedHashMap<String, ChannelHandler>();
		AbstractChannelHandlerContext ctx = head.next;
		for (;;) {
			if (ctx == tail) {
				return map;
			}
			map.put(ctx.name(), ctx.handler());
			ctx = ctx.next;
		}
	}
	
	@Override
	public final Iterator<Map.Entry<String, ChannelHandler>> iterator() {
		return toMap().entrySet().iterator();
	}
	
	@Override
    public final String toString() {
        StringBuilder buf = new StringBuilder()
            .append(StringUtil.simpleClassName(this))
            .append('{');
        AbstractChannelHandlerContext ctx = head.next;
        for (;;) {
            if (ctx == tail) {
                break;
            }

            buf.append('(')
               .append(ctx.name())
               .append(" = ")
               .append(ctx.handler().getClass().getName())
               .append(')');

            ctx = ctx.next;
            if (ctx == tail) {
                break;
            }

            buf.append(", ");
        }
        buf.append('}');
        return buf.toString();
    }
	
	@Override
	public final ChannelPipeline fireChannelRegistered() {
		if (head.executor().inEventLoop()) {
			if (head.invokeHandler()) {
				head.channelRegistered(head);
			} else {
				head.fireChannelRegistered();
			}
		} else {
			head.executor().execute(this::fireChannelRegistered);
		}
		return this;
	}
	
	@Override
	public final ChannelPipeline fireChannelUnregistered() {
		if(head.executor().inEventLoop()) {
			if (head.invokeHandler()) {
				head.channelUnregistered(head);
			} else {
				head.fireChannelUnregistered();
			} 
		} else {
			head.executor().execute(this::fireChannelUnregistered);
		}
		return this;
	}
	
	private synchronized void destroy() {
		destroyUp(head.next, false);
	}
	
	private void destroyUp(AbstractChannelHandlerContext ctx, boolean inEventLoop) {
		final Thread currentThread = Thread.currentThread();
		final AbstractChannelHandlerContext tail = this.tail;
		for (;;) {
			if (ctx == tail) {
				destroyDown(currentThread, tail.prev, inEventLoop);
				break;
			}
			final EventExecutor executor = ctx.executor();
			if (!inEventLoop && !executor.inEventLoop(currentThread)) {
				final AbstractChannelHandlerContext finalCtx = ctx;
				executor.execute(new Runnable() {
					@Override
					public void run() {
						destroyUp(finalCtx, true);
					}
				});
				break;
			}
			ctx = ctx.next;
			inEventLoop = false;
		}
	}
	
	private void destroyDown(Thread currentThread, AbstractChannelHandlerContext ctx, boolean inEventLoop) {
		final AbstractChannelHandlerContext head = this.head;
		for (;;) {
			if (ctx == head) {
				break;
			}
			final EventExecutor executor = ctx.executor();
			if (inEventLoop || executor.inEventLoop(currentThread)) {
				atomicRemoveFromHandlerList(ctx);
				callHandlerRemoved0(ctx);
			} else {
				final AbstractChannelHandlerContext finalCtx = ctx;
				executor.execute(new Runnable() {
					@Override
					public void run() {
						destroyDown(Thread.currentThread(), finalCtx, true);
					}
				});
				break;
			}
			ctx = ctx.prev;
			inEventLoop = false;
		}
	}
	
	@Override
	public final ChannelPipeline fireChannelActive() {
		if (head.executor().inEventLoop()) {
			if (head.invokeHandler()) {
				head.channelActive(head);
			} else {
				head.fireChannelActive();
			}
		} else {
			head.executor().execute(this::fireChannelActive);
		}
		return this;
	}
	
	@Override
	public final ChannelPipeline fireChannelInactive() {
		if (head.executor().inEventLoop()) {
			if (head.invokeHandler()) {
				head.channelInactive(head);
			} else {
				head.fireChannelInactive();
			}
		} else {
			head.executor().execute(this::fireChannelInactive);
		}
		return this;
	}
	
	@Override
	public final ChannelPipeline fireExceptionCaught(Throwable cause) {
		if (head.executor().inEventLoop()) {
			if (head.invokeHandler()) {
				head.exceptionCaught(head, cause);
			} else {
				head.fireExceptionCaught(cause);
			}
		} else {
			head.executor().execute(() -> fireExceptionCaught(cause));
		}
		return this;
	}
	
	@Override
    public final ChannelPipeline fireUserEventTriggered(Object event) {
        if (head.executor().inEventLoop()) {
            if (head.invokeHandler()) {
                head.userEventTriggered(head, event);
            } else {
                head.fireUserEventTriggered(event);
            }
        } else {
            head.executor().execute(() -> fireUserEventTriggered(event));
        }
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelRead(Object msg) {
        if (head.executor().inEventLoop()) {
            if (head.invokeHandler()) {
                head.channelRead(head, msg);
            } else {
                head.fireChannelRead(msg);
            }
        } else {
            head.executor().execute(() -> fireChannelRead(msg));
        }
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelReadComplete() {
        if (head.executor().inEventLoop()) {
            if (head.invokeHandler()) {
                head.channelReadComplete(head);
            } else {
                head.fireChannelReadComplete();
            }
        } else {
            head.executor().execute(this::fireChannelReadComplete);
        }
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelWritabilityChanged() {
        if (head.executor().inEventLoop()) {
            if (head.invokeHandler()) {
                head.channelWritabilityChanged(head);
            } else {
                head.fireChannelWritabilityChanged();
            }
        } else {
            head.executor().execute(this::fireChannelWritabilityChanged);
        }
        return this;
    }

    
    @Override
    public final ChannelFuture bind(SocketAddress localAddress) {
    	return tail.bind(localAddress);
    }
    
    @Override
    public final ChannelFuture connect(SocketAddress remoteAddress) {
    	return tail.connect(remoteAddress);
    }
    
    @Override
    public final ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
    	return tail.connect(remoteAddress, localAddress);
    }
    
    @Override
    public final ChannelFuture disconnect() {
    	return tail.disconnect();
    }
    
    @Override
    public final ChannelFuture close() {
    	return tail.close();
    }
    
    @Override
    public final ChannelFuture deregister() {
        return tail.deregister();
    }

    @Override
    public final ChannelPipeline flush() {
        tail.flush();
        return this;
    }

    @Override
    public final ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        return tail.bind(localAddress, promise);
    }

    @Override
    public final ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        return tail.connect(remoteAddress, promise);
    }

    @Override
    public final ChannelFuture connect(
            SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
        return tail.connect(remoteAddress, localAddress, promise);
    }

    @Override
    public final ChannelFuture disconnect(ChannelPromise promise) {
        return tail.disconnect(promise);
    }

    @Override
    public final ChannelFuture close(ChannelPromise promise) {
        return tail.close(promise);
    }

    @Override
    public final ChannelFuture deregister(final ChannelPromise promise) {
        return tail.deregister(promise);
    }

    @Override
    public final ChannelPipeline read() {
        tail.read();
        return this;
    }

    @Override
    public final ChannelFuture write(Object msg) {
        return tail.write(msg);
    }

    @Override
    public final ChannelFuture write(Object msg, ChannelPromise promise) {
        return tail.write(msg, promise);
    }

    @Override
    public final ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        return tail.writeAndFlush(msg, promise);
    }

    @Override
    public final ChannelFuture writeAndFlush(Object msg) {
        return tail.writeAndFlush(msg);
    }

    @Override
    public final ChannelPromise newPromise() {
        return new DefaultChannelPromise(channel);
    }

    @Override
    public final ChannelProgressivePromise newProgressivePromise() {
        return new DefaultChannelProgressivePromise(channel);
    }

    @Override
    public final ChannelFuture newSucceededFuture() {
        return succeededFuture;
    }

    @Override
    public final ChannelFuture newFailedFuture(Throwable cause) {
        return new FailedChannelFuture(channel, null, cause);
    }

    @Override
    public final ChannelPromise voidPromise() {
        return voidPromise;
    }

    private void checkDuplicateName(String name) {
    	if (context0(name) != null) {
    		throw new IllegalArgumentException("Duplicate handler name: " + name);
    	}
    }
    
    private AbstractChannelHandlerContext context0(String name) {
    	AbstractChannelHandlerContext context = head.next;
    	while (context != tail) {
    		if (context.name().equals(name)) {
    			return context;
    		}
    		context = context.next;
    	}
    	return null;
    }
	
    private AbstractChannelHandlerContext getContextOrDie(String name) {
    	AbstractChannelHandlerContext ctx = (AbstractChannelHandlerContext) context(name);
    	if (ctx == null) {
    		throw new NoSuchElementException(name);
    	} else {
    		return ctx;
    	}
    }
    
    private AbstractChannelHandlerContext getContextOrDie(ChannelHandler handler) {
    	AbstractChannelHandlerContext ctx = (AbstractChannelHandlerContext) context(handler);
    	if (ctx == null) {
    		throw new NoSuchElementException(handler.getClass().getName());
    	} else {
    		return ctx;
    	}
    }
    
    private AbstractChannelHandlerContext getContextOrDie(Class<? extends ChannelHandler> handlerType) {
    	AbstractChannelHandlerContext ctx = (AbstractChannelHandlerContext) context(handlerType);
    	if (ctx == null) {
    		throw new NoSuchElementException(handlerType.getName());
    	} else {
    		return ctx;
    	}
    }
    
    private void callHandlerAddedForAllHandlers() {
    	final PendingHandlerCallback pendingHandlerCallbackHead;
    	synchronized (this) {
    		assert !registered;
    		registered = true;
    		pendingHandlerCallbackHead = this.pendingHandlerCallbackHead;
    		this.pendingHandlerCallbackHead = null;
    	}
    	
    	PendingHandlerCallback task = pendingHandlerCallbackHead;
    	while (task != null) {
    		task.execute();
    		task = task.next;
    	}
    }
    
    private void callHandlerCallbackLater(AbstractChannelHandlerContext ctx, boolean added) {
    	assert !registered;
    	PendingHandlerCallback task = added ? new PendingHandlerAddedTask(ctx) : new PendingHandlerRemovedTask(ctx);
    	PendingHandlerCallback pending = pendingHandlerCallbackHead;
    	if (pending == null) {
    		pendingHandlerCallbackHead = task;
    	} else {
    		while (pending.next != null) {
    			pending = pending.next;
    		}
    		pending.next = task;
    	}
    }
    
    private void callHandlerAddedInEventLoop(final AbstractChannelHandlerContext newCtx, EventExecutor executor) {
    	newCtx.setAddPending();
    	executor.execute(new Runnable() {
    		@Override
    		public void run() {
    			callHandlerAdded0(newCtx);
    		}
    	});
    }
    
    protected void onUnhandledInboundException(Throwable cause) {
    	try {
    		logger.warn("An exceptionCaught() event was fired, and it reached at the tail of the pipeline. " +
                    "It usually means the last handler in the pipeline did not handle the exception.",
                    cause);
    	} finally {
    		ReferenceCountUtil.release(cause);
    	}
    }
    
    protected void onUnhandledInboundChannelActive() {
    
    }
    
    protected void onUnhandledInboundChannelInactive() {
    	
    }
    
    protected void onUnhandledInboundMessage(Object msg) {
    	try {
    		logger.debug("Discarded inbound message {} that reached at the tail of the pipeline. " +
    				"Please check your pipeline configuration.", msg);
    	} finally {
    		ReferenceCountUtil.release(msg);
    	}
    }
    
    protected void onUnhandledInboundMessage(ChannelHandlerContext ctx, Object msg) {
    	onUnhandledInboundMessage(msg);
    	if (logger.isDebugEnabled()) {
    		logger.debug("Discarded message pipeline: {}.Channel: {}.",
    				ctx.pipeline().names(), ctx.channel());
    	}
    }
    
    protected void onUnhandledInboundChannelReadComplete() {
    	
    }
    
    protected void onUnhandledInboundUserEventTriggered(Object evt) {
    	ReferenceCountUtil.release(evt);
    }
    
    protected void onUnhandledChannelWritabilityChanged() {
    	
    }
    
    protected void incrementPendingOutboundBytes(long size) {
    	ChannelOutboundBuffer buffer = channel.unsafe().outboundBuffer();
    	if (buffer != null) {
    		buffer.incrementPendingOutboundBytes(size);
    	}
    }
    
    protected void decrementPendingOutboundBytes(long size) {
    	ChannelOutboundBuffer buffer = channel.unsafe().outboundBuffer();
    	if (buffer != null) {
    		buffer.decrementPendingOutboundBytes(size);
    	}
    }
    
	final class TailContext extends AbstractChannelHandlerContext implements ChannelInboundHandler {
		TailContext(DefaultChannelPipeline pipeline) {
			super(pipeline, null, TAIL_NAME, TailContext.class);
			setAddComplete();
		}
		
		@Override
		public ChannelHandler handler() {
			return this;
		}
		
		@Override
		public void channelRegistered(ChannelHandlerContext ctx) {}
		
		@Override
		public void channelUnregistered(ChannelHandlerContext ctx) {}
		
		@Override
		public void channelActive(ChannelHandlerContext ctx) {
			onUnhandledInboundChannelActive();
		}
		
		@Override
		public void channelInactive(ChannelHandlerContext ctx) {
			onUnhandledInboundChannelInactive();
		}
		
		@Override
		public void channelWritabilityChanged(ChannelHandlerContext ctx) {
			onUnhandledChannelWritabilityChanged();
		}
		
		@Override
		public void handlerAdded(ChannelHandlerContext ctx) {}
		
		@Override
		public void handlerRemoved(ChannelHandlerContext ctx) {}
		
		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
			onUnhandledInboundUserEventTriggered(evt);
		}
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			onUnhandledInboundException(cause);
		}
		
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			onUnhandledInboundMessage(msg);
		}
		
		@Override
		public void channelReadComplete(ChannelHandlerContext ctx) {
			onUnhandledInboundChannelReadComplete();
		}
	}
	
	final class HeadContext extends AbstractChannelHandlerContext 
			implements ChannelOutboundHandler, ChannelInboundHandler {
		
		private final Unsafe unsafe;
		
		HeadContext(DefaultChannelPipeline pipeline) {
			super(pipeline, null, HEAD_NAME, HeadContext.class);
			unsafe = pipeline.channel().unsafe();
			setAddComplete();
		}
		
		@Override
		public ChannelHandler handler() {
			return this;
		}
		
		@Override
		public void handlerAdded(ChannelHandlerContext ctx) {
			
		}
		
		@Override
		public void handlerRemoved(ChannelHandlerContext ctx) {
			
		}
		
		@Override
		public void bind(
				ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) {
			unsafe.bind(localAddress, promise);
		}
		
		@Override
		public void connect(
				ChannelHandlerContext ctx,
				SocketAddress remoteAddress, SocketAddress localAddress,
				ChannelPromise promise) {
			unsafe.connect(remoteAddress, localAddress, promise);
		}
		
		@Override
		public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) {
			unsafe.disconnect(promise);
		}
		
		@Override
		public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
			unsafe.close(promise);
		}
		
		@Override
		public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) {
			unsafe.deregister(promise);
		}
		
		@Override
		public void read(ChannelHandlerContext ctx) {
			unsafe.beginRead();
		}
		
		@Override
		public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
			unsafe.write(msg, promise);
		}
		
		@Override
		public void flush(ChannelHandlerContext ctx) {
			unsafe.flush();
		}
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			ctx.fireExceptionCaught(cause);
		}
		
		@Override
		public void channelRegistered(ChannelHandlerContext ctx) {
			invokeHandlerAddedIfNeeded();
			ctx.fireChannelRegistered();
		}
		
		@Override
		public void channelUnregistered(ChannelHandlerContext ctx) {
			ctx.fireChannelUnregistered();
			if (!channel.isOpen()) {
				destroy();
			}
		}
		
		@Override
		public void channelActive(ChannelHandlerContext ctx) {
			ctx.fireChannelActive();
			
			readIfIsAutoRead();
		}
		
		@Override
		public void channelInactive(ChannelHandlerContext ctx) {
			ctx.fireChannelInactive();
		}
		
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			ctx.fireChannelRead(msg);
		}
		
		@Override
		public void channelReadComplete(ChannelHandlerContext ctx) {
			ctx.fireChannelReadComplete();
			
			readIfIsAutoRead();
		}
		
		private void readIfIsAutoRead() {
			if (channel.config().isAutoRead()) {
				channel.read();
			}
		}
		
		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
			ctx.fireUserEventTriggered(evt);
		}
		
		@Override
		public void channelWritabilityChanged(ChannelHandlerContext ctx) {
			ctx.fireChannelWritabilityChanged();
		}
	}
	
	private abstract static class PendingHandlerCallback implements Runnable {
		final AbstractChannelHandlerContext ctx;
		PendingHandlerCallback next;
		
		PendingHandlerCallback(AbstractChannelHandlerContext ctx) {
			this.ctx = ctx;
		}
		
		abstract void execute();
	}
	
	private final class PendingHandlerAddedTask extends PendingHandlerCallback {
		
		PendingHandlerAddedTask(AbstractChannelHandlerContext ctx) {
			super(ctx);
		}
		
		@Override
		public void run() {
			callHandlerAdded0(ctx);
		}
		
		@Override
		void execute() {
			EventExecutor executor = ctx.executor();
			if (executor.inEventLoop()) {
				callHandlerAdded0(ctx);
			} else {
				try {
					executor.execute(this);
				} catch (RejectedExecutionException e) {
					if (logger.isWarnEnabled()) {
						logger.warn(
								"Can't invoke handlerAdded() as the EventExecutor {} rejected it, removing handler {}.",
								executor, ctx.name(), e);
					}
					atomicRemoveFromHandlerList(ctx);
					ctx.setRemoved();
				}
			}
		}
	}
	
	private final class PendingHandlerRemovedTask extends PendingHandlerCallback {
		PendingHandlerRemovedTask(AbstractChannelHandlerContext ctx) {
			super(ctx);
		}
		
		@Override
		public void run() {
			callHandlerRemoved0(ctx);
		}
		
		@Override
		void execute() {
			EventExecutor executor = ctx.executor();
			if (executor.inEventLoop()) {
				callHandlerRemoved0(ctx);
			} else {
				try {
					executor.execute(this);
				} catch (RejectedExecutionException e) {
					if (logger.isWarnEnabled()) {
						logger.warn(
								"Can't invoke handlerRemoved() as the EventExecutor {} rejected it," +
									" removing handler {}.", executor, ctx.name(), e);
					}
					ctx.setRemoved();
				}
			}
		}
	}
}
