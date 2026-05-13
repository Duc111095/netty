package netty.channel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.WeakHashMap;

import netty.common.util.concurrent.FastThreadLocal;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

final class ChannelHandlerMask {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChannelHandlerMask.class);
	
	static final int MASK_EXCEPTION_CAUGHT = 1;
	static final int MASK_CHANNEL_REGISTERED = 1 << 1;
	static final int MASK_CHANNEL_UNREGISTERED = 1 << 2;
	static final int MASK_CHANNEL_ACTIVE = 1 << 3;
	static final int MASK_CHANNEL_INACTIVE = 1 << 4;
	static final int MASK_CHANNEL_READ = 1 << 5;
	static final int MASK_CHANNEL_READ_COMPLETE = 1 << 6;
	static final int MASK_USER_EVENT_TRIGGERED = 1 << 7;
	static final int MASK_CHANNEL_WRITABILITY_CHANGED = 1 << 8;
	static final int MASK_BIND = 1 << 9;
	static final int MASK_CONNECT = 1 << 10;
	static final int MASK_DISCONNECT = 1 << 11;
	static final int MASK_CLOSE = 1 << 12;
	static final int MASK_DEREGISTER = 1 << 13;
	static final int MASK_READ = 1 << 14;
	static final int MASK_WRITE = 1 << 15;
	static final int MASK_FLUSH = 1 << 16;
	
	static final int MASK_ONLY_INBOUND = MASK_CHANNEL_REGISTERED |
			MASK_CHANNEL_UNREGISTERED | MASK_CHANNEL_ACTIVE | MASK_CHANNEL_INACTIVE | MASK_CHANNEL_READ |
			MASK_CHANNEL_READ_COMPLETE | MASK_USER_EVENT_TRIGGERED | MASK_CHANNEL_WRITABILITY_CHANGED;
	private static final int MASK_ALL_INBOUND = MASK_EXCEPTION_CAUGHT | MASK_ONLY_INBOUND;
	static final int MASK_ONLY_OUTBOUND = MASK_BIND | MASK_CONNECT | MASK_DISCONNECT |
			MASK_CLOSE | MASK_DEREGISTER | MASK_READ| MASK_WRITE | MASK_FLUSH;
	private static final int MASK_ALL_OUTBOUND = MASK_EXCEPTION_CAUGHT | MASK_ONLY_OUTBOUND;
	
	private static final FastThreadLocal<Map<Class<? extends ChannelHandler>, Integer>> MASKS = 
			new FastThreadLocal<Map<Class<? extends ChannelHandler>, Integer>>() {
		@Override
		protected Map<Class<? extends ChannelHandler>, Integer> initialValue() {
			return new WeakHashMap<Class<? extends ChannelHandler>, Integer>(32);
		}
	};
	
	static int mask(Class<? extends ChannelHandler> clazz) {
		Map<Class<? extends ChannelHandler>, Integer> cache = MASKS.get();
		Integer mask = cache.get(clazz);
		if (mask == null) {
			mask = mask0(clazz);
			cache.put(clazz, mask);
		}
		return mask;
	}
	
	private static int mask0(Class<? extends ChannelHandler> handlerType) {
        int mask = MASK_EXCEPTION_CAUGHT;
        try {
            if (ChannelInboundHandler.class.isAssignableFrom(handlerType)) {
                mask |= MASK_ALL_INBOUND;

                if (isSkippable(handlerType, "channelRegistered", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_REGISTERED;
                }
                if (isSkippable(handlerType, "channelUnregistered", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_UNREGISTERED;
                }
                if (isSkippable(handlerType, "channelActive", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_ACTIVE;
                }
                if (isSkippable(handlerType, "channelInactive", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_INACTIVE;
                }
                if (isSkippable(handlerType, "channelRead", ChannelHandlerContext.class, Object.class)) {
                    mask &= ~MASK_CHANNEL_READ;
                }
                if (isSkippable(handlerType, "channelReadComplete", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_READ_COMPLETE;
                }
                if (isSkippable(handlerType, "channelWritabilityChanged", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_WRITABILITY_CHANGED;
                }
                if (isSkippable(handlerType, "userEventTriggered", ChannelHandlerContext.class, Object.class)) {
                    mask &= ~MASK_USER_EVENT_TRIGGERED;
                }
            }

            if (ChannelOutboundHandler.class.isAssignableFrom(handlerType)) {
                mask |= MASK_ALL_OUTBOUND;

                if (isSkippable(handlerType, "bind", ChannelHandlerContext.class,
                        SocketAddress.class, ChannelPromise.class)) {
                    mask &= ~MASK_BIND;
                }
                if (isSkippable(handlerType, "connect", ChannelHandlerContext.class, SocketAddress.class,
                        SocketAddress.class, ChannelPromise.class)) {
                    mask &= ~MASK_CONNECT;
                }
                if (isSkippable(handlerType, "disconnect", ChannelHandlerContext.class, ChannelPromise.class)) {
                    mask &= ~MASK_DISCONNECT;
                }
                if (isSkippable(handlerType, "close", ChannelHandlerContext.class, ChannelPromise.class)) {
                    mask &= ~MASK_CLOSE;
                }
                if (isSkippable(handlerType, "deregister", ChannelHandlerContext.class, ChannelPromise.class)) {
                    mask &= ~MASK_DEREGISTER;
                }
                if (isSkippable(handlerType, "read", ChannelHandlerContext.class)) {
                    mask &= ~MASK_READ;
                }
                if (isSkippable(handlerType, "write", ChannelHandlerContext.class,
                        Object.class, ChannelPromise.class)) {
                    mask &= ~MASK_WRITE;
                }
                if (isSkippable(handlerType, "flush", ChannelHandlerContext.class)) {
                    mask &= ~MASK_FLUSH;
                }
            }

            if (isSkippable(handlerType, "exceptionCaught", ChannelHandlerContext.class, Throwable.class)) {
                mask &= ~MASK_EXCEPTION_CAUGHT;
            }
        } catch (Exception e) {
            // Should never reach here.
            PlatformDependent.throwException(e);
        }

        return mask;
    }
	
	@SuppressWarnings({ "deprecation", "removal" })
	private static boolean isSkippable(
			final Class<?> handlerType, final String methodName, final Class<?>... paramTypes) throws Exception {
		return AccessController.doPrivileged(new PrivilegedExceptionAction<Boolean>() {
			@Override
			public Boolean run() throws Exception {
				Method m;
				try {
					m = handlerType.getMethod(methodName, paramTypes);
				} catch (NoSuchMethodException e) {
					if (logger.isDebugEnabled()) {
						logger.debug(
							"Class {} missing method {}, assume we can not skip execution", handlerType, methodName, e);
					}
					return false;
				}
				return m.isAnnotationPresent(Skip.class);
			}
		});
	}
	
	private ChannelHandlerMask() {}
	
	@Target(ElementType.METHOD)
	@Retention(RetentionPolicy.RUNTIME)
	@interface Skip {
		
	}
}
