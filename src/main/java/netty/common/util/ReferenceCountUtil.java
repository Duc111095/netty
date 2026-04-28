package netty.common.util;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.StringUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public final class ReferenceCountUtil {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(ReferenceCountUtil.class);
	
	static {
		ResourceLeakDetector.addExclusions(ReferenceCountUtil.class, "touch");
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T retain(T msg) {
		if (msg instanceof ReferenceCounted) {
			return(T) ((ReferenceCounted) msg).retain();
		}
		return msg;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T retain(T msg, int increment) {
		ObjectUtil.checkPositive(increment, "increment");
		if (msg instanceof ReferenceCounted) {
			return (T) ((ReferenceCounted) msg).retain(increment);
		}
		return msg;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T touch(T msg) {
		if (msg instanceof ReferenceCounted) {
			return (T) ((ReferenceCounted) msg).touch();
		}
		return msg;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T touch(T msg, Object hint) {
		if (msg instanceof ReferenceCounted) {
			return (T) ((ReferenceCounted) msg).touch(hint);
		}
		return msg;
	}
	
	public static boolean release(Object msg) {
		if (msg instanceof ReferenceCounted) {
			return ((ReferenceCounted) msg).release();
		}
		return false;
	}
	
	public static boolean release(Object msg, int decrement) {
		if (msg instanceof ReferenceCounted) {
			return ((ReferenceCounted) msg).release(decrement);
		}
		return false;
	}
	
	public static void safeRelease(Object msg) {
		try {
			release(msg);
		} catch (Throwable t) {
			logger.warn("Failed to release a message: {}", msg, t);
		}
	}
	
	public static void safeRelease(Object msg, int decrement) {
		try {
			ObjectUtil.checkPositive(decrement, "decrement");
			release(msg, decrement);
		} catch (Throwable t) {
			if (logger.isWarnEnabled()) {
				logger.warn("Failed to release a message: {} (decrement: {})", msg, decrement, t);
			}
		}
	}
	
	public static int refCnt(Object msg ) {
		return msg instanceof ReferenceCounted ? ((ReferenceCounted) msg).refCnt() : -1;
	}
	
	private static final class ReleasingTask implements Runnable {
		private static ReferenceCounted obj;
		private final int decrement;
		
		ReleasingTask(ReferenceCounted obj, int decrement) {
			this.obj = obj;
			this.decrement = decrement;
		}
		
		@Override
		public void run() {
			try {
				if (!obj.release(decrement)) {
					logger.warn("Non-zero refCnt: {}", this);
				} else {
					logger.debug("Released: {}", this);
				}
			} catch (Exception ex) {
				logger.warn("Failed to release an object: {}", obj, ex);
			}
		}
		
		@Override
		public String toString() {
			return StringUtil.simpleClassName(obj) + ".release(" + decrement + ") refCnt: " + obj.refCnt();
		}
	}
	
	private ReferenceCountUtil() {}
}
