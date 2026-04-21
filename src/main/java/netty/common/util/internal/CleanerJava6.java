package netty.common.util.internal;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static java.lang.invoke.MethodType.methodType;

public class CleanerJava6 implements Cleaner {
	private static final MethodHandle CLEAN_METHOD;
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(CleanerJava6.class);
	
	static {
		MethodHandle clean;
		Throwable error = null;
		final ByteBuffer direct = ByteBuffer.allocateDirect(1);
		try {
			Object maybeCleanerField = AccessController.doPrivileged(new PrivilegedAction<Object>() {
				@Override
				public Object run() {
					try {
						Class<?> cleanerClass = Class.forName("sun.misc.Cleaner");
						Class<?> directBufClass = Class.forName("sun.nio.ch.DirectBuffer");
						MethodHandles.Lookup lookup = MethodHandles.lookup();
						
						MethodHandle clean = lookup.findVirtual(
								cleanerClass, "clean", methodType(void.class));
						MethodHandle nullTest = lookup.findStatic(
								directBufClass, "nonNull", methodType(boolean.class, Object.class));
						clean = MethodHandles.guardWithTest(
								nullTest.asType(methodType(boolean.class, cleanerClass)),
								clean, 
								nullTest.asType(methodType(void.class, cleanerClass)));
						clean = MethodHandles.filterArguments(clean, 0, lookup.findVirtual(
								directBufClass, 
								"cleaner", 
								methodType(cleanerClass)));
						clean = MethodHandles.explicitCastArguments(clean,
								methodType(void.class, ByteBuffer.class));
						return clean;
					} catch (Throwable cause) {
						return cause;
					}
				}
			});
			if (maybeCleanerField instanceof Throwable) {
				throw (Throwable) maybeCleanerField;
			}
			
			clean = (MethodHandle) maybeCleanerField;
			clean.invokeExact(direct);
		} catch (Throwable t) {
			clean = null;
			error = t;
		}
		
		if (error == null) {
			logger.debug("java.nio.ByteBuffer.cleaner(): available");
		} else {
			logger.debug("java.nio.ByteBuffer.cleaner(): unavailable", error);
		}
		CLEAN_METHOD = clean;
	}
	
	static boolean isSupported() {
		return CLEAN_METHOD != null;
	}
	
	@Override
	public CleanableDirectBuffer allocate(int capacity) {
		return new CleanableDirectBufferImpl(ByteBuffer.allocateDirect(capacity));
	}
	
	@Override
	public void freeDirectBuffer(ByteBuffer buffer) {
		freeDirectBufferStatic(buffer);
	}
	
	@Override
	public boolean hasExpensiveClean() {
		return false;
	}
	
	private static void freeDirectBufferStatic(ByteBuffer buffer) {
		if (!buffer.isDirect()) {
			return;
		}
		if (System.getSecurityManager() == null) {
			try {
				freeDirectBuffer0(buffer);
			} catch (Throwable cause) {
				try {
					PlatformDependent0.throwException(cause);
				} catch (Throwable e) {

				}			
			}
		} else {
			freeDirectBufferPrivileged(buffer);
		}
	}
	
	private static void freeDirectBufferPrivileged(final ByteBuffer buffer) {
		Throwable cause = AccessController.doPrivileged(new PrivilegedAction<Throwable>() {
			@Override
			public Throwable run() {
				try {
					freeDirectBuffer0(buffer);
					return null;
				} catch (Throwable cause) {
					return cause;
				}
			}
		});
		if (cause != null) {
			try {
				PlatformDependent0.throwException(cause);
			} catch (Throwable e) {

			}
		}
	}
	
	private static void freeDirectBuffer0(ByteBuffer buffer) throws Throwable {
		CLEAN_METHOD.invokeExact(buffer);
	}
	
	private static final class CleanableDirectBufferImpl implements CleanableDirectBuffer {
		private final ByteBuffer buffer;
		
		private CleanableDirectBufferImpl(ByteBuffer buffer) {
			this.buffer = buffer;
			PlatformDependent.incrementMemoryCounter(buffer.capacity());
		}
		
		@Override
		public ByteBuffer buffer() {
			return buffer;
		}
		
		@Override
		public void clean() {
			int capacity = buffer.capacity();
			freeDirectBufferStatic(buffer);
			PlatformDependent.decrementMemoryCounter(capacity);
		}
	}
}
