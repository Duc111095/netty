package netty.common.util.internal;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static java.lang.invoke.MethodType.methodType;

import sun.misc.Unsafe;

final class CleanerJava9 implements Cleaner {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(CleanerJava9.class);
	private static final MethodHandle INVOKE_CLEANER;
	
	static {
		final MethodHandle method;
		final Throwable error;
		if (PlatformDependent0.hasUnsafe()) {
			final ByteBuffer buffer = ByteBuffer.allocateDirect(1);
			Object maybeInvokeMethod = AccessController.doPrivileged(new PrivilegedAction<Object>(){
				@Override
				public Object run() {
					try {
						Class<? extends Unsafe> unsafeClass = PlatformDependent0.UNSAFE.getClass();
						MethodHandles.Lookup lookup = MethodHandles.lookup();
						MethodHandle invokeCleaner = lookup.findVirtual(
								unsafeClass, "invokeCleaner", methodType(void.class, ByteBuffer.class));
						invokeCleaner = invokeCleaner.bindTo(PlatformDependent0.UNSAFE);
						invokeCleaner.invokeExact(buffer);
						return invokeCleaner;
					} catch (Throwable e) {
						return e;
					}
				}
			});
			
			if (maybeInvokeMethod instanceof Throwable) {
				method = null;
				error = (Throwable) maybeInvokeMethod;
			} else {
				method = (MethodHandle) maybeInvokeMethod;
				error = null;
			}
		} else {
			method = null;
			error = new UnsupportedOperationException("sun.misc.Unsafe unavailable");
		}
		if (error == null) {
			logger.debug("java.nio.ByteBuffer.cleaner(): available" );
		} else {
			logger.debug("java.nio.ByteBuffer.cleaner(): unavailable", error);
		}
		INVOKE_CLEANER = method;
	}
	
	static boolean isSupported() {
		return INVOKE_CLEANER != null;
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
		if (System.getSecurityManager() == null) {
			try {
				INVOKE_CLEANER.invokeExact(buffer);
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
		Throwable error = AccessController.doPrivileged(new PrivilegedAction<Throwable>() {
			@Override
			public Throwable run() {
				try {
					INVOKE_CLEANER.invokeExact(buffer);
				} catch (Throwable e) {
					return e;
				}
				return null;
			}
		});
		if (error != null) {
			try {
				PlatformDependent0.throwException(error);
			} catch (Throwable e) {
			}
		}
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
