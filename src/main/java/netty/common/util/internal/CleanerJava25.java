package netty.common.util.internal;

import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

final class CleanerJava25 implements Cleaner {
	private static final InternalLogger logger;
	
	private static final MethodHandle INVOKE_ALLOCATOR;
	
	static {
		boolean suitableJavaVersion;
		if (System.getProperty("org.graalvm.nativeimage.imagecode") != null) {
			String v = System.getProperty("java.specification.version");
			try {
				suitableJavaVersion = Integer.parseInt(v) >= 25;
			} catch (NumberFormatException e) {
				suitableJavaVersion = false;
			}
			
			logger = null;
		} else {
			suitableJavaVersion = PlatformDependent0.javaVersion() >= 25;
			logger = InternalLoggerFactory.getInstance("CleanerJava25.class");
		}
		
		MethodHandle method;
		Throwable error;
		if (suitableJavaVersion) {
			try {
				Class<?> arenaCls = Class.forName("java.lang.foreign.Arena");
				Class<?> memsegCls = Class.forName("java.lang.foreign.MemorySegment");
				Class<CleanableDirectBufferImpl> bufCls = CleanableDirectBufferImpl.class;
				MethodHandles.Lookup lookup = MethodHandles.lookup();
				
				MethodHandle ofShared = lookup.findStatic(arenaCls, "ofShared", methodType(arenaCls));
				Object shared = ofShared.invoke();
				((AutoCloseable) shared).close();
				
				MethodHandle allocate = lookup.findVirtual(arenaCls, "allocate", methodType(memsegCls, long.class));
				MethodHandle asByteBuffer = lookup.findVirtual(memsegCls, "asByteBuffer", methodType(ByteBuffer.class));
				MethodHandle address = lookup.findVirtual(memsegCls, "address", methodType(long.class));
				MethodHandle bufClsCtor = lookup.findConstructor(bufCls, 
						methodType(void.class, AutoCloseable.class, ByteBuffer.class, long.class));
				MethodHandle allocateInt = MethodHandles.explicitCastArguments(allocate, 
						methodType(memsegCls, arenaCls, int.class));
				MethodHandle ctorArenaMemsegMemseg = MethodHandles.explicitCastArguments(MethodHandles.filterArguments(bufClsCtor, 1, asByteBuffer, address),
						methodType(bufCls, arenaCls, memsegCls, memsegCls));
				MethodHandle ctorArenaMemsegNull = MethodHandles.permuteArguments(ctorArenaMemsegMemseg, 
						methodType(bufCls, arenaCls, memsegCls, memsegCls), 0, 1, 1);
				MethodHandle ctorArenaMemseg = MethodHandles.insertArguments(ctorArenaMemsegNull, 2, new Object[] {null});
				MethodHandle ctorArenaArenaInt = MethodHandles.collectArguments(ctorArenaMemseg, 1, allocateInt);
				MethodHandle ctorArenaNullInt = MethodHandles.permuteArguments(ctorArenaArenaInt, 
						methodType(bufCls, arenaCls, arenaCls, int.class), 0, 0, 2);
				
				MethodHandle ctorArenaInt = MethodHandles.insertArguments(ctorArenaNullInt, 1, new Object[] {null});
				method = MethodHandles.foldArguments(ctorArenaInt, ofShared);
				error = null;
			} catch (Throwable throwable) {
				error = throwable;
				method = null;
			}
		} else {
			method = null;
			error = new UnsupportedOperationException("java.lang.foreign.MemorySegment unavailable");
		}
		if (logger != null) {
			if (error == null) {
				logger.debug("java.nio.ByteBuffer.cleaner(): available");
			} else {
				logger.debug("java.nio.ByteBuffer.cleaner(): unavailable", error);
			}
		}
		INVOKE_ALLOCATOR = method;
	}
		
	static boolean isSupported() {			
		return INVOKE_ALLOCATOR != null;
	}
		
	@SuppressWarnings("OverlyStrongTypeCast")
	@Override
	public CleanableDirectBuffer allocate(int capacity) {
		PlatformDependent.incrementMemoryCounter(capacity);
		try {
			return (CleanableDirectBufferImpl) INVOKE_ALLOCATOR.invokeExact(capacity);
		} catch (RuntimeException e) {
			PlatformDependent.decrementMemoryCounter(capacity);
			throw e;
		} catch (Throwable e) {
			PlatformDependent.decrementMemoryCounter(capacity);
			throw new IllegalStateException("Unexpected allocation exception", e);
		}
	}
	
	@Override
	public void freeDirectBuffer(ByteBuffer buffer) {
		throw new UnsupportedOperationException("Cannot clean arbitrary ByteBuffer instances");
	}
	
	@Override
	public boolean hasExpensiveClean() {
		return true;
	}
	
	private static final class CleanableDirectBufferImpl implements CleanableDirectBuffer {
		private final AutoCloseable closeable;
		private final ByteBuffer buffer;
		private final long memoryAddress;
		
		CleanableDirectBufferImpl(AutoCloseable closeable, ByteBuffer buffer, long memoryAddress) {
			this.closeable = closeable;
			this.buffer = buffer;
			this.memoryAddress = memoryAddress;
		}
		
		@Override
		public ByteBuffer buffer() {
			return buffer;
		}
		
		@Override
		public void clean() {
			int capacity = buffer.capacity();
			try {
				closeable.close();
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				throw new IllegalStateException("unexpected close exception", e);
			} finally {
				PlatformDependent.decrementMemoryCounter(capacity);
			}
		}
		
		@Override
		public boolean hasMemoryAddress() {
			return true;
		}
		
		@Override
		public long memoryAddress() {
			return memoryAddress;
		}
	}
}
