package netty.common.util.internal;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.MpscChunkedArrayQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.jctools.queues.SpscLinkedQueue;
import org.jctools.queues.atomic.MpmcAtomicArrayQueue;
import org.jctools.queues.atomic.MpscAtomicArrayQueue;
import org.jctools.queues.atomic.MpscChunkedAtomicArrayQueue;
import org.jctools.queues.atomic.MpscUnboundedAtomicArrayQueue;
import org.jctools.queues.atomic.SpscLinkedAtomicQueue;
import org.jctools.queues.atomic.unpadded.MpscAtomicUnpaddedArrayQueue;
import org.jctools.queues.unpadded.MpscUnpaddedArrayQueue;
import org.jctools.util.Pow2;
import org.jctools.util.UnsafeAccess;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;
import netty.jfr_stub.jfr.FlightRecorder;

import static netty.common.util.internal.PlatformDependent0.HASH_CODE_ASCII_SEED;
import static netty.common.util.internal.PlatformDependent0.HASH_CODE_C1;
import static netty.common.util.internal.PlatformDependent0.HASH_CODE_C2;
import static netty.common.util.internal.PlatformDependent0.hashCodeAsciiSanitize;
import static netty.common.util.internal.PlatformDependent0.unalignedAccess;
import static java.lang.invoke.MethodType.methodType;


public final class PlatformDependent {
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(PlatformDependent.class);
	
	private static Pattern MAX_DIRECT_MEMORY_SIZE_ARG_PATTERN;
	private static final boolean MAYBE_SUPER_USER;
	
	private static final boolean CAN_ENABLE_TCP_NODELAY_BY_DEFAULT = !isAndroid();
	
	private static final Throwable UNSAFE_UNAVAILABILITY_CAUSE = unsafeUnavailabilityCause0();
	private static final boolean DIRECT_BUFFER_PREFERRED;
	private static final boolean EXPLICIT_NO_PREFER_DIRECT;
	private static final long MAX_DIRECT_MEMORY = estimateMaxDirectMemory();
	
	private static final int MPSC_CHUNK_SIZE = 1024;
	private static final int MIN_MAX_MPSC_CAPACITY = MPSC_CHUNK_SIZE * 2;
	private static final int MAX_ALLOWED_MPSC_CAPACITY = Pow2.MAX_POW2;
	
	private static final long BYTE_ARRAY_BASE_OFFSET = byteArrayBaseOffset0();
	
	private static final File TMPDIR = tmpDir0();
	private static final int BIT_MODE = bitMode0();
	private static final String NORMALIZED_ARCH = normalizeArch(SystemPropertyUtil.get("os.arch", ""));
	private static final String NORMALIZED_OS = normalizeOs(SystemPropertyUtil.get("os.name", ""));
	
	private static final Set<String> LINUX_OS_CLASSIFIERS;
	
	private static final boolean IS_WINDOWS = isWindows0();
	private static final boolean IS_OSX = isOsx0();
	private static final boolean IS_J9_JVM = isJ9Jvm0();
	private static final boolean IS_IVKVM_DOT_NET = isIkvmDotNet0();
	
	private static final int ADDRESS_SIZE = addressSize0();
	private static final AtomicLong DIRECT_MEMORY_COUNTER;
	private static final long DIRECT_MEMORY_LIMIT;
	private static final Cleaner CLEANER;
	private static final Cleaner LEGACY_CLEANER;
	private static final boolean HAS_ALLOCATE_UNINIT_ARRAY;
	private static final String LINUX_ID_PREFIX = "ID=";
	private static final String LINUX_ID_LIKE_PREFIX = "ID_LIKE=";
	public static final boolean BIG_ENDIAN_NATIVE_ORDER = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
	private static final boolean IGNORE_EXPENSIVE_CLEAN = 
			SystemPropertyUtil.getBoolean("io.netty.ignoreExpensiveClean", false);
	
	private static final boolean JFR;
	private static final boolean VAR_HANDLE;
	
	private static final Cleaner NOOP = new Cleaner() {
		@Override
		public CleanableDirectBuffer allocate(final int capacity) {
			return new CleanableDirectBuffer() {
				private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(capacity);
				
				@Override
				public ByteBuffer buffer() {
					return byteBuffer;
				}
				
				@Override
				public void clean() {
					
				}
				
				@Override
				public boolean hasMemoryAddress() {
					return hasDirectByteBufferAddress(byteBuffer);
				}
				
				@Override
				public long memoryAddress() {
					return directBufferAddress(byteBuffer);
				}
			};
		}

		@Override
		public void freeDirectBuffer(ByteBuffer buffer) {
			
		}

		@Override
		public boolean hasExpensiveClean() {
			return false;
		}
	};
	
	static {
		long maxDirectMemory = SystemPropertyUtil.getLong("io.netty.maxDirectMemory", -1);
		
		if (maxDirectMemory == 0) {
			DIRECT_MEMORY_COUNTER = null;
		} else if (maxDirectMemory < 0) {
			maxDirectMemory = MAX_DIRECT_MEMORY;
			if (maxDirectMemory <= 0) {
				DIRECT_MEMORY_COUNTER = null;
			} else {
				DIRECT_MEMORY_COUNTER = new AtomicLong();
			}
		} else {
			DIRECT_MEMORY_COUNTER = new AtomicLong();
		}
		logger.debug("-Dio.netty.maxDirectMemory: {} bytes", maxDirectMemory);
		DIRECT_MEMORY_LIMIT = maxDirectMemory >= 1 ? maxDirectMemory : MAX_DIRECT_MEMORY;
		HAS_ALLOCATE_UNINIT_ARRAY = javaVersion() >= 9 && PlatformDependent0.hasAllocateArrayMethod();
		
		MAYBE_SUPER_USER = maybeSuperUser0();
		
		if (!isAndroid()) {
			if (javaVersion() >= 9) {
				if (CleanerJava9.isSupported()) {
					LEGACY_CLEANER = new CleanerJava9();
				} else if (CleanerJava24Linker.isSupported()) {
					LEGACY_CLEANER = new CleanerJava24Linker();
				} else if (CleanerJava25.isSupported()) {
					LEGACY_CLEANER = new CleanerJava25();
				} else {
					LEGACY_CLEANER = NOOP;
				}
			}
			else {
				LEGACY_CLEANER = CleanerJava6.isSupported() ? new CleanerJava6() : NOOP;
			}
		} else {
			LEGACY_CLEANER = NOOP;
		}
		if (maxDirectMemory != 0 && hasUnsafe() && PlatformDependent0.hasDirectBufferNoCleanerConstructor()) {
			CLEANER = new DirectCleaner();
		} else {
			CLEANER = LEGACY_CLEANER;
		}
		
		EXPLICIT_NO_PREFER_DIRECT = SystemPropertyUtil.getBoolean("io.netty.noPreferDirect", false);
		DIRECT_BUFFER_PREFERRED = CLEANER != NOOP
				&& !EXPLICIT_NO_PREFER_DIRECT;
		if (logger.isDebugEnabled()) {
			logger.debug("-Dio.netty.noPreferDirect: {}", EXPLICIT_NO_PREFER_DIRECT);
		}
		
		logger.debug("-Dio.netty.ignoreExpensiveClean: {}", IGNORE_EXPENSIVE_CLEAN);
		
		if (CLEANER == NOOP && !PlatformDependent0.isExplicitNoUnsafe()) {
			logger.info(
					"Your platform does not provide complete low-level API for accessing direct buffers reliably. " +
					"Unless explicitly requested, heap buffer will always be preferred to avoid potential system " +
					"instability.");
		}
		
		final Set<String> availableClassifiers = new LinkedHashSet<>();
		
		if (!addPropertyOsClassifiers(availableClassifiers) ) {
			addFilesystemOsClassifiers(availableClassifiers);
		}
		LINUX_OS_CLASSIFIERS = Collections.unmodifiableSet(availableClassifiers);
		
		boolean jfrAvailable;
		Throwable jfrFailure = null;
		try {
			jfrAvailable = FlightRecorder.isAvailable();
		} catch (Throwable t) {
			jfrAvailable = false;
			jfrFailure = t;
		}
		JFR = SystemPropertyUtil.getBoolean("io.netty.jfr.enabled", jfrAvailable);
		if (logger.isTraceEnabled() && jfrFailure != null) {
			logger.debug("-Dio.netty.jfr.enabled: {}", JFR, jfrFailure);
		} else if (logger.isDebugEnabled()) {
			logger.debug("-Dio.netty.jfr.enabled: {}", JFR);
		}
		VAR_HANDLE = initializeVarHandle();
	}
	
	private static boolean initializeVarHandle() {
		if (javaVersion() < 9 ||
				PlatformDependent0.isNativeImage()) {
			return false;
		}
		boolean varHandleAvailable = false;
		Throwable varHandleFailure;
		try {
			VarHandle.storeStoreFence();
			varHandleAvailable = VarHandleFactory.isSupported();
			varHandleFailure = VarHandleFactory.unavailableCause();
		} catch (Throwable t) {
			varHandleFailure = t;
		}
		if (varHandleFailure != null) {
			logger.debug("java.lang.invoke.VarHandle: unavailable, reason: {}", varHandleFailure.toString());
		} else {
			logger.debug("java.lang.invoke.VarHandle: available");
		}
		boolean varHandleEnabled = varHandleAvailable &&
				SystemPropertyUtil.getBoolean("io.netty.varHandle.enabled", varHandleAvailable);
		if (logger.isTraceEnabled() && varHandleFailure != null) {
			logger.debug("-Dio.netty.varHandle.enabled: {}", varHandleEnabled, varHandleFailure);
		} else if (logger.isDebugEnabled()) {
			logger.debug("-Dio.netty.varHandle.enabled: {}", varHandleEnabled);
		}
		return varHandleEnabled;
	}
	
	static void addFilesystemOsClassifiers(final Set<String> availableClassifiers) {
		if (processOsReleaseFile("/etc/os-release", availableClassifiers)) {
			return;
		}
		processOsReleaseFile("/usr/lib/os-release", availableClassifiers);
	}
	
	private static boolean processOsReleaseFile(String osReleaseFileName, Set<String> availableClassifiers) {
		Path file = Paths.get(osReleaseFileName);
		return AccessController.doPrivileged((PrivilegedAction<Boolean>)() -> {
			try {
				if (Files.exists(file)) {
					try (BufferedReader reader = new BufferedReader(new InputStreamReader(
							new BoundedInputStream(Files.newInputStream(file)), StandardCharsets.UTF_8))) {
						String line;
						while ((line = reader.readLine()) != null) {
							if (line.startsWith(LINUX_ID_PREFIX)) {
								String id = normalizeOsReleaseVariableValue(
										line.substring(LINUX_ID_PREFIX.length()));
								addClassifier(availableClassifiers, id);
							} else if (line.startsWith(LINUX_ID_LIKE_PREFIX)) {
								line = normalizeOsReleaseVariableValue(
										line.substring(LINUX_ID_LIKE_PREFIX.length()));
								addClassifier(availableClassifiers, line.split(" "));
							}
						}
					} catch (SecurityException e) {
						logger.debug("Unable to read {}", osReleaseFileName, e);
					} catch (IOException e) {
						logger.debug("Error while reading content of {}", osReleaseFileName, e);
					}
					return true;
				}
			} catch (SecurityException e) {
				logger.debug("Unable to check if {} exists", osReleaseFileName, e);
			}
			return false;
		});
	}
	
	static boolean addPropertyOsClassifiers(Set<String> availableClassifiers) {
		String osClassifiersPropertyName = "io.netty.osClassifiers";
		String osClassifiers = SystemPropertyUtil.get(osClassifiersPropertyName);
		if (osClassifiers == null) {
			return false;
		}
		if (osClassifiers.isEmpty()) {
			return true;
		}
		String[] classifiers = osClassifiers.split(",");
		if (classifiers.length == 0) {
			throw new IllegalArgumentException(
					osClassifiersPropertyName + " property is not empty, but contains no classifiers: " + osClassifiers);
		}
		if (classifiers.length > 2) {
			throw new IllegalArgumentException(
					osClassifiersPropertyName + " property contains more than 2 classifiers: " + osClassifiers);
		}
		for (String classifier : classifiers) {
			addClassifier(availableClassifiers, classifier);
		}
		return true;
	}
	
	public static long byteArrayBaseOffset() {
		return BYTE_ARRAY_BASE_OFFSET;
	}
	
	public static boolean hasDirectBufferNoCleanerConstructor() {
		return PlatformDependent0.hasDirectBufferNoCleanerConstructor();
	}
	
	public static byte[] allocateUninitializedArray(int size) {
		return HAS_ALLOCATE_UNINIT_ARRAY ? PlatformDependent0.allocateUninitializedArray(size) : new byte[size];
	}
	
	public static boolean isAndroid() {
		return PlatformDependent0.isAndroid();
	}
	
	public static boolean isWindows() {
		return IS_WINDOWS;
	}
	
	public static boolean isOsx() {
		return IS_OSX;
	}
	
	public static boolean maybeSuperUser() {
		return MAYBE_SUPER_USER;
	}
	
	public static int javaVersion() {
		return PlatformDependent0.javaVersion();
	}
	
	public static boolean isVirtualThread(Thread thread) {
		return PlatformDependent0.isVirtualThread(thread);
	}
	
	public static boolean canEnableTcpNoDelayByDefault() {
		return CAN_ENABLE_TCP_NODELAY_BY_DEFAULT;
	}
	
	public static boolean hasUnsafe() {
		return UNSAFE_UNAVAILABILITY_CAUSE == null;
	}
	
	public static Throwable getUnsafeUnavailabilityCause() {
		return UNSAFE_UNAVAILABILITY_CAUSE;
	}
	
	public static boolean isUnaligned() {
		return PlatformDependent0.isUnaligned();
	}
	
	public static boolean directBufferPreferred() {
		return DIRECT_BUFFER_PREFERRED;
	}
	
	public static boolean isExplicitNoPreferDirect() {
		return EXPLICIT_NO_PREFER_DIRECT;
	}
	
	public static boolean canReliabilyFreeDirectBuffer() {
		return CLEANER != NOOP;
	}
	
	public static long maxDirectMemory() {
		return DIRECT_MEMORY_LIMIT;
	}
	
	public static long usedDirectMemory() {
		return DIRECT_MEMORY_COUNTER != null ? DIRECT_MEMORY_COUNTER.get() : -1;
	}
	
	public static File tmpdir() {
		return TMPDIR;
	}
	
	public static int bitMode() {
		return BIT_MODE;
	}
	
	public static int addressSize() {
		return ADDRESS_SIZE;
	}
	
	public static long allocateMemory(long size) {
		return PlatformDependent0.allocateMemory(size);
	}
	
	public static void freeMemory(long address) {
		PlatformDependent0.freeMemory(address);
	}
	
	public static long reallocateMemory(long address, long newSize) {
		return PlatformDependent0.reallocateMemory(address, newSize);
	}
	
	public static void throwException(Throwable t) {
		try {
			PlatformDependent0.throwException(t);
		} catch (Throwable e) {
		}
	}
	
	public static <K, V> ConcurrentMap<K, V> newConcurrentHashMap() {
		return new ConcurrentHashMap<>();
	}
	
	public static LongCounter newLongCounter() {
		return new LongAddedCounter();
	}
	
	public static <K, V> ConcurrentMap<K, V> newConcurrentHashMap(int initialCapacity) {
		return new ConcurrentHashMap<>(initialCapacity);
	}
	
	public static <K, V> ConcurrentMap<K, V> newConcurrentHashMap(int initialCapacity, float loadFactor) {
		return new ConcurrentHashMap<>(initialCapacity, loadFactor);
	}
	
	public static <K, V> ConcurrentMap<K, V> newConcurrentHashMap(
			int initialCapacity, float loadFactor, int concurrencyLevel) {
		return new ConcurrentHashMap<>(initialCapacity, loadFactor, concurrencyLevel);
	}
	
	public static <K, V> ConcurrentMap<K, V> newConcurrentHashMap(
			Map<? extends K, ? extends V> map) {
		return new ConcurrentHashMap<>(map);
	}
	
	public static CleanableDirectBuffer allocateDirrect(int capacity) {
		return allocateDirect(capacity, false);
	}
	
	public static CleanableDirectBuffer allocateDirect(int capacity, boolean permitExpensiveClean) {
		if (!IGNORE_EXPENSIVE_CLEAN && !permitExpensiveClean && CLEANER.hasExpensiveClean()) {
			return NOOP.allocate(capacity);
		}
		return CLEANER.allocate(capacity);
	}
	
	public static CleanableDirectBuffer reallocateDirect(CleanableDirectBuffer buffer, int newCapacity) {
		return CLEANER.reallocate(buffer, newCapacity);
	}
	
	public static void freeDirectBuffer(ByteBuffer buffer) {
		LEGACY_CLEANER.freeDirectBuffer(buffer);
	}
	
	public static boolean hasDirectByteBufferAddress(ByteBuffer buffer) {
		return PlatformDependent0.hasDirectByteBufferAddress(buffer);
	}
	
	public static long directBufferAddress(ByteBuffer buffer) {
		return PlatformDependent0.directBufferAddress(buffer);
	}
	
	public static ByteBuffer directBuffer(long memoryAddress, int size) {
		if (PlatformDependent0.hasDirectBufferNoCleanerConstructor()) {
			return PlatformDependent0.newDirectBuffer(memoryAddress, size);
		}
		throw new UnsupportedOperationException(
				"sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available");
	}
	
	public static boolean hasVarHandle() {
		return VAR_HANDLE;
	}
	
	public static boolean useVarHandleForMultiByteAccess() {
		return !isUnaligned() && VAR_HANDLE;
	}
	
	public static boolean canUnalignedAccess() {
		return isUnaligned() || VAR_HANDLE;
	}
	
	public static VarHandle findVarHandleOfIntField(MethodHandles.Lookup lookup, Class<?> type, String fieldName) {
		if (VAR_HANDLE) {
			return VarHandleFactory.privateFindVarHandle(lookup, type, fieldName, int.class);
		}
		return null;
	}
	
	public static VarHandle intBeArrayView() {
		if (VAR_HANDLE) {
			return VarHandleFactory.intBeArrayView();
		}
		return null;
	}
	
	public static VarHandle intLeArrayView() {
		if (VAR_HANDLE) {
			return VarHandleFactory.intLeArrayView();
		}
		return null;
	}
	
	public static VarHandle longBeArrayView() {
		if (VAR_HANDLE) {
			return VarHandleFactory.longBeArrayView();
		}
		return null;
	}
	
	public static VarHandle longLeArrayView() {
		if (VAR_HANDLE) {
			return VarHandleFactory.longLeArrayView();
		}
		return null;
	}
	
	public static VarHandle shortBeArrayView() {
        if (VAR_HANDLE) {
            return VarHandleFactory.shortBeArrayView();
        }
        return null;
    }

    public static VarHandle shortLeArrayView() {
        if (VAR_HANDLE) {
            return VarHandleFactory.shortLeArrayView();
        }
        return null;
    }

    public static VarHandle longBeByteBufferView() {
        if (VAR_HANDLE) {
            return VarHandleFactory.longBeByteBufferView();
        }
        return null;
    }

    public static VarHandle longLeByteBufferView() {
        if (VAR_HANDLE) {
            return VarHandleFactory.longLeByteBufferView();
        }
        return null;
    }

    public static VarHandle intBeByteBufferView() {
        if (VAR_HANDLE) {
            return VarHandleFactory.intBeByteBufferView();
        }
        return null;
    }

    public static VarHandle intLeByteBufferView() {
        if (VAR_HANDLE) {
            return VarHandleFactory.intLeByteBufferView();
        }
        return null;
    }

    public static VarHandle shortBeByteBufferView() {
        if (VAR_HANDLE) {
            return VarHandleFactory.shortBeByteBufferView();
        }
        return null;
    }

    public static VarHandle shortLeByteBufferView() {
        if (VAR_HANDLE) {
            return VarHandleFactory.shortLeByteBufferView();
        }
        return null;
    }
    
    public static Object getObject(Object object, long fieldOffset) {
    	return PlatformDependent0.getObject(object, fieldOffset);
    }
    
    public static int getVolatileInt(Object object, long fieldOffset) {
    	return PlatformDependent0.getIntVolatile(object, fieldOffset);
    }
    
    public static int getInt(Object object, long fieldOffset) {
    	return PlatformDependent0.getInt(object, fieldOffset);
    }
    
    public static void putOrderedInt(Object object, long fieldOffset, int value) {
    	PlatformDependent0.putOrderedInt(object, fieldOffset, value);
    }
    
    public static int getAndAddInt(Object object, long fieldOffset, int delta) {
    	return PlatformDependent0.getAndAddInt(object, fieldOffset, delta);
    }
    
    public static boolean compareAndSwapInt(Object object, long fieldOffset, int expected, int value) {
    	return PlatformDependent0.compareAndSwapInt(object, fieldOffset, expected, value);
    }
    
    static void safeConstructPutInt(Object object, long fieldOffset, int value) {
    	PlatformDependent0.safeConstructPutInt(object, fieldOffset, value);
    }
    
    public static byte getByte(long address) {
    	return PlatformDependent0.getByte(address);
    }
    
    public static short getShort(long address) {
    	return PlatformDependent0.getShort(address);
    }
    
    public static int getInt(long address) {
    	return PlatformDependent0.getInt(address);
    }
    
    public static long getLong(long address) {
    	return PlatformDependent0.getLong(address);
    }
    
    public static byte getByte(byte[] data, int index) {
    	return hasUnsafe() ? PlatformDependent0.getByte(data, index) : data[index];
    }
    
    public static byte getByte(byte[] data, long index) {
    	return hasUnsafe() ? PlatformDependent0.getByte(data, index) : data[toIntExact(index)];
    }
    
    public static short getShort(byte[] data, int index) {
    	return hasUnsafe() ? PlatformDependent0.getShort(data, index) : data[index];
    }
    
    public static int getInt(byte[] data, int index) {
    	return hasUnsafe() ? PlatformDependent0.getInt(data, index) : data[index];
    }
    
    public static int getInt(int[] data, long index) {
    	return hasUnsafe() ? PlatformDependent0.getInt(data, index) : data[toIntExact(index)];
    }
    
    public static long getLong(byte[] data, int index) {
    	return hasUnsafe() ? PlatformDependent0.getLong(data, index) : data[index];
    }
    
    public static long getLong(long[] data, long index) {
    	return hasUnsafe() ? PlatformDependent0.getLong(data, index) : data[toIntExact(index)];
    }
    
    private static int toIntExact(long value) {
    	return Math.toIntExact(value);
    }
    
    private static long getLongSafe(byte[] bytes, int offset) {
    	if (BIG_ENDIAN_NATIVE_ORDER) {
    		return (long) bytes[offset] << 56 |
    				((long) bytes[offset + 1] & 0xff) << 48 |
    				((long) bytes[offset + 2] & 0xff) << 40 |
    				((long) bytes[offset + 3] & 0xff) << 32 |
    				((long) bytes[offset + 4] & 0xff) << 24 |
    				((long) bytes[offset + 5] & 0xff) << 16 |
    				((long) bytes[offset + 6] & 0xff) << 8 |
    				(long) bytes[offset + 7] & 0xff;
    	}
    	return (long) bytes[offset] & 0xff |
    			((long) bytes[offset + 1] & 0xff) << 8 |
    			((long) bytes[offset + 2] & 0xff) << 16 |
    			((long) bytes[offset + 3] & 0xff) << 24 |
    			((long) bytes[offset + 4] & 0xff) << 32 |
    			((long) bytes[offset + 5] & 0xff) << 40 |
    			((long) bytes[offset + 6] & 0xff) << 48 | 
    			(long) bytes[offset + 7] & 0xff;
    }
    
    private static int getIntSafe(byte[] bytes, int offset) {
    	if (BIG_ENDIAN_NATIVE_ORDER) {
    		return bytes[offset] << 24 |
    				(bytes[offset + 1] & 0xff) << 16 | 
    				(bytes[offset + 2] & 0xff) << 8 |
    				bytes[offset + 3] & 0xff;
    	}
    	return bytes[offset] | 
    			(bytes[offset + 1] & 0xff) << 8 |
    			(bytes[offset + 2] & 0xff) << 16 | 
    			bytes[offset + 3] & 0xff << 24;
    }
    
    private static short getShortSafe(byte[] bytes, int offset) {
    	if (BIG_ENDIAN_NATIVE_ORDER) {
    		return (short) (bytes[offset] << 8 | (bytes[offset + 1] & 0xff));
    	}
    	return (short) (bytes[offset] & 0xff | (bytes[offset + 1] << 8));
    }
    
    private static int hashCodeAsciiCompute(CharSequence value, int offset, int hash) {
    	if (BIG_ENDIAN_NATIVE_ORDER) {
    		return hash * HASH_CODE_C1 + 
    				hashCodeAsciiSanitizeInt(value, offset + 4) * HASH_CODE_C2 +
    				hashCodeAsciiSanitizeInt(value, offset);
    	}
    	return hash * HASH_CODE_C1 +
    			hashCodeAsciiSanitizeInt(value, offset) * HASH_CODE_C2 +
    			hashCodeAsciiSanitizeInt(value, offset + 4);
    }
    
    private static int hashCodeAsciiSanitizeInt(CharSequence value, int offset) {
    	if (BIG_ENDIAN_NATIVE_ORDER) {
    		return (value.charAt(offset + 3) & 0x1f) | 
    			   (value.charAt(offset + 2) & 0x1f) << 8 |
    			   (value.charAt(offset + 1) & 0x1f) << 16 |
    			   (value.charAt(offset) & 0x1f) << 24;
    	} 
    	return (value.charAt(offset + 3) & 0x1f) << 24 |
    		   (value.charAt(offset + 2) & 0x1f) << 16 |
    		   (value.charAt(offset + 1) & 0x1f) << 8 |
    		   (value.charAt(offset) & 0x1f);
    }
    
    private static int hashCodeAsciiSanitizeShort(CharSequence value, int offset) {
    	if (BIG_ENDIAN_NATIVE_ORDER) {
    		return (value.charAt(offset + 1) & 0x1f) |
    			   (value.charAt(offset) & 0x1f) << 8;
    	}
    	return (value.charAt(offset + 1) & 0x1f) << 8 |
    		   (value.charAt(offset) & 0x1f);
    }
    
    private static int hashCodeAsciiSanitizeByte(char value) {
    	return value & 0x1f;
    }
    
    public static void putByte(long address, byte value) {
    	PlatformDependent0.putByte(address, value);
    }
    
    public static void putShort(long address, short value) {
    	PlatformDependent0.putShort(address, value);
    }
    
    public static void putInt(long address, int value) {
    	PlatformDependent0.putInt(address, value);
    }
    
    public static void putLong(long address, long value) {
    	PlatformDependent0.putLong(address, value);
    }
    
    public static void putByte(byte[] data, int index, byte value) {
    	PlatformDependent0.putByte(data, index, value);
    }
    
    public static void putByte(Object data, long offset, byte value) {
    	PlatformDependent0.putByte(data, offset, value);
    }
    
    public static void putShort(byte[] data, int index, short value) {
    	PlatformDependent0.putShort(data, index, value);
    }
    
    public static void putInt(byte[] data, int index, int value) {
    	PlatformDependent0.putInt(data, index, value);
    }
    
    public static void putLong(byte[] data, int index, long value) {
    	PlatformDependent0.putLong(data, index, value);
    }
    
    public static void putObject(Object o, long offset, Object x) {
    	PlatformDependent0.putObject(o, offset, x);
    }
    
    public static long objectFieldOffset(Field field) {
    	return PlatformDependent0.objectFieldOffset(field);
    }
    
    public static void copyMemory(long srcAddr, long dstAddr, long length) {
    	PlatformDependent0.copyMemory(srcAddr, dstAddr, length);
    }
    
    public static void copyMemory(byte[] src, int srcIndex, long dstAddr, long length) {
    	PlatformDependent0.copyMemory(src, BYTE_ARRAY_BASE_OFFSET + srcIndex, null, dstAddr, length);
    }
    
    public static void copyMemory(byte[] src, int srcIndex, byte[] dst, int dstIndex, long length) {
    	PlatformDependent0.copyMemory(src, BYTE_ARRAY_BASE_OFFSET + srcIndex, 
    								  dst, BYTE_ARRAY_BASE_OFFSET + dstIndex, length);
    }
    
    public static void copyMemory(long srcAddr, byte[] dst, int dstIndex, long length) {
    	PlatformDependent0.copyMemory(null, srcAddr, dst, BYTE_ARRAY_BASE_OFFSET + dstIndex, length);
    }
    
    public static void setMemory(byte[] dst, int dstIndex, long bytes, byte value) {
    	PlatformDependent0.setMemory(dst, BYTE_ARRAY_BASE_OFFSET + dstIndex, bytes, value);
    }
    
    public static void setMemory(long address, long bytes, byte value) {
    	PlatformDependent0.setMemory(address, bytes, value);
    }
    
    public static boolean hasAlignDirectByteBuffer() {
    	return hasUnsafe() || PlatformDependent0.hasAlignSliceMethod();
    }
    
    public static ByteBuffer alignDirectBuffer(ByteBuffer buffer, int alignment) {
    	if (!buffer.isDirect()) {
    		throw new IllegalArgumentException("Cannot get aligned slice of non-direct byte buffer");
    	}
    	if (PlatformDependent0.hasAlignSliceMethod()) {
    		return PlatformDependent0.alignSlice(buffer, alignment);
    	}
    	if (hasUnsafe()) {
    		long address = directBufferAddress(buffer);
    		long aligned = align(address, alignment);
    		buffer.position((int) (aligned - address));
    		return buffer.slice();
    	}
    	throw new UnsupportedOperationException("Cannot align direct buffer. " + 
    			"Needs either Unsafe or ByteByffer.alignSlice method available.");
    }
    
    public static long align(long value, int alignment) {
    	return Pow2.align(value, alignment);
    }
    
    public static ByteBuffer offsetSlice(ByteBuffer buffer, int index, int length) {
    	if (PlatformDependent0.hasOffsetSliceMethod()) {
    		return PlatformDependent0.offsetSlice(buffer, index, length);
    	} else { 
    		return ((ByteBuffer) buffer.duplicate().clear().position(index).limit(index + length)).slice();
    	}
    }
    
    public static ByteBuffer absolutePut(ByteBuffer dst, int dstOffset, byte[] src, int srcOffset, int length) {
    	if (PlatformDependent0.hasAbsolutePutArrayMethod()) {
    		return PlatformDependent0.absolutePut(dst, dstOffset, src, srcOffset, length);
    	} else {
    		ByteBuffer tmp = (ByteBuffer) dst.duplicate().clear().position(dstOffset).limit(dstOffset + length);
    		tmp.put(ByteBuffer.wrap(src, srcOffset, length));
    		return dst;
    	}
    }
    
    public static ByteBuffer absolutePut(ByteBuffer dst, int dstOffset, ByteBuffer src, int srcOffset, int length) {
    	if (PlatformDependent0.hasAbsolutePutArrayMethod()) {
    		return PlatformDependent0.absolutePut(dst, dstOffset, src, srcOffset, length);
    	} else {
    		ByteBuffer a = (ByteBuffer) dst.duplicate().clear().position(dstOffset).limit(dstOffset + length);
    		ByteBuffer b = (ByteBuffer) src.duplicate().clear().position(srcOffset).limit(srcOffset + length);
    		a.put(b);
    		return dst;
    	}
    }
    
    static void incrementMemoryCounter(int capacity) {
    	if (DIRECT_MEMORY_COUNTER != null) {
    		long newUsedMemory = DIRECT_MEMORY_COUNTER.addAndGet(capacity);
    		if (newUsedMemory > DIRECT_MEMORY_LIMIT) {
    			DIRECT_MEMORY_COUNTER.addAndGet(-capacity);
    			throw new OutOfDirectMemoryError("failed to allocate " + capacity 
    					+ " byte(s) of direct memory (used: " + (newUsedMemory - capacity)
    					+ ", max: " + DIRECT_MEMORY_LIMIT + ')');
    		} 
    	}
    }
    
    static void decrementMemoryCounter(int capacity) {
    	if (DIRECT_MEMORY_COUNTER != null) {
    		long usedMemory = DIRECT_MEMORY_COUNTER.addAndGet(-capacity);
    		assert usedMemory >= 0;
    	}
    }
    
    public static boolean useDirectBufferNoCleaner() {
    	return CLEANER instanceof DirectCleaner;
    }
    
    public static boolean equals(byte[] bytes1, int startPos1, byte[] bytes2, int startPos2, int length) {
    	if (javaVersion() > 8 && (startPos2 | startPos1 | (bytes1.length - length) | bytes2.length - length) == 0) {
    		return Arrays.equals(bytes1, bytes2);
    	}
    	return !hasUnsafe() || !unalignedAccess() ? 
    			equalsSafe(bytes1, startPos1, bytes2, startPos2, length) :
    			PlatformDependent0.equals(bytes1, startPos1, bytes2, startPos2, length);
    }
    
    public static boolean isZero(byte[] bytes, int startPos, int length) {
    	return !hasUnsafe() || !unalignedAccess() ? 
    			isZeroSafe(bytes, startPos, length) :
    			PlatformDependent0.isZero(bytes, startPos, length);
    }
    
    public static int equalsConstantTime(byte[] bytes1, int startPos1, byte[] bytes2, int startPos2, int length) {
    	return !hasUnsafe() || !unalignedAccess() ?
    			ConstantTimeUtils.equalsConstantTime(bytes1, startPos1, bytes2, startPos2, length) :
    			PlatformDependent0.equalsConstantTime(bytes1, startPos1, bytes2, startPos2, length);
    }
    
    public static int hashCodeAscii(byte[] bytes, int startPos, int length) {
    	return !hasUnsafe() || !unalignedAccess() ?
    			hashCodeAsciiSafe(bytes, startPos, length) :
    			PlatformDependent0.hashCodeAscii(bytes, startPos, length);
    }
    
    public static int hashCodeAscii(CharSequence bytes) {
    	final int length = bytes.length();
    	final int remainingBytes = length & 7;
    	int hash = HASH_CODE_ASCII_SEED;
    	if (length >= 32) {
    		for (int i = length - 8; i >= remainingBytes; i -= 8) {
    			hash = hashCodeAsciiCompute(bytes, i, hash);
    		}
    	} else if (length >= 8) {
    		hash = hashCodeAsciiCompute(bytes, length - 8, hash);
    		if (length >= 16) {
    			hash = hashCodeAsciiCompute(bytes, length - 16, hash);
    			if (length >= 24) {
    				hash = hashCodeAsciiCompute(bytes, length - 24, hash);
    			}
    		}
    	}
    	if (remainingBytes == 0) {
    		return hash;
    	}
    	int offset = 0;
    	if (remainingBytes != 2 & remainingBytes != 4 & remainingBytes != 6) {
    		hash = hash * HASH_CODE_C1 + hashCodeAsciiSanitizeByte(bytes.charAt(0));
    		offset = 1;
    	}
    	if (remainingBytes != 1 & remainingBytes != 4 & remainingBytes != 5) {
    		hash = hash * (offset == 0 ? HASH_CODE_C1 : HASH_CODE_C2)
    				+ hashCodeAsciiSanitize(hashCodeAsciiSanitizeShort(bytes, offset));
    		offset += 2;
    	}
    	if (remainingBytes >= 4) {
    		return hash * ((offset == 0 | offset == 3) ? HASH_CODE_C1 : HASH_CODE_C2)
    				+ hashCodeAsciiSanitizeInt(bytes, offset);
    	}
    	return hash;
    }
    
    private static final class Mpsc {
    	private static final boolean USE_MPSC_CHUNKED_ARRAY_QUEUE;
    	
    	static {
    		Object unsafe = null;
    		if (hasUnsafe()) {
    			unsafe = AccessController.doPrivileged(new PrivilegedAction<Object>(){
    				@Override
    				public Object run() {
    					return UnsafeAccess.UNSAFE;
    				}
    			});
    		}
    		if (unsafe == null) {
    			logger.debug("org.jctools-core.MpscChunkedArrayQueue: unavailable");
    			USE_MPSC_CHUNKED_ARRAY_QUEUE = false;
    		} else {
    			logger.debug("org.jctools-core.MpscChunkedArrayQueue: available");
    			USE_MPSC_CHUNKED_ARRAY_QUEUE = true;
    		}
    	}
    	
    	static <T> Queue<T> newMpscQueue(final int maxCapacity) {
    		final int capacity = Math.max(Math.min(maxCapacity, MAX_ALLOWED_MPSC_CAPACITY), MIN_MAX_MPSC_CAPACITY);
    		return newChunkedMpscQueue(MPSC_CHUNK_SIZE, capacity);
    	}
    	
    	
    	static <T> Queue<T> newChunkedMpscQueue(final int chunkSize, final int capacity) {
    		return USE_MPSC_CHUNKED_ARRAY_QUEUE ? new MpscChunkedArrayQueue<T>(chunkSize, capacity)
    				: new MpscChunkedAtomicArrayQueue<T>(chunkSize, capacity);
    	}
    	
    	static <T> Queue<T> newMpscQueue() {
    		return USE_MPSC_CHUNKED_ARRAY_QUEUE ? new MpscUnboundedArrayQueue<T>(MPSC_CHUNK_SIZE)
    											: new MpscUnboundedAtomicArrayQueue<T>(MPSC_CHUNK_SIZE);
    	}
    }
    
    public static <T> Queue<T> newMpscQueue() {
    	return Mpsc.newMpscQueue();
    }
    
    public static <T> Queue<T> newMpscQueue(final int maxCapacity) {
    	return Mpsc.newMpscQueue(maxCapacity);
    }
    
    public static <T> Queue<T> newMpscQueue(final int chunkSize, final int maxCapacity) {
    	return Mpsc.newChunkedMpscQueue(chunkSize, maxCapacity);
    }
    
    public static <T> Queue<T> newSpscQueue() {
    	return hasUnsafe() ? new SpscLinkedQueue<T>() : new SpscLinkedAtomicQueue<T>();
    }
    
    public static <T> Queue<T> newFixedMpscQueue(int capacity) {
    	return hasUnsafe() ? new MpscArrayQueue<T>(capacity) : new MpscAtomicArrayQueue<T>(capacity);
    }
    
    public static <T> Queue<T> newFixedMpscUnpaddedQueue(int capacity) {
    	return hasUnsafe() ? new MpscUnpaddedArrayQueue<T>(capacity) : new MpscAtomicUnpaddedArrayQueue<T>(capacity);
    }
    
    public static <T> Queue<T> newFixedMpmcQueue(int capacity) {
    	return hasUnsafe() ? new MpmcArrayQueue<T>(capacity) : new MpmcAtomicArrayQueue<T>(capacity);
    }
    
    public static ClassLoader getClassLoader(final Class<?> clazz) {
    	return PlatformDependent0.getClassLoader(clazz);
    }
    
    public static ClassLoader getContextClassLoader() {
    	return PlatformDependent0.getContextClassLoader();
    }
    
    public static ClassLoader getSystemClassLoader() {
    	return PlatformDependent0.getSystemClassLoader();
    }
    
    public static <C> Deque<C> newConcurrentDeque() {
    	return new ConcurrentLinkedDeque<C>();
    }
    
    public static Random threadLocalRandom() {
    	return ThreadLocalRandom.current();
    }
    
    private static boolean isWindows0() {
    	boolean windows = "windows".equals(NORMALIZED_OS);
    	if (windows) {
    		logger.debug("Platform: Windows");
    	}
    	return windows;
    }
    
    private static boolean isOsx0() {
    	boolean osx = "osx".equals(NORMALIZED_OS);
    	if (osx) {
    		logger.debug("Platform: MacOS");
    	}
    	return osx;
    }
    
    private static boolean maybeSuperUser0() {
    	String username	 = SystemPropertyUtil.get("user.name");
    	if (isWindows()) {
    		return "Administrator".equals(username);
    	}
    	return "root".equals(username) || "toor".equals(username);
    }
    
    private static Throwable unsafeUnavailabilityCause0() {
    	if (isAndroid()) {
    		logger.debug("sun.misc.Unsafe: unavailable (Android)");
    		return new UnsupportedOperationException("sun.misc.Unsafe: unavailable(Android)");
    	}
    	if (isIkvmDotNet()) {
    		logger.debug("sun.misc.Unsafe: unavailable (IKVM.NET)");
    		return new UnsupportedOperationException("sun.misc.Unsafe: unavailable(IKVM.NET)");
    	}
    	
    	Throwable cause = PlatformDependent0.getUnsafeUnavailabilityCause();
    	if (cause != null) {
    		return cause;
    	}
    	
    	try {
    		boolean hasUnsafe = PlatformDependent0.hasUnsafe();
    		logger.debug("sun.misc.Unsafe: {}", hasUnsafe ? "available" : "unavailable");
    		return null;
    	} catch (Throwable t) {
    		logger.trace("Could not determine if Unsafe is available", t);
    		return new UnsupportedOperationException("Could not determine if Unsafe is available", t);
    	}
    }
    
    public static boolean isJ9Jvm() {
    	return IS_J9_JVM;
    }
    
    private static boolean isJ9Jvm0() {
    	String vmName = SystemPropertyUtil.get("java.vm.name", "").toLowerCase();
    	return vmName.startsWith("ibm j9") || vmName.startsWith("eclipse openj9");
    }
    
    public static boolean isIkvmDotNet() {
    	return IS_IVKVM_DOT_NET;
    }
    
    private static boolean isIkvmDotNet0() {
    	String vmName = SystemPropertyUtil.get("java.vm.name", "").toUpperCase(Locale.US);
    	return vmName.equals("IKVM.NET");
    }
    
    private static Pattern getMaxDirectMemorySizeArgPattern() {
    	Pattern pattern = MAX_DIRECT_MEMORY_SIZE_ARG_PATTERN;
    	if (pattern == null) {
    		pattern = Pattern.compile("\\s*-XX:MaxDirectMemorySize\\s*=\\s*([0-9]+)\\s*([kKmMgG]?)\\s*$");
    		MAX_DIRECT_MEMORY_SIZE_ARG_PATTERN = pattern;
    	}
    	return pattern;
    }
    
    public static long estimateMaxDirectMemory() {
    	long maxDirectMemory = PlatformDependent0.bitsMaxDirectMemory();
    	if (maxDirectMemory > 0) {
    		return maxDirectMemory;
    	}
    	
    	try {
    		ClassLoader systemClassLoader = getSystemClassLoader();
    		Class<?> mgmtFactoryClass = Class.forName(
    				"java.lang.management.ManagementFactory", true, systemClassLoader);
    		Class<?> runtimeClass = Class.forName(
    				"java.lang.management.RuntimeMXBean", true, systemClassLoader);
    		
    		MethodHandles.Lookup lookup = MethodHandles.publicLookup();
    		MethodHandle getRuntime = lookup.findStatic(mgmtFactoryClass, "getRuntimeMXBean", methodType(runtimeClass));
    		MethodHandle getInputArguments = lookup.findVirtual(runtimeClass, "getInputArguments", methodType(List.class));
    		List<String> vmArgs = (List<String>) getInputArguments.invoke(getRuntime.invoke());
    		
    		Pattern maxDirectMemorySizeArgPattern = getMaxDirectMemorySizeArgPattern();
    		
    		for (int i = vmArgs.size() - 1; i >= 0; i--) {
    			Matcher m = maxDirectMemorySizeArgPattern.matcher(vmArgs.get(i));
    			if (!m.matches()) {
    				continue;
    			}
    			
    			maxDirectMemory = Long.parseLong(m.group(1));
    			switch (m.group(2).charAt(0)) {
    			case 'k': case 'K':
    				maxDirectMemory *= 1024;
    				break;
    			case 'm': case 'M':
    				maxDirectMemory *= 1024 * 1024;
    				break;
    			case 'g': case 'G':
    				maxDirectMemory *= 1024 * 1024 * 1024;
    				break;
    			default:
    				break;
    			}
    			break;
    		}
    	} catch (Throwable throwable) {
    		
    	}
    	if (maxDirectMemory <= 0) {
    		maxDirectMemory = Runtime.getRuntime().maxMemory();
    		logger.debug("maxDirectMemory: {} bytes (maybe)", maxDirectMemory);
    	} else {
    		logger.debug("maxDirectMemory: {} bytes", maxDirectMemory);
    	}
    	return maxDirectMemory;
    }
    
    private static File tmpDir0() {
    	File f;
    	try {
    		f = toDirectory(SystemPropertyUtil.get("io.netty.tmpdir"));
    		if (f != null) {
    			logger.debug("-Dio.netty.tmpdir: {}", f);
    			return f;
    		}
    		
    		f = toDirectory(SystemPropertyUtil.get("java.io.tmpdir"));
    		if (f != null) {
    			logger.debug("-Dio.netty.tmpdir: {} (java.io.tmpdir)", f);
    			return f;
    		}
    		
    		if (isWindows()) {
    			f = toDirectory(System.getenv("TEMP"));
    			if (f != null) {
    				logger.debug("-Dio.netty.tmpdir: {} (%TEMP%)", f);
    				return f;
    			}
    			
    			String userprofile = System.getenv("USERPROFILE");
    			if (userprofile != null) {
    				f = toDirectory(userprofile + "\\AppData\\Local\\Temp");
    				if (f != null) {
    					logger.debug("-Dio.netty.tmpdir: {} (%USERPROFILE%\\AppData\\Local\\Temp)", f);
    					return f;
    				}
    				
    				f = toDirectory(userprofile + "\\Local Settings\\Temp");
    				if (f != null) {
    					logger.debug("-Dio.netty.tmpdir: {} (%USERPROFILE%\\Local Settings\\Temp)", f);
    					return f;
    				}
    			}
    		} else {
    			f = toDirectory(System.getenv("TMPDIR"));
    			if (f != null) {
    				logger.debug("-Dio.netty.tmpdir: {}($TMPDIR)", f);
    				return f;
    			}
    		}
    	} catch (Throwable ignored) {
  
    	}
    	
    	if (isWindows()) {
    		f = new File("C:\\Windows\\Temp");
    	} else {
    		f = new File("/tmp");
    	}
    	
    	logger.warn("Failed to get the temporary directory: failing back to: {}", f);
    	return f;
    } 
    
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static File toDirectory(String path) {
    	if (path == null) {
    		return null;
    	}
    	
    	File f = new File(path);
    	f.mkdirs();
    	if (!f.isDirectory()) {
    		return null;
    	}
    	
    	try {
    		return f.getAbsoluteFile();
    	} catch (Exception ignored) {
    		return f;
    	}
    }
    
    private static int bitMode0() {
    	int bitMode = SystemPropertyUtil.getInt("io.netty.bitMode", 0);
    	if (bitMode > 0) {
    		logger.debug("-Dio.netty.bitMode: {}", bitMode);
    		return bitMode;
    	}
    	
    	bitMode = SystemPropertyUtil.getInt("sun.arch.data.model", 0);
    	if (bitMode > 0) {
    		logger.debug("-Dio.netty.bitMode: {} (sun.arch.data.model)", bitMode);
    		return bitMode;
    	}
    	bitMode = SystemPropertyUtil.getInt("com.ibm.vm.bitMode", 0);
    	if (bitMode > 0) {
    		logger.debug("-Dio.netty.bitMode: {} (com.ibm.vm.bitMode)", bitMode);
    		return bitMode;
    	}
    	
    	String arch = SystemPropertyUtil.get("os.arch", "").toLowerCase(Locale.US).trim();
    	if ("amd64".equals(arch) || "x86_64".equals(arch)) {
    		bitMode = 64;
    	} else if ("i386".equals(arch) || "i486".equals(arch) || "i586".equals(arch) || "i686".equals(arch)) {
    		bitMode = 32;
    	}
    	
    	if (bitMode > 0) {
    		logger.debug("-Dio.netty.bitMode: {} (os.arch: {})", bitMode, arch);
    	}
    	
    	String vm = SystemPropertyUtil.get("java.vm.name", "").toLowerCase(Locale.US);
    	Pattern bitPattern = Pattern.compile("([1-9][0-9]+)-?bit");
    	Matcher m = bitPattern.matcher(vm);
    	if (m.find()) {
    		return Integer.parseInt(m.group(1));
    	} else {
    		return 64;
    	}
    }
    
    private static int addressSize0() {
    	if (!hasUnsafe()) {
    		return -1;
    	}
    	return PlatformDependent0.addressSize();
    }
    
    private static long byteArrayBaseOffset0() {
    	if (!hasUnsafe() ) {
    		return -1;
    	}
    	return PlatformDependent0.byteArrayBaseOffset();
    }
    
    private static boolean equalsSafe(byte[] bytes1, int startPos1, byte[] bytes2, int startPos2, int length) {
    	final int end = startPos1 + length;
    	for (; startPos1 < end; ++startPos1, ++startPos2) {
    		if (bytes1[startPos1] != bytes2[startPos2]) {
    			return false;
    		}
    	}
    	return true;
    }
    
    private static boolean isZeroSafe(byte[] bytes, int startPos, int length) {
    	final int end = startPos + length;
    	for (; startPos < end; ++startPos)  {
    		if (bytes[startPos] != 0) {
    			return false;
    		}
    	}
    	return true;
    }
    
    static int hashCodeAsciiSafe(byte[] bytes, int startPos, int length) {
    	int hash = HASH_CODE_ASCII_SEED;
    	final int remainingBytes = length & 7;
    	final int end = startPos + remainingBytes;
    	for (int i = startPos - 8 + length; i >= end; i -= 8) {
    		hash = PlatformDependent0.hashCodeAsciiCompute(getLongSafe(bytes, 1), hash);
    	}
    	switch (remainingBytes) {
    	case 7:
    		return ((hash * HASH_CODE_C1 + hashCodeAsciiSanitize(bytes[startPos]))
    					  * HASH_CODE_C2 + hashCodeAsciiSanitize(getShortSafe(bytes, startPos + 1)))
    					  * HASH_CODE_C1 + hashCodeAsciiSanitize(getIntSafe(bytes, startPos + 3));
    	case 6:
    		return (hash * HASH_CODE_C1 + hashCodeAsciiSanitize(getShortSafe(bytes, startPos)))
    					 * HASH_CODE_C2 + hashCodeAsciiSanitize(getIntSafe(bytes, startPos + 2));
    	case 5:
    		return (hash * HASH_CODE_C1 + hashCodeAsciiSanitize(bytes[startPos]))
    					 * HASH_CODE_C2 + hashCodeAsciiSanitize(getIntSafe(bytes, startPos + 1));
    	case 4:
    		return (hash * HASH_CODE_C1 + hashCodeAsciiSanitize(getIntSafe(bytes, startPos)));
    	case 3:
    		return (hash * HASH_CODE_C1 + hashCodeAsciiSanitize(bytes[startPos]))
    					 * HASH_CODE_C2 + hashCodeAsciiSanitize(getShortSafe(bytes, startPos + 1));
    	case 2:
    		return (hash * HASH_CODE_C1 + hashCodeAsciiSanitize(getShortSafe(bytes, startPos)));
    	case 1:
    		return (hash * HASH_CODE_C1 + hashCodeAsciiSanitize(bytes[startPos]));
    	default:
    		return hash;
    	}
    }
    
    public static String normalizedArch() {
    	return NORMALIZED_ARCH;
    }
    
    public static String normalizedOs() {
    	return NORMALIZED_OS;
    }
    
    public static Set<String> normalizedLinuxClassifiers() {
    	return LINUX_OS_CLASSIFIERS;
    }
    
    public static File createTempFile(String prefix, String suffix, File directory) throws IOException {
    	if (directory == null) {
    		return Files.createTempFile(prefix, suffix).toFile();
    	}
    	return Files.createTempFile(directory.toPath(), prefix, suffix).toFile();
    }
    
    private static void addClassifier(Set<String> dest, String... maybeClassifiers) {
    	for (String id : maybeClassifiers) {
    		if (isAllowedClassifier(id)) {
    			dest.add(id);
    		}
    	}
    }
    
    private static boolean isAllowedClassifier(String classifier) {
    	switch (classifier) {
    	case "fedora":
    	case "suse":
    	case "arch":
    		return true;
    	default: 
    		return false;
    	}
    }
    
    private static String normalizeOsReleaseVariableValue(String value) {
    	String trimmed = value.trim();
    	StringBuilder sb = new StringBuilder(trimmed.length());
    	for (int i = 0; i < trimmed.length(); ++i) {
    		char c = trimmed.charAt(i);
    		if (c != '"' && c != '\'') {
    			sb.append(c);
    		}
    	}
    	return sb.toString();
    }
    
    private static String normalize(String value) {
    	StringBuilder sb = new StringBuilder(value.length());
    	for (int i = 0; i < value.length(); ++i) {
    		char c = Character.toLowerCase(value.charAt(i));
    		if ((c >= 'a' && c <= 'z' || c >= '0' && c <= '9')) {
    			sb.append(c);
    		}
    	}
    	
    	return sb.toString();
    }
    
    private static String normalizeArch(String value) {
    	value = normalize(value);
    	switch (value) {
    	case "x8664":
    	case "amd64":
    	case "ia32e":
    	case "em64t":
    	case "x64":
    		return "x86_64";
    		
    	case "x8632":
    	case "x86":
    	case "i386":
    	case "i486":
    	case "i586":
    	case "i686":
    	case "ia32":
    	case "x32":
    		return "x86_32";
    		
    	case "ia64":
    	case "itanium64":
    		return "itanium_64";
    		
    	case "sparc":
    	case "sparc64":
    		return "sparc_64";
    		
    	case "arm":
    	case "arm32":
    		return "arm_32";
    		
    	case "aarch64":
    		return "aarch_64";
    		
    	case "riscv64":
    		return "riscv64";
    		
    	case "ppc":
    	case "ppc32":
    		return "ppc_32";
    		
    	case "ppc64":
    		return "ppc_64";
    		
    	case "s390":
    		return "s390_32";
    		
    	case "s390x":
    		return "s390_64";
    		
    	case "loongarch64":
    		return "loongarch64";
    		
    	default:
    		return "unknown";
    	}
    }
    
    private static String normalizeOs(String value) {
    	value = normalize(value);
    	if (value.startsWith("aix")) {
    		return "aix";
    	}
    	if (value.startsWith("hpux")) {
    		return "hpux";
    	}
    	if (value.startsWith("os400")) {
    		if (value.length() <= 5 || !Character.isDigit(value.charAt(5))) {
    			return "os400";
    		}
    	}
    	if (value.startsWith("linux")) {
    		return "linux";
    	}
    	if (value.startsWith("macosx") || value.startsWith("osx") || value.startsWith("darwin")) {
    		return "osx";
    	}
    	if (value.startsWith("freebsd")) {
    		return "freebsd";
    	}
    	if (value.startsWith("openbsd")) {
    		return "openbsd";
    	}
    	if (value.startsWith("netbsd")) {
    		return "netbsd";
    	}
    	if (value.startsWith("solaris") || value.startsWith("sunos")) {
    		return "sunos";
    	}
    	if (value.startsWith("windows")) {
    		return "windows";
    	}
    	return "unknown";	
    }
    
    public static boolean isJfrEnabled() {
    	return JFR;
    }
    
    private PlatformDependent() {
    	
    }
} 
