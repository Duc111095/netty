package netty.common.util.internal;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.jctools.util.Pow2;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;
import netty.jfr_stub.jfr.FlightRecorder;

import static netty.common.util.internal.PlatformDependent0.HASH_CODE_ASCII_SEED;
import static netty.common.util.internal.PlatformDependent0.HASH_CODE_C1;
import static netty.common.util.internal.PlatformDependent0.HASH_CODE_C2;
import static netty.common.util.internal.PlatformDependent0.hashCodeAsciiSanitize;
import static netty.common.util.internal.PlatformDependent0.unalignedAccess;
import static java.lang.Math.max;
import static java.lang.Math.min;
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
	
	private static final File TMPDIR = tmpdir0();
	private static final int BIT_MODE = bitMode0();
	private static final String NORMALIZED_ARCH = normalizeArch(SystemPropertyUtil.get("os.arch", ""));
	private static final String NORMALIZED_OS = normalizeOs(SystemPropertyUtil.get("os.name", ""));
	
	private static final Set<String> LINUX_OS_CLASSIFIERS;
	
	private static final boolean IS_WINDOWS = isWindows0();
	private static final boolean IS_OSX = isOsx0();
	private static final boolean IS_J9_JVM = isJ9IJvm0();
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
	
	public static boolean isAligned() {
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
		PlatformDependent0.throwException(t);
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
    
    
}
