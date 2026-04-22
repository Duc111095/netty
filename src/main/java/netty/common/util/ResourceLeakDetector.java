package netty.common.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import netty.common.util.internal.EmptyArrays;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static netty.common.util.internal.StringUtil.EMPTY_STRING;
import static netty.common.util.internal.StringUtil.NEWLINE;
import static netty.common.util.internal.StringUtil.simpleClassName;


public class ResourceLeakDetector<T> {
	
	private static final String PROP_LEVEL_OLD = "io.netty.leakDetectionLevel";
	private static final String PROP_LEVEL = "io.netty.leakDetection.level";
	private static final Level DEFAULT_LEVEL = Level.SIMPLE;
	
	private static final String PROP_TARGET_RECORDS = "io.netty.leakDetection.targetRecords";
	private static final int DEFAULT_TARGET_RECORDS = 4;
	
	private static final String PROP_SAMPLING_INTERVAL = "io.netty.leakDetection.samplingInterval";
	private static final int DEFAULT_SAMPLING_INTERVAL = 128;
	
	private static final String PROP_TRACK_CLOSE = "io.netty.leakDetection.trackClose";
	private static final boolean DEFAULT_TRACK_CLOSE = true;
	
	private static final int TARGET_RECORDS;
	static final int SAMPLING_INTERVAL;
	private static final boolean TRACK_CLOSE;
	
	public enum Level {
		DISABLED,
		SIMPLE,
		ADVANCED,
		PARANOID;
		
		static Level parseLevel(String levelStr) {
			String trimmedLevelStr = levelStr.trim();
			for (Level l : values()) {
				if (trimmedLevelStr.equalsIgnoreCase(l.name()) || trimmedLevelStr.equals(String.valueOf(l.ordinal()))) {
					return l;
				}
			}
			return DEFAULT_LEVEL;
		}
	}
	
	private static Level level;
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(ResourceLeakDetector.class);
	
	static {
		final boolean disabled;
		if (SystemPropertyUtil.get("io.netty.noResourceLeakDetection") != null) {
			disabled = SystemPropertyUtil.getBoolean("io.netty.noResourceLeakDetection", false);
			logger.debug("-Dio.netty.noResourceLeakDetection: {}", disabled);
			logger.warn(
					"-Dio.netty.noResourceLeakDetection is deprecated. Use '-D{}={}' instead.",
					PROP_LEVEL, Level.DISABLED.name().toLowerCase());
		} else {
			disabled = false;
		}
		
		Level defaultLevel = disabled ? Level.DISABLED : DEFAULT_LEVEL;
		
		String levelStr = SystemPropertyUtil.get(PROP_LEVEL_OLD, defaultLevel.name());
		
		levelStr = SystemPropertyUtil.get(PROP_LEVEL, levelStr);
		Level level = Level.parseLevel(levelStr);
		
		TARGET_RECORDS = SystemPropertyUtil.getInt(PROP_TARGET_RECORDS, DEFAULT_TARGET_RECORDS);
		SAMPLING_INTERVAL = SystemPropertyUtil.getInt(PROP_SAMPLING_INTERVAL, DEFAULT_SAMPLING_INTERVAL);
		TRACK_CLOSE = SystemPropertyUtil.getBoolean(PROP_TRACK_CLOSE, DEFAULT_TRACK_CLOSE);
		
		ResourceLeakDetector.level = level;
		if (logger.isDebugEnabled()) {
			logger.debug("-D{}: {}", PROP_LEVEL, level.name().toLowerCase());
			logger.debug("-D{}: {}", PROP_TARGET_RECORDS, TARGET_RECORDS);
		}
	}
	
	public static void setEnabled(boolean enabled) {
		setLevel(enabled ? Level.SIMPLE : Level.DISABLED);
	}
	
	public static boolean isEnabled() {
		return getLevel().ordinal() > Level.DISABLED.ordinal();
	}
	
	public static void setLevel(Level level) {
		ResourceLeakDetector.level = ObjectUtil.checkNotNull(level, "level");
	}
	
	public static Level getLevel() {
		return level;
	}
	
	private final Set<DefaultResourceLeak<?>> allLeaks = ConcurrentHashMap.newKeySet();
	private final ReferenceQueue<Object> refQueue = new ReferenceQueue<>();
	private final Set<String> reportedLeaks = ConcurrentHashMap.newKeySet();

	private final String resourceType;
	private final int samplingInterval;
	
	private volatile LeakListener leakListener;
	
	public ResourceLeakDetector(Class<?> resourceType) {
		this(simpleClassName(resourceType));
	}
	
	public ResourceLeakDetector(String resourceType) {
		this(resourceType, DEFAULT_SAMPLING_INTERVAL, Long.MAX_VALUE);
	}
	
	public ResourceLeakDetector(
			Class<?> resourceType, int samplingInterval, long maxActive) {
		this(resourceType, samplingInterval);
	}
	
	public ResourceLeakDetector(Class<?> resourceType, int samplingInterval) {
		this(simpleClassName(resourceType), samplingInterval, Long.MAX_VALUE);
	}
	
	public ResourceLeakDetector(
			String resourceType, int samplingInterval, long maxActive) {
		this.resourceType = ObjectUtil.checkNotNull(resourceType, "resourceType");
		this.samplingInterval = samplingInterval;
	}
	
	public final ResourceLeak open(T obj) {
		return track0(obj, false);
	}
	
	public ResourceLeakTracker<T> track(T obj) {
		return track0(obj, false);
	}
	
	public ResourceLeakTracker<T> trackForcibly(T obj) {
		return track0(obj, true);
	}
	
	public boolean isRecordEnabled() {
		Level level = getLevel();
		return (level == Level.ADVANCED || level == Level.PARANOID) && TARGET_RECORDS > 0;
	}
	
	private DefaultResourceLeak<T> track0(T obj, boolean force) {
		Level level = ResourceLeakDetector.level;
		if (force || 
				level == Level.PARANOID ||
				(level != Level.DISABLED && ThreadLocalRandom.current().nextInt(samplingInterval) == 0)) {
			reportLeak();
			return new DefaultResourceLeak<>(obj, refQueue, allLeaks, getInitialHint(resourceType));
		}
		return null;
	}
	
	private void clearRefQueue() {
		for (;;) {
			DefaultResourceLeak<?> ref = (DefaultResourceLeak<?>) refQueue.poll();
			if (ref == null) {
				break;
			}
			ref.dispose();
		}
	}
	
	protected boolean needReport() {
		return logger.isErrorEnabled();
	}
	
	private void reportLeak() {
		if (!needReport()) {
			clearRefQueue();
			return;
		}
		
		for (;;) {
			DefaultResourceLeak<?> ref = (DefaultResourceLeak<?>) refQueue.poll();
			if (ref == null) {
				break;
			}
			
			if (!ref.dispose()) {
				continue;
			}
			
			String records = ref.getReportAndClearRecords();
			if (reportedLeaks.add(records)) {
				if (records.isEmpty()) {
					reportUntracedLeak(resourceType);
				} else {
					reportTracedLeak(resourceType, records);
				}
				
				LeakListener listener = leakListener;
				if (listener != null) {
					listener.onLeak(resourceType, records);
				}
			}
		}
	}
	
	protected void reportTracedLeak(String resourceType, String records) {
		logger.error(
				"LEAK: {}.release() was not called before it's garbage-collected. " +
                "See https://netty.io/wiki/reference-counted-objects.html for more information.{}",
                resourceType, records);
	}
	
	protected void reportUntracedLeak(String resourceType) {
        logger.error("LEAK: {}.release() was not called before it's garbage-collected. " +
                "Enable advanced leak reporting to find out where the leak occurred. " +
                "To enable advanced leak reporting, " +
                "specify the JVM option '-D{}={}' or call {}.setLevel() " +
                "See https://netty.io/wiki/reference-counted-objects.html for more information.",
                resourceType, PROP_LEVEL, Level.ADVANCED.name().toLowerCase(), simpleClassName(this));
    }
	
	protected Object getInitialHint(String resourceType) {
		return null;
	}
	
	public void setLeakListener(LeakListener leakListener) {
		this.leakListener = leakListener;
	}
	
	public interface LeakListener {
		
		void onLeak(String resourceType, String records);	
	}
	
	private static final class DefaultResourceLeak<T> 
		extends WeakReference<Object> implements ResourceLeakTracker<T>, ResourceLeak {
		
		@SuppressWarnings({"unchecked", "rawtypes"}) // generics and updaters do not mix.
        private static final AtomicReferenceFieldUpdater<DefaultResourceLeak<?>, TraceRecord> headUpdater =
                (AtomicReferenceFieldUpdater)
                        AtomicReferenceFieldUpdater.newUpdater(DefaultResourceLeak.class, TraceRecord.class, "head");
		
		@SuppressWarnings({"unchecked", "rawtypes"})
		private static final AtomicIntegerFieldUpdater<DefaultResourceLeak<?>> droppedRecordsUpdater =
				(AtomicIntegerFieldUpdater)
					AtomicIntegerFieldUpdater.newUpdater(DefaultResourceLeak.class, "droppedRecords");
		
		private volatile TraceRecord head;
		private volatile int droppedRecords;
		
		private final Set<DefaultResourceLeak<?>> allLeaks;
		private final int trackedHash;
		
		DefaultResourceLeak(
				Object referent,
				ReferenceQueue<Object> refQueue,
				Set<DefaultResourceLeak<?>> allLeaks,
				Object initialHint) {
			super(referent, refQueue);
			
			assert referent != null;
			
			this.allLeaks = allLeaks;
			trackedHash = System.identityHashCode(referent);
			allLeaks.add(this);
			headUpdater.set(this, initialHint == null ? 
					new TraceRecord(TraceRecord.BOTTOM) : new TraceRecord(TraceRecord.BOTTOM, initialHint));
		}
		
		@Override
		public void record() {
			record0(null);
		}
		
		@Override
		public void record(Object hint) {
			record0(hint);
		}
		
		private void record0(Object hint) {
			if (TARGET_RECORDS > 0) {
				TraceRecord oldHead;
				TraceRecord prevHead;
				TraceRecord newHead;
				boolean dropped;
				do {
					if ((prevHead = oldHead = headUpdater.get(this)) == null ||
							oldHead.pos == TraceRecord.CLOSE_MARK_POS) {
						return;
					}
					final int numElements = oldHead.pos + 1;
					if (numElements >= TARGET_RECORDS) {
						final int backOffFactor = Math.min(numElements - TARGET_RECORDS, 30);
						dropped = ThreadLocalRandom.current().nextInt(1 << backOffFactor) != 0;
						if (dropped) {
							prevHead = oldHead.next;
						}
					} else {
						dropped = false;
					}
					newHead = hint != null ? new TraceRecord(prevHead, hint) : new TraceRecord(prevHead);
				} while (!headUpdater.compareAndSet(this, oldHead, newHead));
				if (dropped) {
					droppedRecordsUpdater.incrementAndGet(this);
				}
			}
		}
		
		boolean dispose() {
			clear();
			return allLeaks.remove(this);
		}
		
		@Override
		public boolean close() {
			if (allLeaks.remove(this)) {
				clear();
				headUpdater.set(this, TRACK_CLOSE ? new TraceRecord(true) : null);
				return true;
			}
			return false;
		}
		
		@Override
		public boolean close(T trackedObject) {
			assert trackedHash == System.identityHashCode(trackedObject);
			
			try {
				return close();
			} finally {
				reachabilityFence0(trackedObject);
			}
		}
		
		private static void reachabilityFence0(Object ref) {
			if (ref != null) {
				synchronized(ref) {
					
				}
			}
		}
		
		@Override
		public Throwable getCloseStackTraceIfAny() {
			TraceRecord head = headUpdater.get(this);
			if (head != null && head.pos == TraceRecord.CLOSE_MARK_POS) {
				return head;
			}
			return null;
		}
		
		@Override
		public String toString() {
			TraceRecord oldHead = headUpdater.get(this);
			return generateReport(oldHead);
		}
		
		String getReportAndClearRecords() {
            TraceRecord oldHead = headUpdater.getAndSet(this, null);
            return generateReport(oldHead);
        }
		
		private String generateReport(TraceRecord oldHead) {
            if (oldHead == null) {
                // Already closed
                return EMPTY_STRING;
            }

            final int dropped = droppedRecordsUpdater.get(this);
            int duped = 0;

            int present = oldHead.pos + 1;
            // Guess about 2 kilobytes per stack trace
            StringBuilder buf = new StringBuilder(present * 2048).append(NEWLINE);
            buf.append("Recent access records: ").append(NEWLINE);

            int i = 1;
            Set<String> seen = new HashSet<>(present);
            for (; oldHead != TraceRecord.BOTTOM; oldHead = oldHead.next) {
                String s = oldHead.toString();
                if (seen.add(s)) {
                    if (oldHead.next == TraceRecord.BOTTOM) {
                        buf.append("Created at:").append(NEWLINE).append(s);
                    } else {
                        buf.append('#').append(i++).append(':').append(NEWLINE).append(s);
                    }
                } else {
                    duped++;
                }
            }

            if (duped > 0) {
                buf.append(": ")
                        .append(duped)
                        .append(" leak records were discarded because they were duplicates")
                        .append(NEWLINE);
            }

            if (dropped > 0) {
                buf.append(": ")
                   .append(dropped)
                   .append(" leak records were discarded because the leak record count is targeted to ")
                   .append(TARGET_RECORDS)
                   .append(". Use system property ")
                   .append(PROP_TARGET_RECORDS)
                   .append(" to increase the limit.")
                   .append(NEWLINE);
            }

            buf.setLength(buf.length() - NEWLINE.length());
            return buf.toString();
        }
	}
	
	private static final AtomicReference<String[]> excludedMethods =
			new AtomicReference<>(EmptyArrays.EMPTY_STRINGS);
	
	public static void addExclusions(@SuppressWarnings("rawtypes") Class clz, String... methodNames) {
		Set<String> nameSet = new HashSet<>(Arrays.asList(methodNames));
		
		for (Method method : clz.getDeclaredMethods()) {
			if (nameSet.remove(method.getName()) && nameSet.isEmpty()) {
				break;
			}
		}
		if (!nameSet.isEmpty()) {
			throw new IllegalArgumentException("Can't find '" + nameSet + "' in " + clz.getName());
		}
		String[] oldMethods;
		String[] newMethods;
		do {
			oldMethods = excludedMethods.get();
			newMethods = Arrays.copyOf(oldMethods, oldMethods.length + 2 * methodNames.length);
			for (int i = 0; i < methodNames.length; i++) {
				newMethods[oldMethods.length + i * 2] = clz.getName();
				newMethods[oldMethods.length + i * 2 + 1] = methodNames[i];
			}
		} while (!excludedMethods.compareAndSet(oldMethods, newMethods));
	}
	
	private static class TraceRecord extends Throwable {
		private static final long serialVersionUID = 6065153674892850720L;
		public static final int BOTTOM_POS = -1;
		public static final int CLOSE_MARK_POS = -2;
		
		private static final TraceRecord BOTTOM = new TraceRecord(false) {
			private static final long serialVersionUID = 7396077602074694571L;
			
			@Override
			public Throwable fillInStackTrace() {
				return this;
			}
		};
		
		private final String hintString;
		private final TraceRecord next;
		private final int pos;
		
		TraceRecord(TraceRecord next, Object hint) {
			hintString = hint instanceof ResourceLeakHint ? ((ResourceLeakHint) hint).toHintString() : hint.toString();
			this.next = next;
			this.pos = next.pos + 1;
		}
		
		TraceRecord(TraceRecord next) {
			hintString = null;
			this.next = next;
			this.pos = next.pos + 1;
		}
		
		private TraceRecord(boolean closeMarker) {
			hintString = null;
			next = null;
			pos = closeMarker ? CLOSE_MARK_POS : BOTTOM_POS;
		}
		
		@Override
		public String toString() {
			StringBuilder buf = new StringBuilder(2048);
			if (hintString != null) {
				buf.append("\tHint: ").append(hintString).append(NEWLINE);
			}
			
			StackTraceElement[] array = getStackTrace();
			out: for (int i = 3; i < array.length; i++) {
				StackTraceElement element = array[i];
				String[] exclusions = excludedMethods.get();
				for (int k = 0; k < exclusions.length; k++) {
					if (exclusions[k].equals(element.getClassName())
							&& exclusions[k + 1].equals(element.getMethodName())) {
						continue out;
					}
				}
				buf.append('\t');
				buf.append(element.toString());
				buf.append(NEWLINE);
			}
			return buf.toString();
		}
	}
}
