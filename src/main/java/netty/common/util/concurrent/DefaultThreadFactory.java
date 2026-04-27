package netty.common.util.concurrent;

import java.util.Locale;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.StringUtil;

public class DefaultThreadFactory implements ThreadFactory {
	
	private static final AtomicInteger poolId = new AtomicInteger();
	
	private final AtomicInteger nextId = new AtomicInteger();
	private final String prefix;
	private final boolean deamon;
	private final int priority;
	protected final ThreadGroup threadGroup;
	
	public DefaultThreadFactory(Class<?> poolType) {
		this(poolType, false, Thread.NORM_PRIORITY);
	}
	
	public DefaultThreadFactory(String poolName) {
		this(poolName, false, Thread.NORM_PRIORITY);
	}
	
	public DefaultThreadFactory(Class<?> poolType, boolean deamon) {
		this(poolType, deamon, Thread.NORM_PRIORITY);
	}
	
	public DefaultThreadFactory(String poolName, boolean deamon) {
		this(poolName, deamon, Thread.NORM_PRIORITY);
	}
	
	public DefaultThreadFactory(Class<?> poolType, int priority) {
		this(poolType, false, priority);
	}
	
	public DefaultThreadFactory(String poolName, int priority) {
		this(poolName, false, priority);
	}
	
	public DefaultThreadFactory(Class<?> poolType, boolean deamon, int priority) {
		this(toPoolName(poolType), deamon, priority);
	}
	
	public static String toPoolName(Class<?> poolType) {
		ObjectUtil.checkNotNull(poolType, "poolType");
		
		String poolName = StringUtil.simpleClassName(poolType);
		switch (poolName.length()) {
		case 0:
			return "unknown";
		case 1:
			return poolName.toLowerCase(Locale.US);
		default:
			if (Character.isUpperCase(poolName.charAt(0)) && Character.isLowerCase(poolName.charAt(1))) {
				return Character.toLowerCase(poolName.charAt(0)) + poolName.substring(1);
			} else {
				return poolName;
			}
		}
	}
	
	public DefaultThreadFactory(String poolName, boolean daemon, int priority) {
		this(poolName, daemon, priority, null);
	}
	
	public DefaultThreadFactory(String poolName, boolean deamon, int priority, ThreadGroup threadGroup) {
		ObjectUtil.checkNotNull(poolName, "poolName");
		
		if (priority < Thread.MIN_PRIORITY || priority > Thread.MAX_PRIORITY) {
			throw new IllegalArgumentException(
					"priority: " + priority + " (expected: Thread.MIN_PRIORITY <= priority <= Thread.MAX_PRIORITY)");
		}
		prefix = poolName + '-' + poolId.incrementAndGet() + '-';
		this.deamon = deamon;
		this.priority = priority;
		this.threadGroup = threadGroup;
	}
	
	
	@Override
	public Thread newThread(Runnable r) {
		Thread t = newThread(FastThreadLocalRunnable.wrap(r), prefix + nextId.incrementAndGet());
		
		try {
			if (t.isDaemon() != deamon) {
				t.setDaemon(deamon);
			}
			if (t.getPriority() != priority) {
				t.setPriority(priority);
			}
		} catch (Exception ignored) {
			
		}
		return t;
	}
	
	protected Thread newThread(Runnable r, String name) {
		return new FastThreadLocalThread(threadGroup, r, name);
	}
	
}
