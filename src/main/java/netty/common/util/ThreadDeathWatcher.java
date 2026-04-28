package netty.common.util;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import netty.common.util.concurrent.DefaultThreadFactory;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.StringUtil;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public final class ThreadDeathWatcher {
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(ThreadDeathWatcher.class);
	
	static final ThreadFactory threadFactory;
	
	private static final Queue<Entry> pendingEntries = new ConcurrentLinkedQueue<Entry>();
	private static final Watcher watcher = new Watcher();
	private static final AtomicBoolean started = new AtomicBoolean();
	private static volatile Thread watcherThread;
	
	static {
		String poolName = "threadDeathWatcher";
		String serviceThreadPrefix = SystemPropertyUtil.get("io.netty.serviceThreadPrefix");
		if (!StringUtil.isNullOrEmpty(serviceThreadPrefix)) {
			poolName = serviceThreadPrefix + poolName;
		}
		threadFactory = new DefaultThreadFactory(poolName, true, Thread.MIN_PRIORITY, null);
	}
	
	public static void watch(Thread thread, Runnable task) {
		ObjectUtil.checkNotNull(thread, "thread");
		ObjectUtil.checkNotNull(task, "task");
		
		if (!thread.isAlive()) {
			throw new IllegalArgumentException("thread must be alive.");
		}
		
		schedule(thread, task, true);
	}
	
	public static void unwatch(Thread thread, Runnable task) {
		schedule(ObjectUtil.checkNotNull(thread, "thread"),
				ObjectUtil.checkNotNull(task, "task"),
				false);
	}
	
	private static void schedule(Thread thread, Runnable task, boolean isWatch) {
		pendingEntries.add(new Entry(thread, task, isWatch));
		
		if (started.compareAndSet(false, true)) {
			final Thread watcherThread = threadFactory.newThread(watcher);
			
			AccessController.doPrivileged(new PrivilegedAction<Void>() {
				@Override
				public Void run() {
					watcherThread.setContextClassLoader(null);
					return null;
				}
			});
			
			watcherThread.start();
			ThreadDeathWatcher.watcherThread = watcherThread;
		}
	}
	
	public static boolean awaitInactivity(long timeout, TimeUnit unit) throws InterruptedException {
		ObjectUtil.checkNotNull(unit, "unit");
		
		Thread watcherThread = ThreadDeathWatcher.watcherThread;
		if (watcherThread != null) {
			watcherThread.join(unit.toMillis(timeout));
			return !watcherThread.isAlive();
		} else {
			return true;
		}
	}
	
	private ThreadDeathWatcher() { }
	
	private static final class Watcher implements Runnable {
		
		private final List<Entry> watchees = new ArrayList<Entry>();
		
		@Override
		public void run() {
			for (;;) {
				fetchWatchees();
				notifyWatchees();
				
				fetchWatchees();
				notifyWatchees();
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ignore) {
					
				}
				
				if (watchees.isEmpty() && pendingEntries.isEmpty()) {
					boolean stopped = started.compareAndSet(true, false);
					assert stopped;
					
					if (pendingEntries.isEmpty()) {
						break;
					}
					
					if (!started.compareAndSet(false, true)) {
						break;
					}
				}
			}
		}
		
		private void fetchWatchees() {
			for (;;) {
				Entry e = pendingEntries.poll();
				if (e == null) {
					break;
				}
				if (e.isWatch) {
					watchees.add(e);
				} else {
					watchees.remove(e);
				}
			}
		}
		
		private void notifyWatchees() {
			List<Entry> watchees = this.watchees;
			for (int i = 0; i < watchees.size();) {
				Entry e = watchees.get(i);
				if (!e.thread.isAlive()) {
					watchees.remove(i);
					try {
						e.task.run();
					} catch (Throwable t) {
						logger.warn("Thread death watcher task raised an exception:", t);
					}
				} else {
					i++;
				}
			}
		}
	}
	
	private static final class Entry {
		final Thread thread;
		final Runnable task;
		final boolean isWatch;
		
		Entry(Thread thread, Runnable task, boolean isWatch) {
			this.thread = thread;
			this.task = task;
			this.isWatch = isWatch;
		}
		
		@Override
		public int hashCode() {
			return thread.hashCode() ^ task.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (!(obj instanceof Entry)) {
				return false;
			}
			Entry that = (Entry) obj;
			return thread == that.thread && task == that.task;
		}
	}
}
