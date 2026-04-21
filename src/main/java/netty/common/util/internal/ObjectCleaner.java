package netty.common.util.internal;

import java.lang.ref.ReferenceQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static netty.common.util.internal.SystemPropertyUtil.getInt;

public final class ObjectCleaner {
	private static final int REFERENCE_QUEUE_POLL_TIMEOUT_MS =
			Math.max(500, SystemPropertyUtil.getInt("io.netty.util.internal.ObjectCleaner.refQueuePollTimeout", 10000));
	
	static final String CLEANER_THREAD_NAME = ObjectCleaner.class.getSimpleName() + "Thread";
	private static final Set<AutomaticCleanerReference> LIVE_SET = ConcurrentHashMap.newKeySet();
	private static final ReferenceQueue<Object> REFERENCE_QUEUE = new ReferenceQueue<>();
	private static final AtomicBoolean CLEANER_RUNNING = new AtomicBoolean(false);
	private static final Runnable CLEANER_TASK = new Runnable() {
		@Override
		public void run() {
			boolean interrupted = false;
			for (;;) {
				while ()
			}
		}
	}
}
