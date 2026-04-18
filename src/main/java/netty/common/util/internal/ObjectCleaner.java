package netty.common.util.internal;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ObjectCleaner {
	private static final int REFERENCE_QUEUE_POLL_TIMEOUT_MS =
			Math.max(500, SystemPropertyUtil.getInt("io.netty.util.internal.ObjectCleaner.refQueuePollTimeout", 10000));
	
	static final String CLEANER_THREAD_NAME = ObjectCleaner.class.getSimpleName() + "Thread";
	private static final Set<AutomaticCleanerReference> LIVE_SET = ConcurrentHashMap.newKeySet();
	
	
}
