package netty.common.util;

import org.jetbrains.annotations.Nullable;

public interface ResourceLeakTracker<T> {
	
	void record();
	
	void record(Object hint);
	
	boolean close(T trackedObject);
	
	default @Nullable Throwable getCloseStackTraceIfAny() {
		return null;
	}
}
