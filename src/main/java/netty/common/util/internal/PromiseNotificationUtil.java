package netty.common.util.internal;

import netty.common.util.concurrent.Promise;
import netty.common.util.internal.logging.InternalLogger;

public final class PromiseNotificationUtil {
	private PromiseNotificationUtil() {}
	
	public static void tryCancel(Promise<?> p, InternalLogger logger) {
		if (!p.cancel(false) && logger != null) {
			Throwable err = p.cause();
			if (err == null) {
				logger.warn("Failed to mark a promise as success because it has succeeded already: {}", p);
			} else {
				logger.warn("Failed to mark a promise as success because it has failed already: {}, unnotified cause: ", p, err);
			}
		}
	}
	
	public static void tryFailure(Promise<?> p, Throwable cause, InternalLogger logger) {
		if (!p.tryFailure(cause) && logger != null) {
			Throwable err = p.cause();
			if (err == null) {
				logger.warn("Failed to mark a promise as failed because it has succeeded already: {}", p);
			} else {
				logger.warn("Failed to mark a promise as failed because it has failed already: {}, unnotified cause: ", p, err);
			}
		}
	}
	
	public static <V> void trySuccess(Promise<? super V> p, V result, InternalLogger logger) {
		if (!p.trySuccess(result) && logger != null) {
			Throwable err = p.cause();
			if (err == null) {
				logger.warn("Failed to mark a promise as success because it has succeeded already: {}", p);
			} else {
				logger.warn("Failed to mark a promise as success because it has failed already: {}, unnotified cause: ", p, err);
			}
		}
	}
}
