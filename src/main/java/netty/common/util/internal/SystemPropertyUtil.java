package netty.common.util.internal;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static netty.common.util.internal.ObjectUtil.checkNonEmpty;

import java.security.AccessController;
import java.security.PrivilegedAction;

@SuppressWarnings("removal")
public final class SystemPropertyUtil {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(SystemPropertyUtil.class);
	
	public static boolean contains(String key) {
		return get(key) != null;
	}
	
	public static String get(String key) {
		return get(key, null);
	}
	
	@SuppressWarnings({ "deprecation" })
	public static String get(final String key, String def) {
		checkNonEmpty(key, "key");
		
		String value = null;
		try {
			if (System.getSecurityManager() == null) {
				value = System.getProperty(key);
			} else {
				value = AccessController.doPrivileged(new PrivilegedAction<String>() {
					@Override
					public String run() {
						return System.getProperty(key);
					}
				});
			}
		} catch (SecurityException e) {
			logger.warn("Unable to retrieve a system property '{}'; default values will be used.", key, e);
		}
		
		if (value == null) {
			return def;
		}
		
		return value;
	}
	
	public static boolean getBoolean(String key, boolean def) {
		String value = get(key);
		if (value == null) {
			return def;
		}
		
		value = value.trim().toLowerCase();
		if (value.isEmpty()) {
			return def;
		}
		
		if ("true".equals(value) || "yes".equals(value) || "1".equals(value)) {
			return true;
		}
		
		if ("false".equals(value) || "no".equals(value) || "0".equals(value) ) {
			return false;
		}
		
		logger.warn(
				"Unable to parse the boolean system property '{}':{} - using the default value: {}",
				key, value, def
		);
		
		return def;
	}
	
	public static int getInt(String key, int def) {
		String value = get(key);
		if (value == null) {
			return def;
		}
		value = value.trim();
		try {
			return Integer.parseInt(value);
		} catch (Exception e) {
			
		}
		logger.warn(
				"Unable to parse the integer system property '{}':{} - using the default value: {}",
				key, value, def
				);
		return def;
	}
	
	public static long getLong(String key, long def) {
		String value = get(key);
		if (value == null) {
			return def;
		}
		
		value = value.trim();
		try {
			return Long.parseLong(value);
		} catch (Exception e) {}
		
		logger.warn(
				"Unable to parse the long integer system property '{}':{} - using the default value: {}", 
				key, value, def
		);
		return def;
	}
	
	private SystemPropertyUtil() {}
} 
