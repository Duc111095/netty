package netty.channel.nio;

import netty.channel.IoHandler;
import netty.common.util.IntSupplier;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public final class NioIoHandler implements IoHandler {
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioIoHandler.class);
	
	private static final int CLEANUP_INTERVAL = 256;
	
	private static final boolean DISABLE_KEY_SET_OPTIMIZATION = 
			SystemPropertyUtil.getBoolean("io.netty.noKeySetOptimization", false);
	
	private static final int MIN_PREMATURE_SELECTOR_RETURNS = 3;
	private static final int SELECTOR_AUTO_REBUILD_THRESHOLD;
	
	private final IntSupplier selectNonSupplier = new IntSupplier() {
		@Override
		public int get() throws Exception {
			return selectNow();
		}
	};
	
	static {
		int selectorAutoRebuildThreshold = SystemPropertyUtil.getInt("io.netty.selectorAutoRebuildThreshold", 512);
		if (selectorAutoRebuildThreshold < MIN_PREMATURE_SELECTOR_RETURNS) {
			selectorAutoRebuildThreshold = 0;
		}
		
		SELECTOR_AUTO_REBUILD_THRESHOLD = selectorAutoRebuildThreshold;
		
		if (logger.isDebugEnabled()) {
			logger.debug("-Dio.netty.noKeySetOptimization: {}", DISABLE_KEY_SET_OPTIMIZATION);
			logger.debug("-Dio.netty.selectorAutoRebuildThreshold: {}", SELECTOR_AUTO_REBUILD_THRESHOLD);
		}
	}
}
