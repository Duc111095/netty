package netty.common.util;

import java.util.Locale;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.SystemPropertyUtil;

public final class NettyRuntime {

	static class AvailableProcessorsHolder {
		private int availableProcessors;
		
		synchronized void setAvailableProcessors(final int availableProcessors) {
			ObjectUtil.checkPositive(availableProcessors, "availableProcessors");
			if (this.availableProcessors != 0) {
				final String message = String.format(
						Locale.ROOT,
						"availableProcessors is already to set to [%d], rejecting [%d]",
						this.availableProcessors,
						availableProcessors);
				throw new IllegalStateException(message);
			}
			this.availableProcessors = availableProcessors;
		}
		
		@SuppressForbidden(reason = "to obtain default number of available processors")
		synchronized int availableProcessors() {
			if (this.availableProcessors == 0) {
				final int availableProcessors = 
						SystemPropertyUtil.getInt(
								"io.netty.availableProcessors",
								Runtime.getRuntime().availableProcessors());
				setAvailableProcessors(availableProcessors);
			}
			return this.availableProcessors;
		}
	}
	
	private static final AvailableProcessorsHolder holder = new AvailableProcessorsHolder();
	
	public static void setAvailableProcessors(final int availableProcessors) {
		holder.setAvailableProcessors(availableProcessors);
	}
	
	public static int availableProcessors() {
		return holder.availableProcessors;
	}
	
	private NettyRuntime() {
		
	}
}
