package netty.common.util.concurrent;

import netty.common.util.internal.ObjectUtil;

final class FastThreadLocalRunnable implements Runnable {

	private final Runnable runnable;
	
	private FastThreadLocalRunnable(Runnable runnable) {
		this.runnable = ObjectUtil.checkNotNull(runnable, "runnable");
	}
	
	@Override
	public void run() {
		try {
			runnable.run();
		} finally {
			FastThreadLocal.removeAll();
		}
	}
	
	static Runnable wrap(Runnable runnable) {
		return  runnable instanceof FastThreadLocalRunnable ? runnable : new FastThreadLocalRunnable(runnable);
	}
	
}
