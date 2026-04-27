package netty.common.util.concurrent;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class DefaultEventExecutorChooserFactory implements EventExecutorChooserFactory {
	
	public static final DefaultEventExecutorChooserFactory INSTANCE = new DefaultEventExecutorChooserFactory();
	
	private DefaultEventExecutorChooserFactory() {
		
	}
	
	@Override
	public EventExecutorChooser newChooser(EventExecutor[] executors) {
		if (isPowerOfTwo(executors.length)) {
			return new PowerOfTwoEventExecutorChooser(executors);
		} else {
			return new GenericEventExecutorChooser(executors);
		}
	}
	
	private static boolean isPowerOfTwo(int val) {
		return (val & -val) == val;
	}
	
	private static final class PowerOfTwoEventExecutorChooser implements EventExecutorChooser {
		private final AtomicInteger idx = new AtomicInteger();
		private final EventExecutor[] executors;
		
		PowerOfTwoEventExecutorChooser(EventExecutor[] executors) {
			this.executors = executors;
		}
		
		@Override
		public EventExecutor next() {
			return executors[idx.getAndIncrement() & executors.length - 1];
		}
	}
	
	private static final class GenericEventExecutorChooser implements EventExecutorChooser {
		private final AtomicLong idx = new AtomicLong();
		private final EventExecutor[] executors;
		
		GenericEventExecutorChooser(EventExecutor[] executors) {
			this.executors = executors;
		}
		
		@Override
		public EventExecutor next() {
			return executors[(int) Math.abs(idx.getAndIncrement() % executors.length)];
		}
	}
}
