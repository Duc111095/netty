package netty.common.util.concurrent;

import java.util.List;

import netty.common.util.concurrent.AutoScalingEventExecutorChooserFactory.AutoScalingUtilizationMetric;

public interface EventExecutorChooserFactory {
	
	EventExecutorChooser newChooser(EventExecutor[] executors);
	
	interface EventExecutorChooser {
		EventExecutor next();
	}
	
	interface ObservableEventExecutorChooser extends EventExecutorChooser {
		
		int activeExecutorCount();
		
		List<AutoScalingUtilizationMetric> executorUtilizations();
	}
}
