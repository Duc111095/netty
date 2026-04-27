package netty.common.util.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import netty.common.util.internal.ObjectUtil;

public final class AutoScalingEventExecutorChooserFactory implements EventExecutorChooserFactory {
	
	public static final class AutoScalingUtilizationMetric {
		private final EventExecutor executor;
		private final AtomicLong utilizationBits = new AtomicLong();
		
		AutoScalingUtilizationMetric(EventExecutor executor) {
			this.executor = executor;
		}
		
		public double utilization() {
			return Double.longBitsToDouble(utilizationBits.get());
		}
		
		public EventExecutor executor() {
			return executor;
		}
		
		void setUtilization(double utilization) {
			long bits = Double.doubleToLongBits(utilization);
			utilizationBits.lazySet(bits);
		}
	}
	
	private static final Runnable NO_OOP_TASK = () -> {};
	private final int minChildren;
	private final int maxChildren;
	private final long utilizationCheckPeriodNanos;
	private final double scaleDownThreshold;
	private final double scaleUpThreshold;
	private final int maxRampUpStep;
	private final int maxRampDownStep;
	private final int scalingPatienceCycles;
	
	public AutoScalingEventExecutorChooserFactory(int minThreads, int maxThreads, long utilizationWindow,
			TimeUnit windowUnit, double scaleDownThreshold,
			double scaleUpThreshold, int maxRampUpStep, int maxRampDownStep,
			int scalingPatienceCycles) {
		minChildren = ObjectUtil.checkPositiveOrZero(minThreads, "minThreads");
		maxChildren = ObjectUtil.checkPositive(maxThreads, "maxThreads");
		if (minThreads > maxThreads) {
			throw new IllegalArgumentException(String.format(
					"minThreads: %d must not be greater that maxThreads: %d", minThreads, maxThreads));
		}
		utilizationCheckPeriodNanos = ObjectUtil.checkNotNull(windowUnit, "windowUnit")
													.toNanos(ObjectUtil.checkPositive(utilizationWindow, 
															"utilizationWindow"));
		this.scaleDownThreshold = ObjectUtil.checkInRange(scaleDownThreshold, 0.0, 1.0, "scaleDownThreshold");
		this.scaleUpThreshold = ObjectUtil.checkInRange(scaleUpThreshold, 0.0, 1.0, "scaleUpThreshold");
		if (scaleDownThreshold >= scaleUpThreshold) {
			throw new IllegalArgumentException(
					"scaleDownThreshold must be less than scaleUpThreshold: " +
					scaleDownThreshold + " >= " + scaleUpThreshold);
		}
		this.maxRampUpStep = ObjectUtil.checkPositive(maxRampUpStep, "maxRampUpStep");
		this.maxRampDownStep =  ObjectUtil.checkPositive(maxRampDownStep, "maxRampDownStep");
		this.scalingPatienceCycles = ObjectUtil.checkPositiveOrZero(scalingPatienceCycles, "scalingPatienceCycles");
	}
	
	@Override
	public EventExecutorChooser newChooser(EventExecutor[] executors) {
		return new AutoScalingEventExecutorChooser(executors);
	}
	
	private static final class AutoScalingState {
		final int activeChildrenCount;
		final long nextWakeUpIndex;
		final EventExecutor[] activeExecutors;
		final EventExecutorChooser activeExecutorChooser;
		
		AutoScalingState(int activeChildrenCount, long nextWakeUpIndex, EventExecutor[] activeExecutors) {
			this.activeChildrenCount = activeChildrenCount;
			this.nextWakeUpIndex = nextWakeUpIndex;
			this.activeExecutors = activeExecutors;
			activeExecutorChooser = DefaultEventExecutorChooserFactory.INSTANCE.newChooser(activeExecutors);
		}
	}
	
	private final class AutoScalingEventExecutorChooser implements ObservableEventExecutorChooser {
		private final EventExecutor[] executors;
		private final EventExecutorChooser allExecutorsChooser;
		private final AtomicReference<AutoScalingState> state;
		private final List<AutoScalingUtilizationMetric> utilizationMetrics;
		
		AutoScalingEventExecutorChooser(EventExecutor[] executors) {
			this.executors = executors;
			List<AutoScalingUtilizationMetric> metrics = new ArrayList<>(executors.length);
			for (EventExecutor executor : executors) {
				metrics.add(new AutoScalingUtilizationMetric(executor));
			}
			utilizationMetrics = Collections.unmodifiableList(metrics);
			allExecutorsChooser = DefaultEventExecutorChooserFactory.INSTANCE.newChooser(executors);
			
			AutoScalingState initialState = new AutoScalingState(maxChildren, 0L, executors);
			state = new AtomicReference<>(initialState);
			
			ScheduledFuture<?> utilizationMonitoringTask = GlobalEventExecutor.INSTANCE.scheduleAtFixedRate(
					new UtilizationMonitor(), utilizationCheckPeriodNanos, utilizationCheckPeriodNanos, 
					TimeUnit.NANOSECONDS);
			
			if (executors.length > 0) {
				executors[0].terminationFuture().addListener(future -> utilizationMonitoringTask.cancel(false));
			}
		}
		
		@Override
		public EventExecutor next() {
			AutoScalingState currentState = this.state.get();
			
			if (currentState.activeExecutors.length == 0) {
				tryScaleUpBy(1);
				return allExecutorsChooser.next();
			}
			return currentState.activeExecutorChooser.next();
		}
		
		private void tryScaleUpBy(int amount) {
			if (amount <= 0) {
				return;
			}
			
			for (;;) {
				AutoScalingState oldState = state.get();
				if (oldState.activeChildrenCount >= maxChildren) {
					return;
				}
				int canAdd = Math.min(amount, maxChildren - oldState.activeChildrenCount);
				List<EventExecutor> wokenUp = new ArrayList<>(canAdd);
				final long startIndex = oldState.nextWakeUpIndex;
				
				for (int i = 0; i < executors.length; i++) {
					EventExecutor child = executors[(int) (Math.abs((startIndex + i) % executors.length))];
					if (wokenUp.size() >= canAdd) {
						break;
					}
					if (child instanceof SingleThreadEventExecutor) {
						SingleThreadEventExecutor stee = (SingleThreadEventExecutor) child;
						if (stee.isSuspended()) {
							stee.execute(NO_OOP_TASK);
							wokenUp.add(stee);
						}
					} 
				}
				
				if (wokenUp.isEmpty()) {
					return;
				}
				
				List<EventExecutor> newActiveList = new ArrayList<>(oldState.activeExecutors.length + wokenUp.size());
				Collections.addAll(newActiveList, oldState.activeExecutors);
				newActiveList.addAll(wokenUp);
				
				AutoScalingState newState = new AutoScalingState(
						oldState.activeChildrenCount + wokenUp.size(),
						startIndex + wokenUp.size(),
						newActiveList.toArray(new EventExecutor[0]));
				
				if (state.compareAndSet(oldState, newState)) {
					return;
				}
			}
		}
		
		@Override
		public int activeExecutorCount() {
			return state.get().activeChildrenCount;
		}
		
		@Override
		public List<AutoScalingUtilizationMetric> executorUtilizations() {
			return utilizationMetrics;
		}
		
		private final class UtilizationMonitor implements Runnable {
			private final List<SingleThreadEventExecutor> consistentlyIdleChildren = new ArrayList<>(maxChildren);
			private long lastCheckTimeNanos;
			
			@Override
			public void run() {
				if (executors.length == 0 || executors[0].isShuttingDown()) {
					return;
				}
				
				final long now = executors[0].ticker().nanoTime();
				long totalTime;
				
				if (lastCheckTimeNanos == 0) {
					totalTime = utilizationCheckPeriodNanos;
				} else {
					totalTime = now - lastCheckTimeNanos;
				}
				
				lastCheckTimeNanos = now;
				
				if (totalTime <= 0) {
					return;
				}
				
				int consistentlyBusyChildren = 0;
				consistentlyIdleChildren.clear();
				
				final AutoScalingState currentState = state.get();
				
				for (int i = 0; i < executors.length; i++) {
					EventExecutor child = executors[i];
					if (!(child instanceof SingleThreadEventExecutor)) {
						continue;
					}
					
					SingleThreadEventExecutor eventExecutor = (SingleThreadEventExecutor) child;
					
					double utilization = 0.0;
					if (!eventExecutor.isSuspended()) {
						long activeTime = eventExecutor.getAndResetAccumulatedActiveTimeNanos();
						
						if (activeTime == 0) {
							long lastActivity = eventExecutor.getLastActivityTimeNanos();
							long idleTime = now - lastActivity;
							
							if (idleTime < totalTime) {
								activeTime = totalTime - idleTime;
							}
						}
						
						utilization = Math.min(1.0, (double) activeTime / totalTime);
						
						if (utilization < scaleDownThreshold) {
							int idleCycles = eventExecutor.getAndIncrementIdleCycles();
							if (idleCycles >= scalingPatienceCycles &&
									eventExecutor.getNumOfRegisteredChannels() <= 0) {
								consistentlyIdleChildren.add(eventExecutor);
							}
						} else if (utilization > scaleUpThreshold) {
							int busyCycles = eventExecutor.getAndIncrementBusyCycles();
							eventExecutor.resetIdleCycles();
							if (busyCycles >= scalingPatienceCycles) {
								consistentlyBusyChildren++;
							}
						} else {
							eventExecutor.resetIdleCycles();
							eventExecutor.resetBusyCycles();
						}
					}
					
					utilizationMetrics.get(i).setUtilization(utilization);
				}
				
				int currentActive = currentState.activeChildrenCount;
				
				if (consistentlyBusyChildren > 0 && currentActive < maxChildren) {
					int threadsToAdd = Math.min(consistentlyBusyChildren, maxRampUpStep);
					threadsToAdd = Math.min(threadsToAdd, maxChildren - currentActive);
					if (threadsToAdd > 0) {
						tryScaleUpBy(threadsToAdd);
						
						return;
					}
				}
				
				boolean changed = false;
				if (!consistentlyIdleChildren.isEmpty() && currentActive > minChildren) {
					int threadsToRemove = Math.min(consistentlyIdleChildren.size(), maxRampDownStep);
					threadsToRemove = Math.min(threadsToRemove, currentActive - minChildren);
					
					for (int i = 0; i < threadsToRemove; i++) {
						SingleThreadEventExecutor childToSuspend = consistentlyIdleChildren.get(i);
						if (childToSuspend.trySuspend()) {
							childToSuspend.resetBusyCycles();
							childToSuspend.resetIdleCycles();
							changed = true;
						}
					}
				}
				
				if (changed || currentActive != currentState.activeExecutors.length) {
					rebuildActiveExecutors();
				}
			}
			
			private void rebuildActiveExecutors() {
				for (;;) {
					AutoScalingState oldState = state.get();
					List<EventExecutor> active = new ArrayList<>(oldState.activeChildrenCount);
					for (EventExecutor executor : executors) {
						if (!executor.isSuspended()) {
							active.add(executor);
						}
					}
					EventExecutor[] newActiveExecutors = active.toArray(new EventExecutor[0]);
					
					AutoScalingState newState = new AutoScalingState(
							newActiveExecutors.length, oldState.nextWakeUpIndex, newActiveExecutors);
					
					if (state.compareAndSet(oldState, newState)) {
						break;
					}
				}
			}
		}
	}
}
