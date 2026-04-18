package netty.common.util.concurrent;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static netty.common.util.internal.ObjectUtil.checkPositiveOrZero;
import static java.util.Objects.requireNonNull;

final class DefaultMockTicker implements MockTicker {
	
	// The lock is fair. so writers get to process condition signals in the order they queued up.
	private final ReentrantLock lock = new ReentrantLock(true);
	private final Condition tickCondition = lock.newCondition();
	private final Condition sleeperCondition = lock.newCondition();
	private final AtomicLong nanoTime = new AtomicLong();
	private final Set<Thread> sleepers = Collections.newSetFromMap(new IdentityHashMap<>());
	
	@Override
	public long nanoTime() {
		return nanoTime.get();
	}

	@Override
	public void sleep(long delay, TimeUnit unit) throws InterruptedException {
		checkPositiveOrZero(delay, "delay");
		requireNonNull(unit, "unit");
		
		if (delay == 0) {
			return;
		}
		
		final long delayNanos = unit.toNanos(delay);
		lock.lockInterruptibly();
		try {
			final long startTimeNanos = nanoTime();
			sleepers.add(Thread.currentThread());
			sleeperCondition.signalAll();
			do {
				tickCondition.await();
			} while (nanoTime() - startTimeNanos < delayNanos);
		} finally {
			sleepers.remove(Thread.currentThread());
			lock.unlock();
		}
	}

	public void awaitSleepingThread(Thread thread) throws InterruptedException {
		lock.lockInterruptibly();
		try {
			while (!sleepers.contains(thread)) {
				sleeperCondition.await();
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void advance(long amount, TimeUnit unit) {
		checkPositiveOrZero(amount, "amount");
		requireNonNull(unit, "unit");
		
		if (amount == 0) {
			return;
		}
		
		final long amountNanos = unit.toNanos(amount);
		lock.lock();
		try {
			nanoTime.addAndGet(amountNanos);
			tickCondition.signalAll();
		} finally {
			lock.unlock();
		}
	}
}
