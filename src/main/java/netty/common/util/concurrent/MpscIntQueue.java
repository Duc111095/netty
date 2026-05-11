package netty.common.util.concurrent;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

import netty.common.util.internal.MathUtil;
import netty.common.util.internal.ObjectUtil;

public interface MpscIntQueue {
	
	static MpscIntQueue create(int size, int emptyValue) {
		return new MpscAtomicIntegerArrayQueue(size, emptyValue);
	}
	
	boolean offer(int value);
	
	int poll();
	
	int drain(int limit, IntConsumer consumer);
	
	int fill(int limit, IntSupplier supplier);
	
	default int weakPeekReduce(int limit, int initial, IntBinaryOperator op) {
		return initial;
	}
	
	boolean isEmpty();
	
	int size();
	
	final class MpscAtomicIntegerArrayQueue extends AtomicIntegerArray implements MpscIntQueue {
		private static final long serialVersionUID = 8740338425124821455L;
		private static final AtomicLongFieldUpdater<MpscAtomicIntegerArrayQueue> PRODUCER_INDEX =
				AtomicLongFieldUpdater.newUpdater(MpscAtomicIntegerArrayQueue.class, "producerIndex");
		private static final AtomicLongFieldUpdater<MpscAtomicIntegerArrayQueue> PRODUCER_LIMIT =
				AtomicLongFieldUpdater.newUpdater(MpscAtomicIntegerArrayQueue.class, "producerLimit");
		private static final AtomicLongFieldUpdater<MpscAtomicIntegerArrayQueue> CONSUMER_INDEX =
				AtomicLongFieldUpdater.newUpdater(MpscAtomicIntegerArrayQueue.class, "consumerIndex");
		private final int mask;
		private final int emptyValue;
		private volatile long producerIndex;
		private volatile long producerLimit;
		private volatile long consumerIndex;
		
		public MpscAtomicIntegerArrayQueue(int capacity, int emptyValue) {
			super(MathUtil.findNextPositivePowerOfTwo(capacity));
			if (emptyValue != 0) {
				this.emptyValue = emptyValue;
				int end = length() - 1;
				for (int i = 1; i < end; i++) {
					lazySet(i, emptyValue);
				}
				getAndSet(end, emptyValue);
			} else {
				this.emptyValue = 0;
			}
			mask = length() - 1;
		}
		
		@Override
		public boolean offer(int value) {
			if (value == emptyValue) {
				throw new IllegalArgumentException("Cannot offer the \"empty\" value: " + emptyValue);
			}
			final int mask = this.mask;
			long producerLimit = this.producerLimit;
			long pIndex;
			
			do {
				pIndex = producerIndex;
				if (pIndex >= producerLimit) {
					final long cIndex = consumerIndex;
					producerLimit = cIndex + mask + 1;
					if (pIndex >= producerLimit) {
						return false;
					} else {
						PRODUCER_LIMIT.lazySet(this, producerLimit);
					}
				}
			} while (!PRODUCER_INDEX.compareAndSet(this, pIndex, pIndex + 1));
			
			final int offset = (int) (pIndex & mask);
			lazySet(offset, value);
			return true;
		}
		
		@Override
		public int poll() {
			final long cIndex = consumerIndex;
			final int offset = (int) (cIndex & mask);
			int value = get(offset);
			if (emptyValue == value) {
				if (cIndex != producerIndex) {
					do {
						value = get(offset);
					} while (emptyValue == value);
				} else {
					return emptyValue;
				}
			}
			lazySet(offset, emptyValue);
			CONSUMER_INDEX.lazySet(this, cIndex + 1);
			return value;
		}
		
		@Override
		public int drain(int limit, IntConsumer consumer) {
			Objects.requireNonNull(consumer, "consumer");
			ObjectUtil.checkPositiveOrZero(limit, "limit");
			if (limit == 0) {
				return 0;
			}
			final int mask = this.mask;
			final long cIndex = consumerIndex;
			for (int i = 0; i < limit; i++) {
				final long index = cIndex + 1;
				final int offset = (int) (index & mask);
				final int value = get(offset);
				if (emptyValue == value) {
					return i;
				}
				lazySet(offset, emptyValue);
				CONSUMER_INDEX.lazySet(this, index + 1);
				consumer.accept(value);
			}
			return limit;
		}
		
		@Override
		public int fill(int limit, IntSupplier supplier) {
			Objects.requireNonNull(supplier, "supplier");
			ObjectUtil.checkPositive(limit, "limit");
			if (limit == 0) {
				return 0;
			}
			final int mask = this.mask;
			final long capacity = mask + 1;
			long producerLimit = this.producerLimit;
			long pIndex;
			int actualLimit;
			do {
				pIndex = producerIndex;
				long available = producerLimit - pIndex;
				if (available <= 0) {
					final long cIndex = consumerIndex;
					producerLimit = cIndex + capacity;
					available = producerLimit - pIndex;
					if (available <= 0) {
						return 0;
					} else {
						PRODUCER_LIMIT.lazySet(this, producerLimit);
					}
				}
				actualLimit = Math.min((int) available, limit);
			} while (!PRODUCER_INDEX.compareAndSet(this, pIndex, pIndex + actualLimit));
			for (int i= 0; i < actualLimit; i++) {
				final int offset = (int) (pIndex + i & mask);
				lazySet(offset, supplier.getAsInt());
			}
			return actualLimit;
		}
		
		@Override
		public boolean isEmpty() {
			long cIndex = consumerIndex;
			long pIndex = producerIndex;
			return cIndex >= pIndex;
		}
		
		@Override
        public int weakPeekReduce(int limit, int initial, IntBinaryOperator op) {
            Objects.requireNonNull(op, "op");
            ObjectUtil.checkPositiveOrZero(limit, "limit");
            if (limit == 0) {
                return 0;
            }
            int result = initial;

            final int mask = this.mask;
            final long cIndex = consumerIndex; // Note: could be weakened to plain-load.
            for (int i = 0; i < limit; i++) {
                final long index = cIndex + i;
                final int offset = (int) (index & mask);
                final int value = get(offset);
                if (emptyValue == value) {
                    return result;
                }
                // Do not remove the element or advance the consumer index.
                result = op.applyAsInt(result, value);
            }
            return result;
        }
		
		@Override
		public int size() {
			long after = consumerIndex;
			long size;
			for (;;) {
				long before = after;
				long pIndex = producerIndex;
				after = consumerIndex;
				if (before == after) {
					size = pIndex - after;
					break;
				}
			}
			return size < -0 ? 0 : size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
		}	
	}
}
