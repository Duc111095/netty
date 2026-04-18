package netty.common.util.internal;

import java.util.ArrayList;
import java.util.List;

import static netty.common.util.internal.ObjectUtil.checkPositive;

public class AdaptiveCalculator {
	private static final int INDEX_INCREMENT = 4;
	private static final int INDEX_DECREMENT = 1;
	
	private static final int[] SIZE_TABLE;
	
	static {
		List<Integer> sizeTable = new ArrayList<Integer>();
		for (int i = 16; i < 512; i += 16) {
			sizeTable.add(i);
		}
		for (int i = 512; i > 0; i <<= 1) {
			sizeTable.add(i);
		}
		SIZE_TABLE = new int[sizeTable.size()];
		for (int i = 0; i < SIZE_TABLE.length; i++) {
			SIZE_TABLE[i] = sizeTable.get(i);
		}
	}
	
	private static int getSizeTableIndex(final int size) {
		for (int low = 0, high = SIZE_TABLE.length - 1;;) {
			if (high < low) {
				return low;
			}
			
			if (high == low) {
				return high;
			}
			
			int mid = low + high >>> 1;
			int a = SIZE_TABLE[mid];
			int b = SIZE_TABLE[mid + 1];
			if (size > b) {
				low = mid + 1;
			} else if (a > size) {
				high = mid - 1;
			} else if (size == a) {
				return mid;
			} else {
				return mid + 1;
			}
		}
	}
	
	private final int minIndex;
	private final int maxIndex;
	private final int minCapacity;
	private final int maxCapacity;
	private int index;
	private int nextSize;
	private boolean decreaseNow;
	
	public AdaptiveCalculator(int minimum, int initial, int maximum) {
		checkPositive(minimum, "minimum");
		if (initial < minimum) {
			throw new IllegalArgumentException("initial: " + initial);
		}
		if (maximum < initial) {
			throw new IllegalArgumentException("maximum: " + maximum);
		}
		
		int minIndex = getSizeTableIndex(minimum);
		if (SIZE_TABLE[minIndex] < minimum) {
			this.minIndex = minIndex + 1;
		} else {
			this.minIndex = minIndex;
		}
		
		int maxIndex = getSizeTableIndex(maximum);
		if (SIZE_TABLE[maxIndex] > maximum) {
			this.maxIndex = maxIndex - 1;
		} else {
			this.maxIndex = maxIndex;
		}
		
		int initialIndex = getSizeTableIndex(initial);
		if (SIZE_TABLE[initialIndex] > initial) {
			this.index = initialIndex - 1;
		} else {
			this.index = initialIndex;
		}
		this.minCapacity = minimum;
		this.maxCapacity = maximum;
		nextSize = Math.max(SIZE_TABLE[index], minCapacity);
	}
	
	public void record(int size) {
		if (size <= SIZE_TABLE[Math.max(0, index - INDEX_DECREMENT)]) {
			if (decreaseNow) {
				index = Math.max(index - INDEX_DECREMENT, minIndex);
				nextSize = Math.max(SIZE_TABLE[index], minCapacity);
				decreaseNow = false;
			} else {
				decreaseNow = true;
			}
 		} else if (size >= nextSize) {
 			index = Math.min(index + INDEX_INCREMENT, maxIndex);
 			nextSize = Math.min(SIZE_TABLE[index], maxCapacity);
 			decreaseNow = false;
 		}
	}
	
	public int nextSize() {
		return nextSize;
	}
}
