package netty.common.util.internal;

public final class MathUtil {
	private MathUtil() {}
	
	public static int findNextPositivePowerOfTwo(final int value) {
		assert value > Integer.MIN_VALUE && value < 0x40000000;
		return 1 << (Integer.numberOfLeadingZeros(value - 1));
	}
	
	public static int safeFindNextPositivePowerOfTwo(final int value) {
		return value <= 0 ? 1 : value >= 0x40000000 ? 0x40000000 : findNextPositivePowerOfTwo(value);
	}
	
	public static boolean isOutOfBounds(int index, int length, int capacity) {
		return (index | length | capacity | index + length) < 0 || index + length > capacity; 			
	}
	
	public static int compare(final int x, final int y) {
		return Integer.compare(x, y);
	}
	
	public static int compare(long x, long y) {
		return Long.compare(x, y);
	}
}
