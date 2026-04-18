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
}
