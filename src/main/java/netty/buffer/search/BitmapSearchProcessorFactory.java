package netty.buffer.search;

import netty.common.util.internal.PlatformDependent;

public class BitmapSearchProcessorFactory extends AbstractSearchProcessorFactory {
	private final long[] bitMasks = new long[256];
	private final long successBit;
	
	public static class Processor implements SearchProcessor {
		private final long[] bitMasks;
		private final long successBit;
		private long currentMask;
		
		Processor(long[] bitMasks, long successBit) {
			this.bitMasks = bitMasks;
			this.successBit = successBit;
		}
		
		@Override
		public boolean process(byte value) {
			currentMask = ((currentMask << 1) | 1) & PlatformDependent.getLong(bitMasks, value & 0xffL);
			return (currentMask & successBit) == 0;
		}
		
		@Override
		public void reset() {
			currentMask = 0;
		}
 	}
	
	BitmapSearchProcessorFactory(byte[] needle) {
		if (needle.length > 64) {
			throw new IllegalArgumentException("Maximum supported search pattern length is 64, got " + needle.length);
		}
		long bit = 1L;
		for (byte c : needle) {
			bitMasks[c & 0xff] = bit;
			bit <<= 1;
		}
		
		successBit = 1L << (needle.length - 1);
	}
	
	@Override
	public Processor newSearchProcessor() {
		return new Processor(bitMasks, successBit);
	}
}
