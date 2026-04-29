package netty.buffer;

import static netty.common.util.internal.MathUtil.isOutOfBounds;
import static netty.common.util.internal.ObjectUtil.checkNotNull;
import static netty.common.util.internal.ObjectUtil.checkPositiveOrZero;
import static netty.common.util.internal.StringUtil.NEWLINE;
import static netty.common.util.internal.StringUtil.isSurrogate;

import java.nio.ByteOrder;

import netty.common.util.CharsetUtil;
import netty.common.util.IllegalReferenceCountException;
import netty.common.util.concurrent.FastThreadLocal;
import netty.common.util.concurrent.FastThreadLocalThread;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.StringUtil;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public final class ByteBufUtil {
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(ByteBufUtil.class);
	private static final FastThreadLocal<byte[]> BYTE_ARRAYS = new FastThreadLocal<byte[]>() {
		@Override
		protected byte[] initialValue() throws Exception {
			return PlatformDependent.allocateUninitializedArray(MAX_TL_ARRAY_LEN);
		}
	};
	
	private static final byte WRITE_UTF_UNKNOWN = (byte) '?';
	private static final int MAX_CHAR_BUFFER_SIZE;
	private static final int THREAD_LOCAL_BUFFER_SIZE;
	private static final int MAX_BYTES_PER_CHAR_UTF8 =
			(int) CharsetUtil.encoder(CharsetUtil.UTF_8).maxBytesPerChar();
	
	static final int WRITE_CHUNK_SIZE = 8192;
	static final ByteBufAllocator DEFAULT_ALLOCATOR;
	private static final boolean SWAR_UNALIGNED = PlatformDependent.canUnalignedAccess();
	
	static {
		String allocType = SystemPropertyUtil.get(
				"io.netty.allocator.type", "adaptive");
		ByteBufAllocator alloc;
		if ("unpooled".equals(allocType)) {
			alloc = UnpooledByteBufAllocator.DEFAULT;
			logger.debug("-Dio.netty.allocator.type: {}", allocType);
		} else if ("pooled".equals(allocType)) {
			alloc = PooledByteBufAllocator.DEFAULT;
			logger.debug("-Dio.netty.allocator.type: {}", allocType);
		} else if ("adaptive".equals(allocType)) {
			alloc = new AdaptiveByteBufAllocator();
			logger.debug("-Dio.netty.allocator.type: {}", allocType);
		} else {
			alloc = PooledByteBufAllocator.DEFAULT;
			logger.debug("-Dio.netty.allocator.type: {}", allocType);
		}
		
		DEFAULT_ALLOCATOR = alloc;
		
		THREAD_LOCAL_BUFFER_SIZE = SystemPropertyUtil.getInt("io.netty.threadLocalDirectBufferSize", 0);
		logger.debug("-Dio.netty.threadLocalDirectBufferSize: {}", THREAD_LOCAL_BUFFER_SIZE);
		
		MAX_CHAR_BUFFER_SIZE = SystemPropertyUtil.getInt("io.netty.maxThreadLocalCharBufferSize", 16 * 1024);
		logger.debug("-Dio.netty.maxThreadLocalCharBufferSize: {}", MAX_CHAR_BUFFER_SIZE);
	}
	
	static final int MAX_TL_ARRAY_LEN = 1024;
	
	static byte[] threadLocalTempArray(int minLength) {
		if (minLength <= MAX_TL_ARRAY_LEN && FastThreadLocalThread.currentThreadHasFastThreadLocal()) {
			return BYTE_ARRAYS.get();
		}
		return PlatformDependent.allocateUninitializedArray(minLength);
	}
	
	public static boolean isAccessible(ByteBuf buffer) {
		return buffer.isAccessible();
	}
	
	public static ByteBuf ensureAccessible(ByteBuf buffer) {
		if (!buffer.isAccessible()) {
			throw new IllegalReferenceCountException(buffer.refCnt());
		}
		return buffer;
	}
	
	public static String hexDump(ByteBuf buffer) {
		return hexDump(buffer, buffer.readerIndex(), buffer.readableBytes());
	}
	
	public static String hexDump(ByteBuf buffer, int fromIndex, int length) {
		return HexUtil.hexDump(buffer, fromIndex, length);
	}
	
	public static String hexDump(byte[] array) {
		return hexDump(array, 0, array.length);
	}
	
	public static String hexDump(byte[] array, int fromIndex, int length) {
		return HexUtil.hexDump(array, fromIndex, length);
	}
	
	public static byte decodeHexByte(CharSequence s, int pos) {
		return StringUtil.decodeHexByte(s, pos);
	}
	
	public static byte[] decodeHexDump(CharSequence hexDump) {
		return StringUtil.decodeHexDump(hexDump, 0, hexDump.length());
	}
	
	public static byte[] decodeHexDump(CharSequence hexDump, int fromIndex, int length) {
		return StringUtil.decodeHexDump(hexDump, fromIndex, length);
	}
	
	public static boolean ensureWritableSuccess(int ensureWritableResult) {
		return ensureWritableResult == 0 || ensureWritableResult == 2;
	}
	
	public static int hashCode(ByteBuf buffer) {
		final int aLen = buffer.readableBytes();
		final int intCount = aLen >>> 2;
		final int byteCount = aLen & 3;
		
		int hashCode = EmptyByteBuf.EMPTY_BYTE_BUF_HASH_CODE;
		int arrayIndex = buffer.readerIndex();
		if (buffer.order() == ByteOrder.BIG_ENDIAN) {
			for (int i = intCount; i > 0; i--) {
				hashCode = 31 * hashCode + buffer.getInt(arrayIndex);
				arrayIndex += 4;
			}
		} else {
			for (int i = intCount; i > 0;  i--) {
				hashCode = 31 * hashCode + swapInt(buffer.getInt(arrayIndex));
				arrayIndex += 4;
			}
		}
		
		for (int i = byteCount; i > 0; i--) {
			hashCode = 31 * hashCode + buffer.getByte(arrayIndex++);
		}
		if (hashCode == 0) {
			hashCode = -1;
		}
		return hashCode;
	}
	
	public static int indexOf(ByteBuf needle, ByteBuf haystack) {
		if (haystack == null || needle == null) {
			return -1;
		}
		
		if (needle.readableBytes() > haystack.readableBytes()) {
			return -1;
		}
		
		int n = haystack.readableBytes();
		int m = needle.readableBytes();
		if (m == 0) {
			return 0;
		}
		if (m == 1) {
			return haystack.indexOf(haystack.readerIndex(), haystack.writerIndex(),
					needle.getByte(needle.readerIndex()));
		}
		
		int i;
		int j = 0;
		int aStartIndex = needle.readerIndex();
		int bStartIndex = haystack.readerIndex();
		long suffixes = maxSuf(needle, m, aStartIndex, true);
		long prefixes = maxSuf(needle, m, aStartIndex, false);
		int ell = Math.max((int) (suffixes >> 32), (int) (prefixes >> 32));
		int per = Math.max((int) suffixes, (int) prefixes);
		int memory;
		int length = Math.min(m - per, ell + 1);
		
		if (equals(needle, aStartIndex, needle, aStartIndex + per, length)) {
			memory = -1;
			while (j <= n - m) {
				i = Math.max(ell, memory) + 1;
				while (i < m && needle.getByte(i + aStartIndex) == haystack.getByte(i + j + bStartIndex)) {
					++i;
				}
				if (i > n) {
					return -1;
				}
				if (i >= m) {
					i = ell;
					while (i > memory && needle.getByte(i + aStartIndex) == haystack.getByte(i + j + bStartIndex)) {
						--i;
					}
					if (i <= memory) {
						return j + bStartIndex;
					}
					j += per;
					memory = m - per - 1;
				} else {
					j += i - ell;
					memory = -1;
				}
			}
		} else {
			per = Math.max(ell + 1, m - ell - 1) + 1;
			while (j <= n - m) {
				i = ell + 1;
				while (i < m && needle.getByte(i + aStartIndex) == haystack.getByte(i + j + bStartIndex)) {
					++i;
				}
				if (i > n) {
					return -1;
				}
				if (i >= m) {
					i = ell;
					while (i >= 0 && needle.getByte(i + aStartIndex) == haystack.getByte(i + j + bStartIndex)) {
						--i;
					}
					if (i < 0) {
						return j + bStartIndex;
					}
					j += per;
					
				} else {
					j += i - ell;
				}
			}
		}
		return -1;
	}
	
	private static long maxSuf(ByteBuf x, int m, int start, boolean isSuffix) {
		int p = 1;
		int ms = -1;
		int j = start;
		int k = 1;
		byte a;
		byte b;
		while (j + k < m) {
			a = x.getByte(j + k);
			b = x.getByte(ms + k);
			boolean suffix = isSuffix ? a < b : a > b;
			if (suffix) {
				j += k;
				k = 1;
				p = j -ms;
			} else if (a == b) {
				if (k != p) {
					++k;
				} else {
					j += p;
					k = 1;
				}
			} else {
				ms = j;
				j = ms + 1;
				k = p = 1;
			}
		}
		return ((long) ms << 32) + p;
	}
	
	public static boolean equals(ByteBuf a, int aStartIndex, ByteBuf b, int bStartIndex, int length) {
		checkNotNull(a, "a");
		checkNotNull(b, "b");
		checkPositiveOrZero(aStartIndex, "aStartIndex");
		checkPositiveOrZero(bStartIndex, "bStartIndex");
		checkPositiveOrZero(length, "length");
		
		if (a.writerIndex() - length < aStartIndex || b.writerIndex() - length < bStartIndex) {
			return false;
		}
		
		final int longCount = length >>> 3;
		final int byteCount = length & 7;
		
		if (a.order() == b.order()) {
			for (int i = longCount; i > 0; i --) {
				if (a.getLong(aStartIndex) != b.getLong(bStartIndex)) {
					return false;
				}
				aStartIndex += 8;
				bStartIndex += 8;
			}
		} else {
			for (int i = longCount; i > 0; i --) {
				if (a.getLong(bStartIndex) != swapLong(b.getLong(bStartIndex))) {
					return false;
				}
				aStartIndex += 8;
				bStartIndex += 8;
			}
		}
		
		for (int i = byteCount; i > 0; i--) {
			if (a.getByte(aStartIndex) != b.getByte(bStartIndex)) {
				return false;
			}
			aStartIndex ++;
			bStartIndex ++;
		}
		return true;
	}
	
	public static boolean equals(ByteBuf bufferA, ByteBuf bufferB) {
		if (bufferA == bufferB) {
			return true;
		}
		final int aLen = bufferA.readableBytes();
		if (aLen != bufferB.readableBytes()) {
			return false;
		}
		return equals(bufferA, bufferA.readerIndex(), bufferB, bufferB.readerIndex(), aLen);
	}
	
	public static int compare(ByteBuf bufferA, ByteBuf bufferB) {
		if (bufferA == bufferB) {
			return 0;
		}
		final int aLen = bufferA.readableBytes();
		final int bLen = bufferB.readableBytes();
		final int minLength = Math.min(aLen, bLen);
		final int uintCount = minLength >>> 2;
		final int byteCount = minLength & 3;
		int aIndex = bufferA.readerIndex();
		int bIndex = bufferB.readerIndex();
		
		if (uintCount > 0) {
			boolean bufferAIsBigEndian = bufferA.order() == ByteOrder.BIG_ENDIAN;
			final long res;
			int uintCountIncrement = uintCount << 2;
			
			if (bufferA.order() == bufferB.order()) {
				res = bufferAIsBigEndian ? compareUintBigEndian(bufferA, bufferB, aIndex, bIndex, uintCountIncrement) :
					compareUintLittleEndian(bufferA, bufferB, aIndex, bIndex, uintCountIncrement);
			} else {
				res = bufferAIsBigEndian ? compareUintBigEndianA(bufferA, bufferB, aIndex, bIndex, uintCountIncrement) :
					compareUintBigEndianB(bufferA, bufferB, aIndex, bIndex, uintCountIncrement);
			}
			if (res != 0) {
				return (int) Math.min(Integer.MAX_VALUE, Math.max(Integer.MIN_VALUE, res));
			}
			aIndex += uintCountIncrement;
			bIndex += uintCountIncrement;
		}
		
		for (int aEnd = aIndex + byteCount; aIndex < aEnd; ++aIndex, ++bIndex) {
			int comp = bufferA.getUnsignedByte(aIndex) - bufferB.getUnsignedByte(bIndex);
			if (comp != 0) {
				return comp;
			}
		}
		return aLen - bLen;
	}
	
	private static long compareUintBigEndian(
			ByteBuf bufferA, ByteBuf bufferB, int aIndex, int bIndex, int uintCountIncrement) {
		for (int aEnd = aIndex + uintCountIncrement; aIndex < aEnd; aIndex += 4, bIndex += 4) {
			long comp = bufferA.getUnsignedInt(aIndex) - bufferB.getUnsignedInt(bIndex);
			if (comp != 0) {
				return comp;
			}
		} 
		return 0;
	}
	
	private static long compareUintLittleEndian(
			ByteBuf bufferA, ByteBuf bufferB, int aIndex, int bIndex, int uintCountIncrement) {
		for (int aEnd = aIndex + uintCountIncrement; aIndex < aEnd; aIndex += 4, bIndex += 4) {
			long comp = uintFromLE(bufferA.getUnsignedInt(aIndex)) - uintFromLE(bufferB.getUnsignedInt(bIndex));
			if (comp != 0) {
				return comp;
			}			
		}
		return 0;
	}
	
	private static long compareUintBigEndianA(
			ByteBuf bufferA, ByteBuf bufferB, int aIndex, int bIndex, int uintCountIncrement) {
		for (int aEnd = aIndex + uintCountIncrement; aIndex < aEnd; aIndex += 4, bIndex += 4) {
			long a = bufferA.getUnsignedInt(aIndex);
			long b = uintFromLE(bufferB.getUnsignedInt(bIndex));
			long comp = a - b;
			if (comp != 0) {
				return comp;
			}
		}
		return 0;
	}
	
	private static long compareUintBigEndianB(
			ByteBuf bufferA, ByteBuf bufferB, int aIndex, int bIndex, int uintCountIncrement) {
		for (int aEnd = aIndex + uintCountIncrement; aIndex < aEnd; aIndex += 4, bIndex += 4) {
			long a = uintFromLE(bufferA.getUnsignedInt(aIndex));
			long b = bufferB.getUnsignedInt(bIndex);
			long comp = a - b;
			if (comp != 0) {
				return comp;
			}
		}
		return 0;
	}
	
	private static long uintFromLE(long value) {
		return Long.reverseBytes(value) >>> Integer.SIZE;
	}
	
	
}
