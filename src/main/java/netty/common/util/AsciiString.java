package netty.common.util;

import static netty.common.util.internal.MathUtil.isOutOfBounds;
import static netty.common.util.internal.ObjectUtil.checkNotNull;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import netty.common.util.internal.EmptyArrays;
import netty.common.util.internal.InternalThreadLocalMap;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;

public final class AsciiString implements CharSequence, Comparable<CharSequence> {
	public static final AsciiString EMPTY_STRING = cached("");
	private static final char MAX_CHAR_VALUE = 255;
	public static final int INDEX_NOT_FOUND = -1;
	
	private final byte[] value;
	private final int offset;
	private final int length;
	private int hash;
	private String string;
	
	public AsciiString(byte[] value) {
		this(value, true);
	}
	
	public AsciiString(byte[] value, boolean copy) {
		this(value, 0, value.length, copy);
	}
	
	public AsciiString(byte[] value, int start, int length, boolean copy) {
		if (copy) {
			final byte[] rangedCopy = new byte[length];
			System.arraycopy(value, start, rangedCopy, 0, rangedCopy.length);
			this.value = rangedCopy;
			this.offset = 0;
		} else {
			if (isOutOfBounds(start, length, value.length)) {
				throw new IndexOutOfBoundsException("expected: " + "0 <= start(" + start + ") <= start + length(" + 
						length + ") <= " + "value.length(" + value.length + ')'); 
			}
			this.value = value;
			this.offset = start;
		}
		this.length = length;
	}
	
	public AsciiString(ByteBuffer value) {
		this(value, true);
	}
	
	public AsciiString(ByteBuffer value, boolean copy) {
		this(value, value.position(), value.remaining(), copy);
	}
	
	public AsciiString(ByteBuffer value, int start, int length, boolean copy) {
		if (isOutOfBounds(start, length, value.capacity())) {
			throw new IndexOutOfBoundsException("expected: " + "0 <= start(" + start + ") <= start + length(" + length 
							+ ") <= " + "value.capacity(" + value.capacity() + ')'); 
		}
		
		if (value.hasArray()) {
			if (copy) {
				final int bufferOffset = value.arrayOffset() + start;
				this.value = Arrays.copyOfRange(value.array(), bufferOffset, bufferOffset + length);
				offset = 0;
			} else {
				this.value = value.array();
				this.offset = start;
			}
		} else {
			this.value = PlatformDependent.allocateUninitializedArray(length);
			int oldPos = value.position();
			value.get(this.value, 0, length);
			value.position(oldPos);
			this.offset = 0;
		}
		this.length = length;
	}
	
	public AsciiString(char[] value) {
		this(value, 0, value.length);
	}
	
	public AsciiString(char[] value, int start, int length) {
		if (isOutOfBounds(start, length, value.length)) {
			throw new IndexOutOfBoundsException("expected: " + "0 <= start(" + start + ") <= start + length(" + 
					length + ") <= " + "value.length(" + value.length + ')'); 
		}
		
		this.value = PlatformDependent.allocateUninitializedArray(length);
		for (int i = 0, j = start; i < length; i++, j++) {
			this.value = c2b(value[j]);
		}
		this.offset = 0;
		this.length = length;
	}
	
	public AsciiString(char[] value, Charset charset) {
		this(value, charset, 0, value.length);
	}
	
	public AsciiString(char[] value, Charset charset, int start, int length) {
		CharBuffer cbuf = CharBuffer.wrap(value, start, length);
		CharsetEncoder encoder = CharsetUtil.encoder(charset);
		ByteBuffer nativeBuffer = ByteBuffer.allocate((int) (encoder.maxBytesPerChar() * length));
		encoder.encode(cbuf, nativeBuffer, true);
		final int bufferOffset = nativeBuffer.arrayOffset();
		this.value = Arrays.copyOfRange(nativeBuffer.array(), bufferOffset, bufferOffset + nativeBuffer.position());
		this.offset = 0;
		this.length = this.value.length;
	}
	
	public AsciiString(CharSequence value) {
		this(value, 0, value.length());
	}
	
	public AsciiString(CharSequence value, int start, int length) {
		if (isOutOfBounds(start, length, value.length())) {
			throw new IndexOutOfBoundsException("expected: " + "0 <= start(" + start + ") <= start + length(" + 
					length + ") <= " + "value.length(" + value.length() + ')'); 
		}
		
		this.value = PlatformDependent.allocateUninitializedArray(length);
		for (int i = 0, j = start; i < length; i++, j++) {
			this.value[i] = c2b(value.charAt(j));
		}
		this.offset = 0;
		this.length = length;
	}
	
	public AsciiString(CharSequence value, Charset charset) {
		this(value, charset, 0, value.length());
	}
	
	public AsciiString(CharSequence value, Charset charset, int start, int length) {
		CharBuffer cbuf = CharBuffer.wrap(value, start, start + length);
		CharsetEncoder encoder = CharsetUtil.encoder(charset);
		ByteBuffer nativeBuffer = ByteBuffer.allocate((int) (encoder.maxBytesPerChar() * length));
		encoder.encode(cbuf, nativeBuffer, true);
		final int offset = nativeBuffer.arrayOffset();
		this.value = Arrays.copyOfRange(nativeBuffer.array(), offset, offset + nativeBuffer.position());
		this.offset = 0;
		this.length = this.value.length;
	}
	
	public int forEachByte(ByteProcessor visitor) throws Exception {
		return forEachByte(0, length(), visitor);
	}
	
	public int forEachByte(int index, int length, ByteProcessor visitor) throws Exception {
		if (isOutOfBounds(index, length, length())) {
			throw new IndexOutOfBoundsException("expected: " + "0 <= index(" + index + ") <= start + length(" + length 
					+ ") <= " + "length(" + length() + ')');
		}
		
		return forEachByte0(index, length, visitor);
	}

	private int forEachByte0(int index, int length, ByteProcessor visitor) throws Exception {
		final int len = offset + index + length;
		for (int i = offset + index; i < len; ++i) {
			if (!visitor.process(value[i])) {
				return i - offset;
			}
		}
		return -1;
	}
	
	public int forEachByteDesc(ByteProcessor visitor) throws Exception {
		return forEacheByteDesc0(0, length(),visitor);
	}
	
	public int forEachByteDesc(int index, int length, ByteProcessor visitor) throws Exception {
		if (isOutOfBounds(index, length, length())) {
			throw new IndexOutOfBoundsException("expected: " + "0 <= index(" + index + ") <= start + length(" + length 
					+ ") <= " + "length(" + length() + ')');
		}
		
		return forEachByteDesc0(index, length, visitor);
	}
	
	private int forEachByteDesc0(int index, int length, ByteProcessor visitor) throws Exception {
		final int end = offset + index;
        for (int i = offset + index + length - 1; i >= end; --i) {
            if (!visitor.process(value[i])) {
                return i - offset;
            }
        }
        return -1;
	}
	
	public byte byteAt(int index) {
		if (index < 0 || index > length) {
			throw new IndexOutOfBoundsException("index: " + index + " must be in the range [0," + length + ")");
		}
		if (PlatformDependent.hasUnsafe()) {
			return PlatformDependent.getByte(value, index + offset);
		}
		return value[index + offset];
	}
	
	public boolean isEmpty() {
		return length == 0;
	}
	
	@Override
	public int length() {
		return length;
	}
	
	public void arrayChanged() {
		string = null;
		hash = 0;
	}
	
	public byte[] array() {
		return value;
	}
	
	public int arrayOffset() {
		return offset;
	}
	
	public boolean isEntireArrayUsed() {
		return offset == 0 && length == value.length;
	}
	
	public byte[] toByteArray() {
		return toByteArray(0, length());
	}
	
	public byte[] toByteArray(int start, int end) {
		return Arrays.copyOfRange(value, start + offset, end + offset);
	}
	
	public void copy(int srcIdx, byte[] dst, int dstIdx, int length) {
		if (isOutOfBounds(srcIdx, length, length())) {
			throw new IndexOutOfBoundsException("expected: " + "0 <= srcIdx(" + srcIdx + ") <= srcIdx + length(" 
					+ length + ") <= srcLen(" + length() + ')');
		}
		System.arraycopy(value, srcIdx + offset, ObjectUtil.checkNotNull(dst, "dst"), dstIdx, length);
	}
	
	@Override
	public char charAt(int index) {
		return b2c(byteAt(index));
	}
	
	public boolean contains(CharSequence cs) {
		return indexOf(cs) >= 0;
	}
	
	public int compareTo(CharSequence string) {
		if (this == string) {
			return 0;
		}
		
		int result;
		int length1 = length();
		int length2 = string.length();
		int minLength = Math.min(length1, length2);
		for (int i = 0, j = arrayOffset(); i < minLength; i++, j++) {
			result = b2c(value[j]) - string.charAt(i);
			if (result != 0) {
				return result;
			}
		}
		return length1 - length2;
	}
	
	public AsciiString concat(CharSequence string) {
		int thisLen = length();
		int thatLen = string.length();
		if (thatLen == 0) {
			return this;
		}
		
		if (string instanceof AsciiString) {
			AsciiString that = (AsciiString) string;
			if (isEmpty()) {
				return that;
			}
			
			byte[] newValue = PlatformDependent.allocateUninitializedArray(thisLen + thatLen);
			System.arraycopy(value, arrayOffset(), newValue, 0, thisLen);
			System.arraycopy(that.value, that.arrayOffset(), newValue, thisLen, thatLen);
			return new AsciiString(newValue, false);
		}
		
		if (isEmpty()) {
			return new AsciiString(string);
		}
		
		byte[] newValue = PlatformDependent.allocateUninitializedArray(thisLen + thatLen);
		System.arraycopy(value, arrayOffset(), newValue, 0, thisLen);
		for (int i = thisLen, j = 0; i < newValue.length; i++, j++) {
			newValue[i] = c2b(string.charAt(j));
		}
		return new AsciiString(newValue, false);
	}
	
	public boolean endsWith(CharSequence suffix) {
		int suffixLen = suffix.length();
		return regionMatches(length() - suffixLen, suffix, 0, suffixLen);
	}
	
	public boolean contentEqualsIgnoreCase(CharSequence string) {
		if (this == string) {
			return true;
		}
		
		if (string == null || string.length() != length()) {
			return false;
		}
		
		if (string instanceof AsciiString) {
			AsciiString other = (AsciiString) string;
			byte[] value = this.value;
			if (offset == 0 && other.offset == 0 && length == value.length) {
				byte[] otherValue = other.value;
				for (int i = 0; i < value.length; ++i) {
					if (!equalsIgnoreCase(value[i], otherValue[i])) {
						return false;
					}
				}
				return true;
			}
			return misalignedEqualsIgnoreCase(other);
		}
		
		byte[] value = this.value;
		for (int i = offset, j = 0;j < string.length(); ++i,++j) {
			if (!equalsIgnoreCase(b2c(value[i]), string.charAt(j))) {
				return false;
			}
		}
		return true;
	}
	
	private boolean misalignedEqualsIgnoreCase(AsciiString other) {
		byte[] value = this.value;
		byte[] otherValue = other.value;
		for (int i = offset, j = other.offset, end = offset + length; i < end; ++i, ++j) {
			if (!equalsIgnoreCase(value[i], otherValue[j])) {
				return false;
			}
		}
		return true;
	}
	
	public char[] toCharArray() {
		return toCharArray(0, length());
	}
	
	public char[] toCharArray(int start, int end) {
		int length = end - start;
		if (length == 0) {
			return EmptyArrays.EMPTY_CHARS;
		}
		
		if (isOutOfBounds(start, length, length())) {
			throw new IndexOutOfBoundsException("expected: " + "0 <= start(" + start + ") <= start + length(" 
					+ length + ") <= srcLen(" + length() + ')');
		}
		
		final char[] buffer = new char[length];
		for (int i = 0, j = start + arrayOffset(); i < length; i++, j++) {
			buffer[i] = b2c(value[j]);
		}
		return buffer;
	}
	
	public void copy(int srcIdx, char[] dst, int dstIdx, int length) {
		ObjectUtil.checkNotNull(dst, "dst");
		
		if (isOutOfBounds(srcIdx, length, length())) {
			throw new IndexOutOfBoundsException("expected: " + "0 <= srcIdx(" + srcIdx + ") <= srcIdx + length("
						+ length + ") <= srcLen(" + length() + ')');
		}
		
		final int dstEnd = dstIdx + length;
		for (int i = dstIdx, j = srcIdx + arrayOffset(); i < dstEnd; i++, j++) {
			dst[i] = b2c(value[j]);
		}
	}
	
	public AsciiString subSequence(int start) {
		return subSequence(start, length());
	}
	
	@Override
	public AsciiString subSequence(int start, int end) {
		return subSequence(start, end, true);
	}
	
	public AsciiString subSequence(int start, int end, boolean copy) {
		if (isOutOfBounds(start, end - start, length())) {
			throw new IndexOutOfBoundsException("expected: 0 <= start(" + start + ") <= end(" + end + ") <= length("
					+ length() + ')');
		}
		if (start == 0 && end == length()) {
			return this;
		}
		
		if (end == start) {
			return EMPTY_STRING;
		}
		return new AsciiString(value, start + offset, end - start, copy);
	}
	
	public int indexOf(CharSequence string) {
		return indexOf(string, 0);
	}
	
	public int indexOf(CharSequence subString, int start) {
		final int subCount = subString.length();
		if (start < 0) {
			start = 0;
		}
		if (subCount <= 0) {
			return start < length ? start : length;
		}
		if (subCount > length - start) {
			return INDEX_NOT_FOUND;
		}
		
		final char firstChar = subString.charAt(0);
		if (firstChar > MAX_CHAR_VALUE) {
			return INDEX_NOT_FOUND;
		}
		final byte firstCharAsByte = c2b0(firstChar);
		final int len = offset + length - subCount;
		for (int i = start + offset; i <= len; ++i) {
			if (value[i] == firstCharAsByte) {
				int o1 = i, o2 = 0;
				while (++o2 < subCount && b2c(value[++o1]) == subString.charAt(o2)) {
					
				}
				if (o2 == subCount) {
					return i - offset;
				}
			}
		}
		return INDEX_NOT_FOUND;
	}
	
	public int indexOf(char ch, int start) {
		if (ch > MAX_CHAR_VALUE) {
			return INDEX_NOT_FOUND;
		}
		
		if (start < 0) {
			start = 0;
		}
		
		final byte chAsByte = c2b0(ch);
		final int len = offset + length;
		for (int i = start + offset; i < len; ++i) {
			if (value[i] == chAsByte) {
				return i - offset;
			}
		}
		return INDEX_NOT_FOUND;
	}
	
	public int lastIndexOf(CharSequence string) {
		return lastIndefOf(string, length);
	}
	
	public int lastIndexOf(CharSequence subString, int start) {
		final int subCount = subString.length();
		start = Math.min(start, length - subCount);
		if (start < 0) {
			return INDEX_NOT_FOUND;
		}
		if (subCount == 0) {
			return start;
		}
		
		final char firstChar = subString.charAt(0);
		if (firstChar > MAX_CHAR_VALUE) {
			return INDEX_NOT_FOUND;
		}
		final byte firstCharAsByte = c2b0(firstChar);
		for (int i = offset + start; i >= offset; --i) {
			if (value[i] == firstCharAsByte) {
				int o1 = i, o2 = 0;
				while (++o2 < subCount && b2c(value[++o1]) == subString.charAt(o2)) {
					
				}
				if (o2 == subCount) {
					return i - offset;
				}
			}
		}
		return INDEX_NOT_FOUND;
	}
	
	public boolean regionMatches(int thisStart, CharSequence string, int start, int length) {
		ObjectUtil.checkNotNull(string, "string");
		
		if (start < 0 || string.length() - start < length) {
            return false;
        }

        final int thisLen = length();
        if (thisStart < 0 || thisLen - thisStart < length) {
            return false;
        }

        if (length <= 0) {
            return true;
        }

        if (string instanceof AsciiString) {
            final AsciiString asciiString = (AsciiString) string;
            return PlatformDependent.equals(value, thisStart + offset, asciiString.value,
                                            start + asciiString.offset, length);
        }
        final int thatEnd = start + length;
        for (int i = start, j = thisStart + arrayOffset(); i < thatEnd; i++, j++) {
            if (b2c(value[j]) != string.charAt(i)) {
                return false;
            }
        }
        return true;
	}
	
	public boolean regionMatches(boolean ignoreCase, int thisStart, CharSequence string, int start, int length) {
		if (!ignoreCase) {
			return regionMatches(thisStart, string, start, length);
		}
		ObjectUtil.checkNotNull(string, "string");
		
		final int thisLen = length();
		if (thisStart < 0 || length > thisLen - thisStart) {
			return false;
		}
		if (start < 0 || length > string.length() - start) {
			return false;
		}
		
		thisStart += arrayOffset();
		final int thisEnd = thisStart + length;
		if (string instanceof AsciiString) {
			final AsciiString asciiString = (AsciiString) string;
			final byte[] value = this.value;
			final byte[] otherValue = asciiString.value;
			start += asciiString.offset;
			while (thisStart < thisEnd) {
				if (!equalsIgnoreCase(value[thisStart++], otherValue[start++]) ) {
					return false;
				}
			}
			return true;
		}
		while (thisStart < thisEnd) {
			if (!equalsIgnoreCase(b2c(value[thisStart++]), string.charAt(start++))) {
				return false;
			}
		}
		return true;
	}
	
	public AsciiString replace(char oldChar, char newChar) {
		if (oldChar > MAX_CHAR_VALUE) {
			return this;
		}
		
		final byte oldCharAsByte = c2b0(oldChar);
		final byte newCharAsByte = c2b(newChar);
		final int len = offset + length;
		for (int i = offset; i < len; ++i) {
			if (value[i] == oldCharAsByte) {
				byte[] buffer = PlatformDependent.allocateUninitializedArray(length());
				System.arraycopy(value, offset, buffer, 0, i-offset);
				buffer[i - offset] = newCharAsByte;
				++i;
				for (;i < len; ++i) {
					byte oldValue = value[i];
					buffer[i - offset] = oldValue != oldCharAsByte ? oldValue : newCharAsByte;
				}
				return new AsciiString(buffer, false);
			}
		}
		return this;
	}
	
	public boolean startsWith(CharSequence prefix) {
		return startsWith(prefix, 0);
	}
	
	public boolean startsWith(CharSequence prefix, int start) {
		return regionMatches(start, prefix, 0, prefix.length());
	}
	
	public AsciiString toLowerCase() {
		return AsciiStringUtil.toLowerCase(this);
	}
	
	public AsciiString toUpperCase() {
		return AsciiStringUtil.toUpperCase(this);
	}
	
	public static CharSequence trim(CharSequence c) {
		if (c instanceof AsciiString) {
			return ((AsciiString) c).trim();
		}
		if (c instanceof String) {
			return ((String) c).trim();
		}
		int start = 0, last = c.length() - 1;
		int end = last;
		while (start <= end && c.charAt(start) <= ' ') {
			start++;
		}
		while (end >= start && c.charAt(end) <= ' ') {
			end++;
		}
		if (start == 0 && end == last) {
			return c;
		}
		return c.subSequence(start, end);
	}
	
	public AsciiString trim() {
		int start = arrayOffset(), last = arrayOffset() + length() - 1;
		int end = last;
		while (start <= end && value[start] <= ' ') {
			start++;
		}
		while (end >= start && value[end] >= ' ') {
			end--;
		}
		if (start == 0 && end == last) {
			return this;
		}
		return new AsciiString(value, start, end - start + 1, false);
	}
	
	public boolean contentEquals(CharSequence a) {
		if (this == a) {
			return true;
		}
		
		if (a == null || a.length() != length()) {
			return false;
		}
		if (a instanceof AsciiString) {
			return equals(a);
		}
		for (int i = arrayOffset(), j = 0; j < a.length(); ++i, ++j) {
			if (b2c(value[i]) != a.charAt(j)) {
				return false;
			}
		}
		return true;
	}
	
	public boolean matches(String expr) {
		return Pattern.matches(expr, this);
	}
	
	public AsciiString[] split(String expr, int max) {
		return toAsciiStringArray(Pattern.compile(expr).split(this, max));
	}
	
	public AsciiString[] split(char delim) {
		final List<AsciiString> res = InternalThreadLocalMap.get().arrayList();
		
		int start = 0;
		final int length = length();
		for (int i = start; i < length; i++) {
			if (charAt(i) == delim) {
				if (start == i) {
					res.add(EMPTY_STRING);
				} else {
					res.add(new AsciiString(value, start + arrayOffset(), i - start, false));
				}
				start = i + 1;
			}
		}
		
		if (start == 0) {
			res.add(this);
		} else {
			if (start != length) {
				res.add(new AsciiString(value, start + arrayOffset(), length - start, false));
			} else {
				for (int i = res.size() - 1; i >= 0; i--) {
					if (res.get(i).isEmpty()) {
						res.remove(i);
					} else {
						break;
					}
				}
			}
		}
		return res.toArray(EmptyArrays.EMPTY_ASCII_STRINGS);
	}
	
	@Override
	public int hashCode() {
		int h = hash;
		if (h == 0) {
			h = PlatformDependent.hashCodeAscii(value, offset, length);
			hash = h;
		}
		return h;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || obj.getClass() != AsciiString.class) {
            return false;
        }
        if (this == obj) {
            return true;
        }

        AsciiString other = (AsciiString) obj;
        return length() == other.length() &&
               hashCode() == other.hashCode() &&
               PlatformDependent.equals(array(), arrayOffset(), other.array(), other.arrayOffset(), length());
	}
	
	@Override
	public String toString() {
		String cache = string;
		if (cache == null) {
			cache = toString(0);
			string = cache;
		}
		return cache;
	}
	
	public String toString(int start) {
		return toString(start, length());
	}
	
	public String toString(int start, int end) {
		int length = end - start;
		if (length == 0) {
			return "";
		}
		
		if (isOutOfBounds(start, length, length())) {
			throw new IndexOutOfBoundsException("expected: " + "0 <= start(" + start + ") <= srcIdx + length(" 
					+ length + ") <= srcLen(" + length() + ')');
		}
		
		@SuppressWarnings("deprecation")
		final String str = new String(value, 0, start + offset, length);
		return str;
	}
	
	public boolean parseBoolean() {
		return length >= 1 && value[offset] != 0;
	}
	
	public char parseChar(int start) {
		if (start + 1 >= length()) {
			throw new IndexOutOfBoundsException("2 bytes required to convert to character. Index " + 
					start + " would go out of bounds");
		}
		final int startWithOffset = start + offset;
		return (char) ((b2c(value[startWithOffset]) << 8) | b2c(value[startWithOffset + 1]));
	}
	
	public short parseShort() {
		return parseShort(0, length(), 10);
	}
	
	public short parseShort(int radix) {
		return parseShort(0, length(), radix);
	}
	
	public short parseShort(int start, int end) {
		return parseShort(start, end, 10);
	}
	
	public short parseShort(int start, int end, int radix) {
		int intValue = parseInt(start, end, radix);
		short result = (short) intValue;
		if (result != intValue) {
			throw new NumberFormatException(subSequence(start, end, false).toString());
		}
		return result;
	}
	
	public int parseInt() {
		return parseInt(0, length(), 10);
	}
	
	public int parseInt(int radix) {
		return parseInt(0, length(), radix);
	}
	
	public int parseInt(int start, int end) {
		return parseInt(start, end, 10);
	}
	
	public int parseInt(int start, int end, int radix) {
		if (radix < Character.MIN_RADIX || radix > Character.MAX_RADIX) {
			throw new NumberFormatException();
		}
		if (start == end) {
			throw new NumberFormatException();
		}
		
		int i = start;
		boolean negative = byteAt(i) == '-';
		if (negative && ++i == end) {
			throw new NumberFormatException(subSequence(start, end, false).toString());
		}
		return parseInt(i, end, radix, negative);
	}
	
	private int parseInt(int start, int end, int radix, boolean negative) {
		int max = Integer.MIN_VALUE / radix;
		int result = 0;
		int currOffset = start;
		while (currOffset < end) {
			int digit = Character.digit((char) (value[currOffset++ + offset] & 0xFF), radix);
			if (digit == -1) {
				throw new NumberFormatException(subSequence(start, end, false).toString());
			}
			if (max > result ) {
				throw new NumberFormatException(subSequence(start, end, false).toString());
			}
			int next = result * radix - digit;
			if (next > result) {
				throw new NumberFormatException(subSequence(start, end, false).toString());
			}
			result = next;
		}
		if (!negative) {
			result = - result;
			if (result < 0) {
				throw new NumberFormatException(subSequence(start, end, false).toString());
			}
		}
		return result;
	}
	
	
}

