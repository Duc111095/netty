package netty.common.util.internal;

import java.io.IOException;
import java.util.Arrays;

import static netty.common.util.internal.ObjectUtil.checkPositive;
import static netty.common.util.internal.ObjectUtil.checkNonEmpty;


public final class AppendableCharSequence implements CharSequence, Appendable {
	private char[] chars;
	private int pos;
	
	public AppendableCharSequence(int length) {
		chars = new char[checkPositive(length, "length")];
	}
	
	private AppendableCharSequence(char[] chars) {
		this.chars = checkNonEmpty(chars, "chars");
		pos = chars.length;
	}
	
	public void setLength(int length) {
		if (length < 0 || length > pos) {
			throw new IllegalArgumentException("length: " + length + " (length >= 0, <= " + pos + ')');
		}
		this.pos = length;
	}
	
	
	
	@Override
	public Appendable append(CharSequence csq) throws IOException {
		return append(csq, 0, csq.length());
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end) throws IOException {
		if (csq.length() < end) {
			throw new IndexOutOfBoundsException("expected: csq.length() >= (" 
					+ end + "), but actual is (" + csq.length() + ")");
		}
		int length = end - start;
		if (length > chars.length - pos) {
			chars = expand(chars, pos + length, pos);
		}
		if (csq instanceof AppendableCharSequence) {
			AppendableCharSequence seq = (AppendableCharSequence) csq;
			char[] src = seq.chars;
			System.arraycopy(src, start, chars, pos, length);
			pos += length;
			return this;
		}
		for (int i = start; i < end; i++) {
			chars[pos++] = csq.charAt(i);
		}
		return this;
	}
	
	public void reset() {
		pos = 0;
	}
	
	@Override
	public String toString() {
		return new String(chars, 0, pos);
	}
	
	public String substring(int start, int end) {
		int length = end - start;
		if (start > pos || length > pos) {
			throw new IndexOutOfBoundsException("expected: start and length <= ("
					+ pos + ")");
		}
		return new String(chars, start, length);
	}
	
	public String subStringUnsafe(int start, int end) {
		return new String(chars, start, end);
	}
	
	private static char[] expand(char[] array, int neededSpace, int size) {
		int newCapacity = array.length;
		do {
			newCapacity <<= 1;
			if (newCapacity < 0) {
				throw new IllegalStateException();
			}
		} while (neededSpace > newCapacity);
		char[] newArray = new char[newCapacity];
		System.arraycopy(array, 0, newArray, 0, size);
		return newArray;
	}

	@Override
	public Appendable append(char c) throws IOException {
		if (pos == chars.length) {
			char[] old = chars;
			chars = new char[old.length << 1];
			System.arraycopy(old, 0, chars, 0, old.length);
		}
		chars[pos++] = c;
		return this;
	}

	@Override
	public int length() {
		return pos;
	}

	@Override
	public char charAt(int index) {
		if (index > pos) {
			throw new IndexOutOfBoundsException();
		}
		return chars[index];
	}
	
	public char charAtUnsafe(int index) {
		return chars[index];
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		if (start == end) {
			return new AppendableCharSequence(Math.min(16, chars.length));
		}
		return new AppendableCharSequence(Arrays.copyOfRange(chars, start, end));
	}

}
