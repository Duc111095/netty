package netty.buffer;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.StringUtil;

import static netty.common.util.internal.ObjectUtil.checkPositiveOrZero;

public class ByteBufInputStream extends InputStream implements DataInput {
	private final ByteBuf buffer;
	private final int startIndex;
	private final int endIndex;
	private boolean closed;
	
	private final boolean releaseOnClose;
	
	public ByteBufInputStream(ByteBuf buffer) {
		this(buffer, buffer.readableBytes());
	}
	
	public ByteBufInputStream(ByteBuf buffer, int length) {
		this(buffer, length, false);
	}
	
	public ByteBufInputStream(ByteBuf buffer, boolean releaseOnClose) {
		this(buffer, buffer.readableBytes(), releaseOnClose);
	}
	
	public ByteBufInputStream(ByteBuf buffer, int length, boolean releaseOnClose) {
		ObjectUtil.checkNotNull(buffer, "buffer");
		if (length < 0) {
			if (releaseOnClose) {
				buffer.release();
			}
			checkPositiveOrZero(length, "length");
		}
		if (length > buffer.readableBytes()) {
			if (releaseOnClose) {
				buffer.release();
			}
			throw new IndexOutOfBoundsException("Too many bytes to be read - Needs " 
					+ length + ", maximum is " + buffer.readableBytes());
		}
		this.releaseOnClose = releaseOnClose;
		this.buffer = buffer;
		startIndex = buffer.readerIndex();
		endIndex = startIndex + length;
		buffer.markReaderIndex();
	}
	
	public int readBytes() {
		return buffer.readerIndex() - startIndex;
	}
	
	@Override
	public void close() throws IOException {
		try {
			super.close();
		} finally {
			if (releaseOnClose && !closed) {
				closed = true;
				buffer.release();
			}
		}
	}
	
	@Override
	public int available() throws IOException {
		return endIndex - buffer.readerIndex();
	}
	
	@Override
	public void mark(int readlimit) {
		buffer.markReaderIndex();
	}
	
	@Override
	public boolean markSupported() {
		return true;
	}
	
	@Override
	public int read() throws IOException {
		int available = available();
		if (available == 0) {
			return -1;
		}
		return buffer.readByte() & 0xff;
 	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int available = available();
		if (available == 0) {
			return -1;
		}
		
		len = Math.min(available, len);
		buffer.readBytes(b, off, len);
		return len;
	}
	
	@Override
	public void reset() throws IOException {
		buffer.resetReaderIndex();
	}
	
	@Override
	public long skip(long n) throws IOException {
		if (n > Integer.MAX_VALUE) {
			return skipBytes(Integer.MAX_VALUE);
		} else {
			return skipBytes((int) n);
		}
	}
	
	@Override
	public boolean readBoolean() throws IOException {
		checkAvailable(1);
		return read() != 0;
	}
	
	@Override
	public byte readByte() throws IOException {
		checkAvailable(1);
		return buffer.readByte();
	}
	
	@Override
	public char readChar() throws IOException {
		return (char) readShort();
	}
	
	@Override
	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}
	
	@Override
	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}
	
	@Override
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}
	
	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		checkAvailable(len);
		buffer.readBytes(b, off, len);
	}
	
	@Override
	public int readInt() throws IOException {
		checkAvailable(4);
		return buffer.readInt();
	}
	
	private StringBuilder lineBuf;
	
	@Override
	public String readLine() throws IOException {
		int available = available();
		if (available == 0) {
			return null;
		}
		if (lineBuf != null) {
			lineBuf.setLength(0);
		}
		
		loop: do {
			int c = buffer.readUnsignedByte();
			--available;
			switch (c) {
			case '\n':
				break loop;
			case '\r':
				if (available > 0 && (char) buffer.getUnsignedByte(buffer.readerIndex()) == '\n') {
					buffer.skipBytes(1);
					--available;
				}
				break loop;
			default:
				if (lineBuf == null) {
					lineBuf = new StringBuilder();
				}
				lineBuf.append((char) c);
			}
		} while (available > 0);
		return lineBuf != null && lineBuf.length() > 0 ? lineBuf.toString() : StringUtil.EMPTY_STRING;
	}
	
	@Override
	public long readLong() throws IOException {
		checkAvailable(8);
		return buffer.readLong();
	}
	
	@Override
	public short readShort() throws IOException {
		checkAvailable(2);
		return buffer.readShort();
	}
	
	@Override
	public String readUTF() throws IOException {
		return DataInputStream.readUTF(this);
	}
	
	@Override
	public int readUnsignedByte() throws IOException {
		return readByte() & 0xFF;
	}
	
	@Override
	public int readUnsignedShort() throws IOException {
		return readShort() & 0xffff;
	}
	
	@Override
	public int skipBytes(int n) throws IOException {
		int nBytes = Math.min(available(), n);
		buffer.skipBytes(nBytes);
		return nBytes;
	}
	
	private void checkAvailable(int fieldSize) throws IOException {
		if (fieldSize < 0) {
			throw new IndexOutOfBoundsException("fieldSize cannot be a negative number");
		}
		if (fieldSize > available()) {
			throw new EOFException("fieldSize is too long! Length is " + fieldSize
					+ ", but maximum is " + available());
		}
	}
}
