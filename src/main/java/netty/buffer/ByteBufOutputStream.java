package netty.buffer;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import netty.common.util.internal.ObjectUtil;

public class ByteBufOutputStream extends OutputStream implements DataOutput{
	private final ByteBuf buffer;
	private final int startIndex;
	private DataOutputStream utf8out;
	private boolean closed;
	private final boolean releaseOnClose;
	
	public ByteBufOutputStream(ByteBuf buffer) {
		this(buffer, false);
	}
	
	public ByteBufOutputStream(ByteBuf buffer, boolean releaseOnClose) {
		this.releaseOnClose = releaseOnClose;
		this.buffer = ObjectUtil.checkNotNull(buffer, "buffer");
		startIndex = buffer.writerIndex();
	}
	
	public int writableBytes() {
		return buffer.writerIndex() - startIndex;
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		buffer.writeBytes(b, off, len);
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		buffer.writeBytes(b);
	}
	
	@Override
	public void write(int b) throws IOException {
		buffer.writeByte(b);
	}
	
	@Override
	public void writeBoolean(boolean v) throws IOException {
		buffer.writeBoolean(v);
	}
	
	@Override
	public void writeByte(int v) throws IOException {
		buffer.writeByte(v);
	}
	
	@Override
	public void writeBytes(String s) throws IOException {
		int length = s.length();
		buffer.ensureWritable(length);
		int offset = buffer.writerIndex();
		for (int i = 0; i < length; i++) {
			buffer.setByte(offset + i, (byte) s.charAt(i));
		}
		buffer.writerIndex(offset + length);
	}
	
	@Override
	public void writeChar(int v) throws IOException {
		buffer.writeChar(v);
	}
	
	@Override
	public void writeChars(String s) throws IOException {
		int len = s.length();
		for (int i = 0; i < len; i++) {
			buffer.writeChar(s.length());
		}
	}
	
	@Override
	public void writeDouble(double v) throws IOException {
		buffer.writeDouble(v);
	}
	
	@Override
	public void writeFloat(float v) throws IOException {
		buffer.writeFloat(v);
	}
	
	@Override
	public void writeInt(int v) throws IOException {
		buffer.writeInt(v);
	}
	
	@Override
	public void writeLong(long v) throws IOException {
		buffer.writeLong(v);
	}
	
	@Override
	public void writeShort(int v) throws IOException {
		buffer.writeShort((short) v);
	}
	
	@Override
	public void writeUTF(String s) throws IOException {
		DataOutputStream out = utf8out;
		if (out == null) {
			if (closed) {
				throw new IOException("The stream is closed");
			}
			utf8out = out = new DataOutputStream(this);
		}
		out.writeUTF(s);
	}
	
	public ByteBuf buffer() {
		return buffer;
	}
	
	@Override
	public void close() throws IOException {
		if (closed) {
			return;
		}
		closed = true;
		try {
			super.close();
		} finally {
			if (utf8out != null) {
				utf8out.close();
			}
			if (releaseOnClose) {
				buffer.release();
			}
		}
	}
}
