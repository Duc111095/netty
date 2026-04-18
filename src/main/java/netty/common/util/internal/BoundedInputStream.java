package netty.common.util.internal;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.jetbrains.annotations.NotNull;

public final class BoundedInputStream extends FilterInputStream {
	
	private final int maxBytesRead;
	private int numRead;
	
	public BoundedInputStream(@NotNull InputStream in, int maxBytesRead) {
		super(in);
		this.maxBytesRead = ObjectUtil.checkPositive(maxBytesRead, "maxRead");
	}
	
	public BoundedInputStream(InputStream in) {
		this(in, 8 * 1024);
	}
	
	@Override
	public int read() throws IOException {
		checkMaxBytesRead();
		
		int b = super.read();
		if (b != -1) {
			numRead++;
		}
		return b;
	}

	@Override
	public int read(byte[] buf, int off, int len) throws IOException {
		checkMaxBytesRead();
		
		int num = Math.min(len, maxBytesRead - numRead + 1);
		int b = super.read(buf, off, num);
		if (b != -1) {
			numRead += b;
		}
		return b;
	}
	
	private void checkMaxBytesRead() throws IOException {
		if (numRead > maxBytesRead) {
			throw new IOException("Maximum number of bytes read: " + numRead);
		}
	}
}
