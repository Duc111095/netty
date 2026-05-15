package netty.channel;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import netty.common.util.AbstractReferenceCounted;
import netty.common.util.IllegalReferenceCountException;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public class DefaultFileRegion extends AbstractReferenceCounted implements FileRegion {
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultFileRegion.class);
	private final File f;
	private final long position;
	private final long count;
	private long transferred;
	private FileChannel file;
	
	public DefaultFileRegion(FileChannel fileChannel, long position, long count) {
		this.file = ObjectUtil.checkNotNull(fileChannel, "fileChannel");
		this.position = ObjectUtil.checkPositiveOrZero(position, "position");
		this.count = ObjectUtil.checkPositiveOrZero(count, "count");
		this.f = null;
	}
	
	public DefaultFileRegion(File file, long position, long count) {
		this.f = ObjectUtil.checkNotNull(file, "file");
		this.position = ObjectUtil.checkPositiveOrZero(position, "position");
		this.count = ObjectUtil.checkPositiveOrZero(count, "count");
	}
	
	public boolean isOpen() {
		return file != null;
	}
	
	@SuppressWarnings("resource")
	public void open() throws IOException {
		if (!isOpen() && refCnt() > 0) {
			file = new RandomAccessFile(f, "r").getChannel();
		}
	}
	
	@Override
	public long position() {
		return position;
	}
	
	@Override
	public long count() {
		return count;
	}
	
	@Override
	public long transfered() {
		return transferred;
	}
	
	@Override
	public long transferred() {
		return transferred;
	}
	
	@Override
	public long transferTo(WritableByteChannel target, long position) throws IOException {
		long count = this.count - position;
		if (count < 0 || position < 0) {
			throw new IllegalArgumentException(
					"position out of range: " + position + 
					" (exceeded: 0 - " + (this.count - 1) + ')');
		}
		if (count == 0) {
			return 0L;
		}
		if (refCnt() == 0) {
			throw new IllegalReferenceCountException(0);
		}
		open();
		long written = file.transferTo(this.position + position, count, target);
		if (written > 0) {
			transferred += written;
		} else if (written == 0) {
			validate(this, position);
		}
		return written;
	}
	
	@Override
	protected void deallocate() {
		FileChannel file = this.file;
		if (file == null) {
			return;
		}
		this.file = null;
		try {
			file.close();
		} catch (IOException e) {
			logger.warn("Failed to close file.", e);
		}
	}
	
	@Override
	public FileRegion retain() {
		super.retain();
		return this;
	}
	
	@Override
	public FileRegion retain(int increment) {
		super.retain(increment);
		return this;
	}
	
	@Override
	public FileRegion touch() {
		return this;
	}
	
	@Override
	public FileRegion touch(Object hint) {
		return this;
	}
	
	static void validate(DefaultFileRegion region, long position) throws IOException {
		long size = region.file.size();
		long count = region.count - position;
		if (region.position + count + position > size) {
			throw new IOException("Underlying file size " + size + " smaller then requested count " + region.count);
		}
	}
}
