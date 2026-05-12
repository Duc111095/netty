package netty.channel;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import netty.common.util.ReferenceCounted;

public interface FileRegion extends ReferenceCounted {
	long position();
	
	long transfered();
	
	long transferred();
	
	long count();
	
	long transferTo(WritableByteChannel target, long position) throws IOException;
	
	@Override
	FileRegion retain();
	
	@Override
	FileRegion retain(int increment);
	
	@Override
	FileRegion touch();
	
	@Override
	FileRegion touch(Object hint);
}
