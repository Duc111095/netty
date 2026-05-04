package netty.buffer;

import netty.common.util.ReferenceCounted;

public interface ByteBufHolder extends ReferenceCounted {

	ByteBuf content();
	
	ByteBufHolder copy();
	
	ByteBufHolder duplicate();
	
	ByteBufHolder retainedDuplicate();
	
	ByteBufHolder replace(ByteBuf content);
	
	@Override
	ByteBufHolder retain();
	
	@Override
	ByteBufHolder retain(int increment);
	
	@Override
	ByteBufHolder touch();
	
	@Override
	ByteBufHolder touch(Object hint);
}
