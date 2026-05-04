package netty.buffer;

import netty.common.util.internal.UnstableApi;

@UnstableApi
interface ChunkInfo {
	int capacity();
	
	boolean isDirect();
	
	long memoryAddress();
}
