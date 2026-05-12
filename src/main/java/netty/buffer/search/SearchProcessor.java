package netty.buffer.search;

import netty.common.util.ByteProcessor;

public interface SearchProcessor extends ByteProcessor {
	
	void reset();
}
