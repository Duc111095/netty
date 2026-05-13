package netty.channel;

public interface MessageSizeEstimator {
	
	Handle newHandle();
	
	interface Handle {
		int size(Object msg);
	}
}
