package netty.channel;

import netty.common.util.IntSupplier;

public interface SelectStrategy {
	int SELECT = -1;
	int CONTINUE = -2;
	int BUSY_WAIT = -3;
	
	int calculateStrategy(IntSupplier selectSupplier, boolean hasTasks) throws Exception;
}
