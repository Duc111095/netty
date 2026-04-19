package netty.common.util.internal;

import java.util.Queue;

public interface PriorityQueue<T> extends Queue<T> {
	
	boolean removeTyped(T node);
	
	boolean containsTyped(T node);
	
	void priorityChanged(T node);
	
	void clearIgnoringIndexes();
	
}
