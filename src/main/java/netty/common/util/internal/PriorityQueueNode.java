package netty.common.util.internal;

public interface PriorityQueueNode {
	int INDEX_NOT_IN_QUEUE = -1;
	int priorityQueueIndex(DefaultPriorityQueue<?> queue);
	void priorityQueueIndex(DefaultPriorityQueue<?> queue, int i); 
}
