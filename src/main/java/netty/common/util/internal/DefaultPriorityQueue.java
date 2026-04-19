package netty.common.util.internal;

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static netty.common.util.internal.PriorityQueueNode.INDEX_NOT_IN_QUEUE;

public final class DefaultPriorityQueue<T extends PriorityQueueNode> extends AbstractQueue<T> 
	implements PriorityQueue<T> {
	
	private static final PriorityQueueNode[] EMPTY_ARRAY = new PriorityQueueNode[0];
	private final Comparator<T> comparator;
	private T[] queue;
	private int size;
	
	@SuppressWarnings("unchecked")
	public DefaultPriorityQueue(Comparator<T> comparator, int initialSize) {
		this.comparator = ObjectUtil.checkNotNull(comparator, "comparator");
		queue = (T[]) (initialSize != 0 ?  new PriorityQueueNode[initialSize] : EMPTY_ARRAY);
	}
	
	@Override
	public boolean isEmpty() {
		return size == 0;
	}
	
	@Override
	public boolean contains(Object o) {
		if (!(o instanceof PriorityQueueNode)) {
			return false;
		}
		PriorityQueueNode node = (PriorityQueueNode) o;
		return contains(node, node.priorityQueueIndex(this));
	}
	
	@Override
	public boolean containsTyped(T node) {
		return contains(node, node.priorityQueueIndex(this));
	}
	
	@Override
	public void clear() {
		for (int i = 0; i < size; i++) {
			T node = queue[i];
			if (node != null) {
				node.priorityQueueIndex(this, INDEX_NOT_IN_QUEUE);
				queue[i] = null;
			}
		}
		size = 0;
	}
	
	@Override
	public void clearIgnoringIndexes() {
		size = 0;
	}
	
	@Override
	public boolean offer(T e) {
		if (e.priorityQueueIndex(this) != INDEX_NOT_IN_QUEUE) {
			throw new IllegalArgumentException("e.priorityQueueIndex(): " + e.priorityQueueIndex(this) + 
					" (expected: " + INDEX_NOT_IN_QUEUE + ") + e: " + e);
		}
		if (size >= queue.length) {
			queue = Arrays.copyOf(queue, queue.length + ((queue.length < 64) ? 
														 (queue.length + 2) :
														 (queue.length >>> 1)));
		}
		bubbleUp(size++, e);
		return true;
	}
	
	@Override
	public T poll() {
		if (size == 0) {
			return null;
		}
		T result = queue[0];
		result.priorityQueueIndex(this, INDEX_NOT_IN_QUEUE);
		
		T last = queue[--size];
		queue[size] = null;
		if (size != 0) {
			bubbleDown(0, last);
		}
		return result;
	}
	
	@Override
	public T peek() {
		return size == 0 ? null : queue[0];
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(Object o) {
		final T node;
		try {
			node = (T) o;
		} catch (ClassCastException e) {
			return false;
		}
		return removeTyped(node);
	}
	
	@Override
	public boolean removeTyped(T node) {
		int i = node.priorityQueueIndex(this);
		if (!contains(node, i)) {
			return false;
		}
		
		node.priorityQueueIndex(this, INDEX_NOT_IN_QUEUE);
		if (--size == 0 || size == i) {
			queue[i] = null;
			return true;
		}
		
		T moved = queue[i] = queue[size];
		queue[size] = null;
		if (comparator.compare(node, moved) < 0) {
			bubbleDown(i, moved);
		} else {
			bubbleUp(i, moved);
		}
		return true;
	}
	
	@Override
	public void priorityChanged(T node) {
		int i = node.priorityQueueIndex(this);
		if (!contains(node, i)) {
			return;
		}
		if (i == 0) {
			bubbleDown(i, node);
		} else {
			int iParent = (i - 1) >>> 1;
			T parent = queue[iParent];
			if (comparator.compare(node, parent) < 0) {
				bubbleUp(i, node);
			} else {
				bubbleDown(i, node);
			}
		}
	}
	
	@Override
	public Object[] toArray() {
		return Arrays.copyOf(queue, size);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <X> X[] toArray(X[] a) {
		if (a.length < size) {
			return (X[]) Arrays.copyOf(queue, size, a.getClass());
		}
		System.arraycopy(queue, 0, a, 0, size);
		if (a.length > size) {
			a[size] = null;
		}
		return a;
	}
	
	@Override
	public Iterator<T> iterator() {
		return new PriorityQueueIterator();
	}
	
	private final class PriorityQueueIterator implements Iterator<T> {
		private int index;
		
		@Override
		public boolean hasNext() {
			return index < size;
		}
		
		@Override
		public T next() {
			if (index >= size) {
				throw new NoSuchElementException();
			}
			return queue[index++];
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove");
		}
	}
	
	private boolean contains(PriorityQueueNode node, int i) {
		return i >= 0 && i < size && node.equals(queue[i]);
	}
	
	private void bubbleDown(int k, T node) {
		final int half = size >>> 1;
		while (k < half) {
			int iChild = (k << 1) + 1;
			T child = queue[iChild];
			int rightChild = iChild + 1;
			if (rightChild < size && comparator.compare(child, queue[rightChild]) > 0) {
				child = queue[iChild = rightChild];
			}
			if (comparator.compare(node, child) <= 0) {
				break;
			}
			queue[k] = child;
			child.priorityQueueIndex(this, k);
			
			k = iChild;
		}
		queue[k] = node;
		node.priorityQueueIndex(this, k + 1);
	}
	
	private void bubbleUp(int k, T node) {
		while (k > 0) {
			int iParent = (k - 1) >>> 1;
			T parent = queue[iParent];
			if (comparator.compare(node, parent) >= 0) {
				break;
			}
			queue[k] = parent;
			parent.priorityQueueIndex(this, k);
			k = iParent;
		}
		queue[k] = node;
		node.priorityQueueIndex(this, k);
	}

	@Override
	public int size() {
		return size;
	}
}
