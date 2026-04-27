package netty.common.util.concurrent;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import netty.common.util.internal.PlatformDependent;

import static java.util.Objects.requireNonNull;

public class ConcurrentSkipListIntObjMultimap<V> implements Iterable<ConcurrentSkipListIntObjMultimap.IntEntry<V>>{
	private final int noKey;
	private volatile Index<V> head;
	private final LongAdder adder;
	
	static final class Node<V> {
		final int key;
		volatile V val;
		volatile Node<V> next;
		Node(int key, V value, Node<V> next) {
			this.key = key;
			this.val = value;
			this.next = next;
		}
	}
	
	static final class Index<V> {
		final Node<V> node;
		final Index<V> down;
		volatile Index<V> right;
		Index(Node<V> node, Index<V> down, Index<V> right) {
			this.node = node;
			this.down = down;
			this.right = right;
		}
	}
	
	public static final class IntEntry<V> implements Comparable<IntEntry<V>> {
		private final int key;
		private final V value;
		
		public IntEntry(int key, V value) {
			this.key = key;
			this.value = value;
		}
		
		public int getKey() {
			return key;
		}
		
		public V getValue() {
			return value;
		}
		
		@Override
		public boolean equals(Object o) {
			if (!(o instanceof IntEntry)) {
				return false;
			}
			
			IntEntry<?> intEntry = (IntEntry<?>) o;
			return key == intEntry.key && Objects.equals(value, intEntry.value);
		}
		
		@Override
		public int hashCode() {
			int result = key;
			result = result * 31 + Objects.hashCode(value);
			return result;
		}
		
		@Override
		public String toString() {
			return "IntEntry[" + key + " => " + value + ']';
		}
		
		@Override
		public int compareTo(IntEntry<V> o) {
			return Integer.compare(key, o.key);
		}
	}
	
	static int cpr(int x, int y) {
		return Integer.compare(x, y);
	}
	
	final Node<V> baseHead() {
		Index<V> h;
		acquireFence();
		return (h = head) == null ? null : h.node;
	}
	
	static <V> void unlinkNode(Node<V> b, Node<V> n, int noKey) {
		if (b != null && n != null) {
			Node<V> f, p;
			for (;;) {
				if ((f = n.next) != null && f.key == noKey) {
					p = f.next;
					break;
				} else if (NEXT.compareAndSet(n, f, 
											new Node<V>(noKey, null, f))) {
					p = f;
					break;
				}
			}
			NEXT.compareAndSet(b, n, p);
		}
	}
	
	private void addCount(long c) {
		adder.add(c);
	}
	
	final long getAdderCount() {
		long c;
		return (c = adder.sum()) <= 0L ? 0L : c;
	}
	
	private Node<V> findPredecessor(int key) {
		Index<V> q;
		acquireFence();
		if ((q = head) == null || key == noKey) {
			return null;
		} else {
			for (Index<V> r, d;;) {
				while ((r = q.right) != null) {
					Node<V> p; int k;
					if ((p = r.node) == null || (k = p.key) == noKey ||
							p.val == null) {
						RIGHT.compareAndSet(q, r, r.right);
					} else if (cpr(key, k) > 0) {
						q = r;
					} else {
						break;
					}
				}
				if ((d = q.down) != null) {
					q = d;
				} else {
					return q.node;
				}
			}
		}
	}
	
	private Node<V> findNode(int key) {
		if (key == noKey) {
			throw new IllegalArgumentException();
		}
		Node<V> b;
		outer: while ((b = findPredecessor(key)) != null) {
			for (;;) {
				Node<V> n; int k, c;
				if ((n = b.next) == null) {
					break outer;
				} else if ((k = n.key) == noKey) {
					break;
				} else if (n.val == null) {
					unlinkNode(b, n, noKey);
				} else if ((c = cpr(key, k)) > 0) {
					b = n;
				} else if (c == 0) {
					return n;
				} else {
					break outer;
				}
			}
		}
		return null;
	}
	
	private V doGet(int key) {
		Index<V> q;
		acquireFence();
		if (key == noKey) {
			throw new IllegalArgumentException();
		}
		V result = null;
		if ((q = head) != null) {
			outer: for (Index<V> r, d;;) {
				while ((r = q.right) != null) {
					Node<V> p; int k; V v; int c;
					if ((p = r.node) == null || (k = p.key) == noKey ||
							(v = p.val) == null) {
						RIGHT.compareAndSet(q, r, r.right);
					} else if ((c = cpr(key, k)) > 0) {
						q = r;
					} else if (c == 0) {
						result = v;
						break outer;
					} else {
						break;
					}
				}
				if ((d = q.down) != null) {
					q = d;
				} else {
					Node<V> b, n;
					if ((b = q.node) != null) {
						while ((n = b.next) != null) {
							V v; int c;
							int k = n.key;
							if ((v = n.val) == null || k == noKey ||
									(c = cpr(key, k)) > 0) {
								b = n;
							} else {
								if (c == 0) {
									result = v;
								}
								break;
							}
						}
					}
					break;
				}
			}
		}
		return result;
	}
	
	private V doPut(int key, V value, boolean onlyIfAbsent) {
		if (key == noKey) {
			throw new IllegalArgumentException();
		}
		for (;;) {
			Index<V> h; Node<V> b;
			acquireFence();
			int levels = 0;
			if ((h = head) == null) {
				Node<V> base = new Node<V>(noKey, null, null);
				h = new Index<V>(base, null, null);
				b = HEAD.compareAndSet(this, null, h) ? base : null;
			} else {
				for (Index<V> q = h, r, d;;) {
					while ((r = q.right) != null) {
						Node<V> p; int k;
					
							if ((p = r.node) == null || (k = p.key) == noKey ||
									p.val == null) {
								RIGHT.compareAndSet(q, r, r.right);
							} else if (cpr(key, k) > 0) {
								q = r;
							} else {
								break;
							}
						
					}
					if ((d = q.down) != null) {
						++levels;
						q = d;
					} else {
						b = q.node;
						break;
					}
					
				}
				if (b != null) {
					Node<V> z = null;
					for (;;) {
						Node<V> n, p; int k; int c;
						if ((n = b.next) == null) {
							if (b.key == noKey) {
								cpr(key, key);
							}
							c = -1;
						} else if ((k = n.key) == noKey) {
							break;
						} else if ((n.val) == null) {
							unlinkNode(b, n, noKey);
							c = 1;
						} else if ((c = cpr(key, k)) > 0) {
							b = n;
						}
						if (c <= 0 &&
								NEXT.compareAndSet(b, n, p = new Node<V>(key, value, n))) {
							z = p;
							break;
						}
					}
					
					if (z != null) {
						int lr = ThreadLocalRandom.current().nextInt();
						if ((lr & 0x3) == 0) {
							int hr = ThreadLocalRandom.current().nextInt();
							long rnd = ((long) hr << 32) | ((long) lr & 0xfffffffL);
							int skips = levels;
							Index<V> x = null;
							for (;;) {
								x = new Index<V>(z, x, null);
								if (rnd >= 0 || --skips < 0) {
									break;
								} else {
									rnd <<= 1;
								}
							}
							if (addIndicies(h, skips, x, noKey) && skips < 0 &&
									head == h) {
								Index<V> hx = new Index<V>(z, x, null);
								Index<V> nh = new Index<V>(h.node, h, hx);
								HEAD.compareAndSet(this, h, nh);
							}
							if (z.val == null) {
								findPredecessor(key);
							}
						}
						addCount(1L);
						return null;
					}
				}
			}
		}
	}
	
	static <V> boolean addIndicies(Index<V> q, int skips, Index<V> x, int noKey) {
		Node<V> z; int key;
		if (x != null && (z = x.node) != null && (key = z.key) != noKey &&
				q != null) {
			boolean retrying = false;
			for (;;) {
				Index<V> r, d; int c;
				if ((r = q.right) != null) {
					Node<V> p; int k;
					if ((p = r.node) == null || (k = p.key) == noKey ||
							p.val == null) {
						RIGHT.compareAndSet(q, r, r.right);
						c = 0;
					} else if ((c = cpr(key, k)) > 0) {
						q = r;
					} else if (c == 0) {
						break;
					}
				} else {
					c = -1;
				}
				
				if (c < 0) {
					if ((d = q.down) != null && skips > 0) {
						--skips;
						q = d;
					} else if (d != null && !retrying && 
							!addIndicies(d, 0, x.down, noKey)) {
						break;
					} else {
						x.right = r;
						if (RIGHT.compareAndSet(q, r, x)) {
							return true;
						} else {
							retrying = true;
						}
					}
				}
			}
		}
		return false;
	}
	
	final V doRemove(int key, Object value) {
		if (key == noKey) {
			throw new IllegalArgumentException();
		}
		V result = null;
		Node<V> b;
		outer: while ((b = findPredecessor(key)) != null && 
						result == null) {
			for (;;) {
				Node<V> n; int k; V v; int c;
				if ((n = b.next) == null) {
					break outer;
				} else if ((k = n.key) == noKey) {
					break;
				} else if ((v = n.val) == null) {
					unlinkNode(b, n, noKey);
				} else if ((c = cpr(key, k)) > 0) {
					b = n;
				} else if (c < 0) {
					break outer;
				} else if (value != null && !value.equals(v)) {
					b = n;
				} else if (VAL.compareAndSet(n, v, null)) {
					result = v;
					unlinkNode(b, n, noKey);
					break;
				}
			}
		}
		
		if (result != null) {
			tryReduceLevel();
			addCount(-1L);
		}
		return result;
	}
	
	private void tryReduceLevel() {
		Index<V> h, d, e;
		if ((h = head) != null && h.right == null &&
				(d = h.down) != null && d.right == null &&
				(e = d.down) != null && e.right == null &&
				HEAD.compareAndSet(this, h, d) &&
				h.right != null) {
			HEAD.compareAndSet(this, d, h);
		}
	}
	
	final Node<V> findFirst() {
		Node<V> b, n;
		if ((b = baseHead()) != null) {
			while ((n = b.next) != null) {
				if (n.val == null) {
					unlinkNode(b, n, noKey);
				} else {
					return n;
				}
			}
		}
		return null;
	}
	
	final IntEntry<V> findFirstEntry() {
		Node<V> b, n; V v;
		if ((b = baseHead()) != null) {
			while ((n = b.next) != null) {
				if ((v = n.val) == null) {
					unlinkNode(b, n, noKey);
				} else {
					return new IntEntry<V>(n.key, v);
				}
			}
		}
		return null;
	}
	
	private IntEntry<V> doRemoveFirstEntry() {
		Node<V> b, n; V v;
		if ((b = baseHead()) != null) {
			while ((n = b.next) != null) {
				if ((v = n.val) == null || VAL.compareAndSet(n, v, null)) {
					int k = n.key;
					unlinkNode(b, n, noKey);
					if (v != null) {
						tryReduceLevel();
						findPredecessor(k);
						addCount(-1L);
						return new IntEntry<V>(k, v);
					}
				}
			}
		}
		return null;
	}
	
	final Node<V> findLast() {
		outer: for(;;) {
			Index<V> q; Node<V> b;
			acquireFence();
			if ((q = head) == null) {
				break;
			}
			for (Index<V> r, d;;) {
				while ((r = q.right) != null) {
					Node<V> p;
					if ((p = r.node) == null || p.val == null) {
						RIGHT.compareAndSet(q, r, r.right);
					} else {
						q = r;
					}
				}
				if ((d = q.down) != null) {
					q = d;
				} else {
					b = q.node;
					break;
				}
			}
			if (b != null) {
				for (;;) {
					Node<V> n;
					if ((n = b.next) == null) {
						if (b.key == noKey) {
							break outer;
						} else {
							return b;
						}
					} else if (n.key == noKey) {
						break;
					} else if (n.val == null) {
						unlinkNode(b, n, noKey);
					} else {
						b = n;
					}
				}
			}
		}
		return null;
	}
	
	final IntEntry<V> findLastEntry() {
		for (;;) {
			Node<V> n; V v;
			if ((n = findLast()) == null) {
				return null;
			}
			if ((v = n.val) != null) {
				return new IntEntry<V>(n.key, v);
			}
		}
	}
	
	private IntEntry<V> doRemoveLastEntry() {
		outer: for(;;) {
			Index<V> q; Node<V> b;
			acquireFence();
			if ((q = head) == null) {
				break;
			}
			for (;;) {
				Index<V> d, r; Node<V> p;
				while ((r = q.right) != null) {
					if ((p = r.node) == null || p.val == null) {
						RIGHT.compareAndSet(q, r, r.right);
					} else if (p.next != null) {
						q = r;
					} else {
						break;
					}
				}
				if ((d = q.down) != null) {
					q = d;
				} else {
					b = q.node;
					break;
				}
			} 
			if (b != null) {
				for(;;) {
					Node<V> n; int k; V v;
					if ((n = b.next) == null) {
						if (b.key == noKey) {
							break outer;
						} else {
							break;
						}
					} else if ((k = n.key) == noKey) {
						break;
					} else if ((v = n.val) == null) {
						unlinkNode(b, n, noKey);
					} else if (n.next != null) {
						b = n;
					} else if (VAL.compareAndSet(n, v, null)) {
						unlinkNode(b, n, noKey);
						tryReduceLevel();
						findPredecessor(k);
						addCount(-1L);
						return new IntEntry<V>(k, v);
					}
				}
			}
		}
		return null;
	}
	
	
	private static final int EQ = 1;
	private static final int LT = 2;
	private static final int GT = 0;
	
	final IntEntry<V> findNearEntry(int key, int rel) {
		for (;;) {
			Node<V> n; V v;
			if ((n = findNear(key, rel)) == null) {
				return null;
			}
			if ((v = n.val) != null) {
				return new IntEntry<V>(n.key, v);
			}
		}
	}
	
	final Node<V> findNear(int key, int rel) {
		if (key == noKey) {
			throw new IllegalArgumentException();
		}
		Node<V> result;
		outer: for (Node<V> b;;) {
			if ((b = findPredecessor(key)) == null) {
				result = null;
				break;
			}
			for (;;) {
				Node<V> n; int k; int c;
				if ((n = b.next) == null) {
					result = (rel & LT) != 0 && b.key != noKey ? b : null;
					break outer;
				} else if ((k = n.key) == noKey) {
					break;
				} else if (n.val == null) {
					unlinkNode(b, n, noKey);
				} else if (((c = cpr(key, k)) == 0 && (rel & EQ) != 0) ||
						(c < 0 && (rel & LT) == 0)) {
					result = n;
					break outer;
				} else if (c <= 0 && (rel & LT) != 0) {
					result = b.key != noKey ? b : null;
					break outer;
				} else {
					b = n;
				}
			}
		}
		return result;
	}
	
	public ConcurrentSkipListIntObjMultimap(int noKey) {
		this.noKey = noKey;
		adder = new LongAdder();
	}
	
	public boolean containsKey(int key) {
		return doGet(key) != null;
	}
	
	public V get(int key) {
		return doGet(key);
	}
	
	public V getOrDefault(int key, V defaultValue) {
		V v;
		return (v = doGet(key)) == null ? defaultValue : v;
	}
	
	public void put(int key, V value) {
		requireNonNull(value);
		doPut(key, value, false);
	}
	
	public V remove(int key) {
		return doRemove(key, null);
	}
	
	public boolean containsValue(Object value) {
		requireNonNull(value);
		Node<V> b, n; V v;
		if ((b = baseHead()) != null) {
			while ((n = b.next) != null) {
				if ((v = n.val) != null && value.equals(v)) {
					return true;
				} else {
					b = n;
				}
			}
		}
		return false;
	}
	
	public int size() {
		long c;
		return baseHead() == null ? 0 :
			(c = getAdderCount()) >= Integer.MAX_VALUE ?
					Integer.MAX_VALUE : (int) c;
	}
	
	public boolean isEmpty() {
		return findFirst() == null;
	}
	
	public void clear() {
		Index<V> h, r, d; Node<V> b;
		acquireFence();
		while ((h = head) != null) {
			if ((r = h.right) != null) {
				RIGHT.compareAndSet(h, r, null);
			} else if ((d = h.down) != null) {
				HEAD.compareAndSet(this, h, d);
			} else {
				long count = 0L;
				if ((b = h.node) != null) {
					Node<V> n; V v;
					while ((n = b.next) != null) {
						if ((v = n.val) != null &&
							VAL.compareAndSet(n, v, null)) {
							--count;
							v = null;
						}
						if (v == null) {
							unlinkNode(b, n, noKey);
						}
					}
				} 
				if (count != 0L) {
					addCount(count);
				} else {
					break;
				}
			}
		}
	}
	
	public boolean remove(int key, Object value) {
		if (key == noKey) {
			throw new IllegalArgumentException();
		}
		return value != null && doRemove(key, value) != null;
	}
	
	public boolean replace(int key, V oldValue, V newValue) {
		if (key == noKey) {
			throw new IllegalArgumentException();
		}
		requireNonNull(oldValue);
		requireNonNull(newValue);
		for (;;) {
			Node<V> n; V v;
			if ((n = findNode(key)) == null) {
				return false;
			}
			if ((v = n.val) != null) {
				if (!oldValue.equals(v)) {
					return false;
				}
				if (VAL.compareAndSet(n, v, newValue)) {
					return true;
				}
			}
		}
	}
	
	public int firstKey() {
		Node<V> n = findFirst();
		if (n == null) {
			return noKey;
		}
		return n.key;
	}
	
	public int lastKey() {
		Node<V> n = findLast();
		if (n == null) {
			return noKey;
		}
		return n.key;
	}
	
	public IntEntry<V> lowerEntry(int key) {
		return findNearEntry(key, LT);
	}
	
	public int lowerKey(int key) {
		Node<V> n = findNear(key, LT);
		return n == null ? noKey : n.key;
	}
	
	public IntEntry<V> floorEntry(int key) {
		return findNearEntry(key, LT | EQ);
	}
	
	public int floorKey(int key) {
		Node<V> n = findNear(key, LT | EQ);
		return n == null ? noKey : n.key;
	}
	
	public IntEntry<V> ceilingEntry(int key) {
		return findNearEntry(key, GT | EQ);
	}
	
	public int ceilingKey(int key) {
		Node<V> n = findNear(key, GT | EQ);
		return n == null ? noKey : n.key;
	}
	
	public IntEntry<V> higherEntry(int key) {
		return findNearEntry(key, GT);
	}
	
	public int higherKey(int key) {
		Node<V> n = findNear(key, GT);
		return n == null ? noKey : n.key;
	}
	
	public IntEntry<V> firstEntry() {
		return findFirstEntry();
	}
	
	public IntEntry<V> lastEntry() {
		return findLastEntry();
	}
	
	public IntEntry<V> pollFirstEntry() {
		return doRemoveFirstEntry();
	}
	
	public IntEntry<V> pollLastEntry() {
		return doRemoveLastEntry();
	}
	
	public IntEntry<V> pollCeilingEntry(int key) {
		Node<V> node;
		V val;
		do {
			node = findNear(key, GT | EQ);
			if (node == null) {
				return null;
			}
			val = node.val;
		} while (val == null || !remove(node.key, val));
		return new IntEntry<>(node.key, val);
	}
	
	abstract class Iter<T> implements Iterator<T> {
		Node<V> lastReturned;
		Node<V> next;
		V nextValue;
		
		Iter() {
			advance(baseHead());
		}
		
		@Override
		public final boolean hasNext() {
			return next != null;
		}
		
		final void advance(Node<V> b) {
			Node<V> n = null;
			V v = null;
			if ((lastReturned = b) != null) {
				while ((n = b.next) != null && (v = n.val) == null) {
					b = n;
				}
			}
			nextValue = v;
			next = n;
		}
		
		@Override
		public final void remove() {
			Node<V> n; int k;
			if ((n = lastReturned) == null || (k = n.key) == noKey) {
				throw new IllegalStateException();
			}
			ConcurrentSkipListIntObjMultimap.this.remove(k, n.val);
			lastReturned = null;
		}
	}
	
	final class EntryIterator extends Iter<IntEntry<V>> {
		@Override
		public IntEntry<V> next() {
			Node<V> n;
			if ((n = next) == null) {
				throw new NoSuchElementException();
			}
			int k = n.key;
			V v = nextValue;
			advance(n);
			return new IntEntry<V>(k, v);
		}
	}
	
	@Override
	public Iterator<IntEntry<V>> iterator() {
		return new EntryIterator();
	}
	
	public void forEach(BiConsumer<Integer, ? super V> action) {
		requireNonNull(action);
		Node<V> b, n; V v;
		if ((b = baseHead()) != null) {
			while ((n = b.next) != null) {
				if ((v = n.val) != null) {
					action.accept(n.key, v);
				}
				b = n;
			}			
		}
	}
	
	public void replaceAll(BiFunction<Integer, ? super V, ? extends V> function) {
		requireNonNull(function);
		Node<V> b, n; V v;
		if ((b = baseHead()) != null) {
			while ((n = b.next) != null) {
				while ((v = n.val) != null) {
					V r = function.apply(n.key, v);
					requireNonNull(r);
					if (VAL.compareAndSet(n, v, r)) {
						break;
					}
				}
				b = n;
			}
		}
	}
	
	private static final MethodHandle ACQUIRE_FENCE;
	private static final AtomicReferenceFieldUpdater<ConcurrentSkipListIntObjMultimap<?>, Index<?>> HEAD;
	private static final AtomicReferenceFieldUpdater<Node<?>, Node<?>> NEXT;
	private static final AtomicReferenceFieldUpdater<Node<?>, Object> VAL;
	private static final AtomicReferenceFieldUpdater<Index<?>, Index<?>> RIGHT;
	private static volatile int acquireFenceVariable;
	static {
		try {
			MethodHandles.Lookup lookup = MethodHandles.lookup();
			
			Class<ConcurrentSkipListIntObjMultimap<?>> mapCls = cls(ConcurrentSkipListIntObjMultimap.class);
			Class<Index<?>> indexCls = cls(Index.class);
			Class<Node<?>> nodeCls = cls(Node.class);
			
			if (PlatformDependent.hasVarHandle()) {
				Class<VarHandle> varHandleCls = cls(Class.forName("java.lang.invoke.VarHandle"));
				ACQUIRE_FENCE = lookup.findStatic(
						varHandleCls, 
						"acquireFence", 
						MethodType.methodType(Void.TYPE));
			} else {
				ACQUIRE_FENCE = lookup.findStatic(
						mapCls,
						"acquireFenceFallback", 
						MethodType.methodType(Void.TYPE));
			}
			
			HEAD = AtomicReferenceFieldUpdater.newUpdater(mapCls, indexCls, "head");
			NEXT = AtomicReferenceFieldUpdater.newUpdater(nodeCls, nodeCls, "next");
			VAL = AtomicReferenceFieldUpdater.newUpdater(nodeCls, Object.class, "val");
			RIGHT = AtomicReferenceFieldUpdater.newUpdater(indexCls, indexCls, "right");
		} catch (ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
	
	@SuppressWarnings("unchecked")
	private static <T> Class<T> cls(Class<?> cls) {
		return (Class<T>) cls;
	}
	
	private static void acquireFence() {
		try {
			ACQUIRE_FENCE.invokeExact();
		} catch (Throwable e) {
			LinkageError error = new LinkageError();
			error.initCause(e);
			throw error;
		}
	}
	
	private static void acquireFenceCallback() {
		acquireFenceVariable = 1;
		int ignore = acquireFenceVariable;
	}
}
