package netty.channel.nio;

import java.nio.channels.SelectionKey;

import netty.channel.IoOps;

public final class NioIoOps implements IoOps {
	
	public static final NioIoOps NONE = new NioIoOps(0);
	
	public static final NioIoOps ACCEPT = new NioIoOps(SelectionKey.OP_ACCEPT);
	
	public static final NioIoOps CONNECT = new NioIoOps(SelectionKey.OP_CONNECT);
	
	public static final NioIoOps WRITE = new NioIoOps(SelectionKey.OP_WRITE);
	
	public static final NioIoOps READ = new NioIoOps(SelectionKey.OP_READ);
	
	public static final NioIoOps READ_AND_ACCEPT = new NioIoOps(SelectionKey.OP_READ | SelectionKey.OP_ACCEPT);
	
	public static final NioIoOps READ_AND_WRITE = new NioIoOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
	
	private static final NioIoEvent[] EVENTS;
	
	static {
		NioIoOps all = new NioIoOps(
				NONE.value | ACCEPT.value | CONNECT.value | WRITE.value | READ.value);
		
		EVENTS = new NioIoEvent[all.value + 1];
		addToArray(EVENTS, NONE);
		addToArray(EVENTS, ACCEPT);
		addToArray(EVENTS, CONNECT);
		addToArray(EVENTS, WRITE);
		addToArray(EVENTS, READ);
		addToArray(EVENTS, READ_AND_ACCEPT);
		addToArray(EVENTS, READ_AND_WRITE);
		addToArray(EVENTS, all);
	}
	
	private static void addToArray(NioIoEvent[] array, NioIoOps opt) {
		array[opt.value] = new DefaultNioIoEvent(opt);
	}
	
	final int value;
	
	private NioIoOps(int value) {
		this.value = value;
	}
	
	public boolean contains(NioIoOps ops) {
		return isIncludedIn(ops.value);
	}
	
	public NioIoOps with(NioIoOps ops) {
		if (contains(ops)) {
			return this;
		}
		return valueOf(value | ops.value);
	}
	
	public NioIoOps without(NioIoOps ops) {
		if (!contains(ops)) {
			return this;
		}
		return valueOf(value & ~ops.value);
	}
	
	public int value() {
		return value;
	}
	
	@Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NioIoOps nioOps = (NioIoOps) o;
        return value == nioOps.value;
    }

    @Override
    public int hashCode() {
        return value;
    }
    
    public static NioIoOps valueOf(int value) {
    	return eventOf(value).ops();
    }
	
    public boolean isIncludedIn(int ops) {
    	return (ops & value) != 0;
    }
    
    public boolean isNotIncludedIn(int ops) {
    	return (ops & value) == 0;
    }
    
    static NioIoEvent eventOf(int value) {
    	if (value > 0 && value < EVENTS.length) {
    		NioIoEvent event = EVENTS[value];
    		if (event != null) {
    			return event;
    		}
    	}
    	return new DefaultNioIoEvent(new NioIoOps(value));
    }
    
	private static final class DefaultNioIoEvent implements NioIoEvent {
		private final NioIoOps ops;
		
		DefaultNioIoEvent(NioIoOps ops) {
			this.ops = ops;
		}
		
		@Override
		public NioIoOps ops() {
			return ops;
		}
		
		@Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NioIoEvent event = (NioIoEvent) o;
            return event.ops().equals(ops());
        }

        @Override
        public int hashCode() {
            return ops().hashCode();
        }
	}
}
