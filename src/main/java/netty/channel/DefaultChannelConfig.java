package netty.channel;

import static netty.channel.ChannelOption.*;
import static netty.common.util.internal.ObjectUtil.checkNotNull;
import static netty.common.util.internal.ObjectUtil.checkPositive;
import static netty.common.util.internal.ObjectUtil.checkPositiveOrZero;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import netty.buffer.ByteBufAllocator;
import netty.common.util.internal.ObjectUtil;

public class DefaultChannelConfig implements ChannelConfig {
	private static final MessageSizeEstimator DEFAULT_MSG_SIZE_ESTIMATOR = DefaultMessageSizeEstimator.DEFAULT;
	
	private static final int DEFAULT_CONNECT_TIMEOUT = 30000;
	
	private static final AtomicIntegerFieldUpdater<DefaultChannelConfig> AUTOREAD_UPDATER =
			AtomicIntegerFieldUpdater.newUpdater(DefaultChannelConfig.class, "autoRead");
	private static final AtomicReferenceFieldUpdater<DefaultChannelConfig, WriteBufferWaterMark> WATERMARK_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(
					DefaultChannelConfig.class, WriteBufferWaterMark.class, "writeBufferWaterMark");
	
	protected final Channel channel;
	
	private volatile ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
	private volatile RecvByteBufAllocator rcvBufAllocator;
	private volatile MessageSizeEstimator msgSizeEstimator = DEFAULT_MSG_SIZE_ESTIMATOR;
	
	private volatile int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT;
	private volatile int writeSpinCount = 16;
	private volatile int maxMessagesPerWrite = Integer.MAX_VALUE;
	
	private volatile int autoRead = 1;
	private volatile boolean autoClose = true;
	private volatile WriteBufferWaterMark writeBufferWaterMark = WriteBufferWaterMark.DEFAULT;
	private volatile boolean pinEventExecutor = true;
	
	public DefaultChannelConfig(Channel channel) {
		this(channel, new AdaptiveRecvByteBufAllocator());
	}
	
	protected DefaultChannelConfig(Channel channel, RecvByteBufAllocator allocator) {
		setRecvByteBufAllocator(allocator, channel.metadata());
		this.channel = channel;
	}
	
	@Override
	public Map<ChannelOption<?>, Object> getOptions() {
		return getOptions(
				null,
				CONNECT_TIMEOUT_MILLIS, MAX_MESSAGES_PER_READ, WRITE_SPIN_COUNT,
                ALLOCATOR, AUTO_READ, AUTO_CLOSE, RECVBUF_ALLOCATOR, WRITE_BUFFER_HIGH_WATER_MARK,
                WRITE_BUFFER_LOW_WATER_MARK, WRITE_BUFFER_WATER_MARK, MESSAGE_SIZE_ESTIMATOR,
                SINGLE_EVENTEXECUTOR_PER_GROUP, MAX_MESSAGES_PER_WRITE); 
	}
	
	protected Map<ChannelOption<?>, Object> getOptions(
			Map<ChannelOption<?>, Object> result, ChannelOption<?>... options) {
		if (result == null) {
			result = new IdentityHashMap<ChannelOption<?>, Object>();
		}
		for (ChannelOption<?> o : options) {
			result.put(o, getOption(o));
		}
		return result;
	}
	
	@SuppressWarnings("unchecked")
    @Override
    public boolean setOptions(Map<ChannelOption<?>, ?> options) {
        ObjectUtil.checkNotNull(options, "options");

        boolean setAllOptions = true;
        for (Entry<ChannelOption<?>, ?> e: options.entrySet()) {
            if (!setOption((ChannelOption<Object>) e.getKey(), e.getValue())) {
                setAllOptions = false;
            }
        }

        return setAllOptions;
    }

    @Override
    @SuppressWarnings({ "unchecked", "deprecation" })
    public <T> T getOption(ChannelOption<T> option) {
        ObjectUtil.checkNotNull(option, "option");

        if (option == CONNECT_TIMEOUT_MILLIS) {
            return (T) Integer.valueOf(getConnectTimeoutMillis());
        }
        if (option == MAX_MESSAGES_PER_READ) {
            return (T) Integer.valueOf(getMaxMessagesPerRead());
        }
        if (option == WRITE_SPIN_COUNT) {
            return (T) Integer.valueOf(getWriteSpinCount());
        }
        if (option == ALLOCATOR) {
            return (T) getAllocator();
        }
        if (option == RECVBUF_ALLOCATOR) {
            return (T) getRecvByteBufAllocator();
        }
        if (option == AUTO_READ) {
            return (T) Boolean.valueOf(isAutoRead());
        }
        if (option == AUTO_CLOSE) {
            return (T) Boolean.valueOf(isAutoClose());
        }
        if (option == WRITE_BUFFER_HIGH_WATER_MARK) {
            return (T) Integer.valueOf(getWriteBufferHighWaterMark());
        }
        if (option == WRITE_BUFFER_LOW_WATER_MARK) {
            return (T) Integer.valueOf(getWriteBufferLowWaterMark());
        }
        if (option == WRITE_BUFFER_WATER_MARK) {
            return (T) getWriteBufferWaterMark();
        }
        if (option == MESSAGE_SIZE_ESTIMATOR) {
            return (T) getMessageSizeEstimator();
        }
        if (option == SINGLE_EVENTEXECUTOR_PER_GROUP) {
            return (T) Boolean.valueOf(getPinEventExecutorPerGroup());
        }
        if (option == MAX_MESSAGES_PER_WRITE) {
            return (T) Integer.valueOf(getMaxMessagesPerWrite());
        }
        return null;
    }

    @Override
    @SuppressWarnings("deprecation")
    public <T> boolean setOption(ChannelOption<T> option, T value) {
        validate(option, value);

        if (option == CONNECT_TIMEOUT_MILLIS) {
            setConnectTimeoutMillis((Integer) value);
        } else if (option == MAX_MESSAGES_PER_READ) {
            setMaxMessagesPerRead((Integer) value);
        } else if (option == WRITE_SPIN_COUNT) {
            setWriteSpinCount((Integer) value);
        } else if (option == ALLOCATOR) {
            setAllocator((ByteBufAllocator) value);
        } else if (option == RECVBUF_ALLOCATOR) {
            setRecvByteBufAllocator((RecvByteBufAllocator) value);
        } else if (option == AUTO_READ) {
            setAutoRead((Boolean) value);
        } else if (option == AUTO_CLOSE) {
            setAutoClose((Boolean) value);
        } else if (option == WRITE_BUFFER_HIGH_WATER_MARK) {
            setWriteBufferHighWaterMark((Integer) value);
        } else if (option == WRITE_BUFFER_LOW_WATER_MARK) {
            setWriteBufferLowWaterMark((Integer) value);
        } else if (option == WRITE_BUFFER_WATER_MARK) {
            setWriteBufferWaterMark((WriteBufferWaterMark) value);
        } else if (option == MESSAGE_SIZE_ESTIMATOR) {
            setMessageSizeEstimator((MessageSizeEstimator) value);
        } else if (option == SINGLE_EVENTEXECUTOR_PER_GROUP) {
            setPinEventExecutorPerGroup((Boolean) value);
        } else if (option == MAX_MESSAGES_PER_WRITE) {
            setMaxMessagesPerWrite((Integer) value);
        } else {
            return false;
        }

        return true;
    }
    	
    protected <T> void validate(ChannelOption<T> option, T value) {
    	ObjectUtil.checkNotNull(option, "option").validate(value);
    }
    
    @Override
    public int getConnectTimeoutMillis() {
    	return connectTimeoutMillis;
    }
    
    @Override
    public ChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis) {
        checkPositiveOrZero(connectTimeoutMillis, "connectTimeoutMillis");
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    @Override
    public int getMaxMessagesPerRead() {
        try {
            MaxMessagesRecvByteBufAllocator allocator = getRecvByteBufAllocator();
            return allocator.maxMessagesPerRead();
        } catch (ClassCastException e) {
            throw new IllegalStateException("getRecvByteBufAllocator() must return an object of type " +
                    "MaxMessagesRecvByteBufAllocator", e);
        }
    }
    
    @Override
    public ChannelConfig setMaxMessagesPerRead(int maxMessagesPerRead) {
        try {
            MaxMessagesRecvByteBufAllocator allocator = getRecvByteBufAllocator();
            allocator.maxMessagesPerRead(maxMessagesPerRead);
            return this;
        } catch (ClassCastException e) {
            throw new IllegalStateException("getRecvByteBufAllocator() must return an object of type " +
                    "MaxMessagesRecvByteBufAllocator", e);
        }
    }
    
    public int getMaxMessagesPerWrite() {
        return maxMessagesPerWrite;
    }

    public ChannelConfig setMaxMessagesPerWrite(int maxMessagesPerWrite) {
        this.maxMessagesPerWrite = ObjectUtil.checkPositive(maxMessagesPerWrite, "maxMessagesPerWrite");
        return this;
    }

    @Override
    public int getWriteSpinCount() {
        return writeSpinCount;
    }
    
    @Override
    public ChannelConfig setWriteSpinCount(int writeSpinCount) {
        checkPositive(writeSpinCount, "writeSpinCount");
        if (writeSpinCount == Integer.MAX_VALUE) {
        	--writeSpinCount;
        }
        this.writeSpinCount = writeSpinCount;
        return this;
    }
    
    @Override
    public ByteBufAllocator getAllocator() {
    	return allocator;
    }
    
    @Override
    public ChannelConfig setAllocator(ByteBufAllocator allocator) {
    	this.allocator = ObjectUtil.checkNotNull(allocator, "allocator");
    	return this;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T extends RecvByteBufAllocator> T getRecvByteBufAllocator() {
        return (T) rcvBufAllocator;
    }

    @Override
    public ChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator) {
        rcvBufAllocator = checkNotNull(allocator, "allocator");
        return this;
    }
    
    private void setRecvByteBufAllocator(RecvByteBufAllocator allocator, ChannelMetadata metadata) {
    	checkNotNull(allocator, "allocator");
    	checkNotNull(metadata, "metadata");
    	if (allocator instanceof MaxMessagesRecvByteBufAllocator) {
    		((MaxMessagesRecvByteBufAllocator) allocator).maxMessagesPerRead(metadata.defaultMaxMessagesPerRead());
    	}
    	setRecvByteBufAllocator(allocator);
    }
    
    @Override
    public boolean isAutoRead() {
    	return autoRead == 1;
    }
    
    @Override
    public ChannelConfig setAutoRead(boolean autoRead) {
    	boolean oldAutoRead = AUTOREAD_UPDATER.getAndSet(this, autoRead ? 1 : 0) == 1;
    	if (autoRead && !oldAutoRead) {
    		channel.read();
    	} else if (!autoRead && oldAutoRead) {
    		autoReadCleared();
    	}
    	return this;
    }
    
    protected void autoReadCleared() {}
    
    @Override
    public boolean isAutoClose() {
    	return autoClose;
    }
    
    @Override
    public ChannelConfig setAutoClose(boolean autoClose) {
    	this.autoClose = autoClose;
    	return this;
    }
    
    @Override
    public int getWriteBufferHighWaterMark() {
    	return writeBufferWaterMark.high();
    }
    
    @Override
    public ChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
    	checkPositiveOrZero(writeBufferHighWaterMark, "writeBufferHighWaterMark");
    	for (;;) {
    		WriteBufferWaterMark waterMark = writeBufferWaterMark;
    		if (writeBufferHighWaterMark < waterMark.low()) {
    			throw new IllegalArgumentException(
    					"writeBufferHighWaterMark cannot be less than " +
    						 	"writeBufferLowWaterMark (" + waterMark.low() + "): " +
    							writeBufferHighWaterMark);
    		}
    		if (WATERMARK_UPDATER.compareAndSet(this, waterMark, 
    				new WriteBufferWaterMark(waterMark.low(), writeBufferHighWaterMark, false))) {
    			return this;
    		}
    	}
    }
    
    @Override
    public int getWriteBufferLowWaterMark() {
    	return writeBufferWaterMark.low();
    }
    
    @Override
    public ChannelConfig setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
        checkPositiveOrZero(writeBufferLowWaterMark, "writeBufferLowWaterMark");
        for (;;) {
            WriteBufferWaterMark waterMark = writeBufferWaterMark;
            if (writeBufferLowWaterMark > waterMark.high()) {
                throw new IllegalArgumentException(
                        "writeBufferLowWaterMark cannot be greater than " +
                                "writeBufferHighWaterMark (" + waterMark.high() + "): " +
                                writeBufferLowWaterMark);
            }
            if (WATERMARK_UPDATER.compareAndSet(this, waterMark,
                    new WriteBufferWaterMark(writeBufferLowWaterMark, waterMark.high(), false))) {
                return this;
            }
        }
    }
    
    @Override
    public ChannelConfig setWriteBufferWaterMark(WriteBufferWaterMark writeBufferWaterMark) {
    	this.writeBufferWaterMark = checkNotNull(writeBufferWaterMark, "writeBufferWaterMark");
    	return this;
    }
    
    @Override
    public WriteBufferWaterMark getWriteBufferWaterMark() {
    	return writeBufferWaterMark;
    }
    
    @Override
    public MessageSizeEstimator getMessageSizeEstimator() {
    	return msgSizeEstimator;
    }
    
    @Override
    public ChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator) {
    	this.msgSizeEstimator = ObjectUtil.checkNotNull(estimator, "estimator");
    	return this;
    }
    
    private ChannelConfig setPinEventExecutorPerGroup(boolean pinEventExecutor) {
    	this.pinEventExecutor = pinEventExecutor;
    	return this;
    }
    
    private boolean getPinEventExecutorPerGroup() {
    	return pinEventExecutor;
    }
}
