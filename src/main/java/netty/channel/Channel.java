package netty.channel;

import java.net.SocketAddress;

import netty.buffer.ByteBufAllocator;
import netty.common.util.AttributeMap;

public interface Channel extends AttributeMap, ChannelOutboundInvoker, Comparable<Channel> {
	ChannelId id();
	
	EventLoop eventLoop();
	
	Channel parent();
	
	ChannelConfig config();
	
	boolean isOpen();
	
	boolean isRegistered();
	
	boolean isActive();
	
	ChannelMetadata metadata();
	
	SocketAddress localAddress();
	
	SocketAddress remoteAddress();
	
	ChannelFuture closeFuture();
	
	default boolean isWritable() {
		ChannelOutboundBuffer buf = unsafe().outboundBuffer();
		return buf != null && buf.isWritable();
	}
	
	default long bytesBeforeUnwritable() {
		ChannelOutboundBuffer buf = unsafe().outboundBuffer();	
		return buf != null ? buf.bytesBeforeUnwritable() : 0;
	}
	
	default long bytesBeforeWritable() {
		ChannelOutboundBuffer buf = unsafe().outboundBuffer();
		return buf != null ? buf.bytesBeforeWritable() : Long.MAX_VALUE;
	}
	
	Unsafe unsafe();
	
	ChannelPipeline pipeline();
	
	default ByteBufAllocator alloc() {
		return config().getAllocator();
	}
	
	default <T> T getOption(ChannelOption<T> option) {
		return config().getOption(option);
	}
	
	default <T> boolean setOption(ChannelOption<T> option, T value) {
		return config().setOption(option, value);
	}
	
	@Override
	default Channel read() {
		pipeline().read();
		return this;
	}
	
	@Override
	default Channel flush() {
		pipeline().flush();
		return this;
	}
	
	@Override
	default ChannelFuture writeAndFlush(Object msg) {
		return pipeline().writeAndFlush(msg);
	}
	
	@Override
	default ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
		return pipeline().writeAndFlush(msg, promise);
	}
	
	@Override
	default ChannelFuture write(Object msg, ChannelPromise promise) {
		return pipeline().write(msg, promise);
	}
	
	@Override
	default ChannelFuture write(Object msg) {
		return pipeline().write(msg);
	}
	
	@Override
	default ChannelFuture deregister(ChannelPromise promise) {
		return pipeline().deregister(promise);
	}
	
	@Override
	default ChannelFuture close(ChannelPromise promise) {
		return pipeline().close(promise);
	}
	
	@Override
	default ChannelFuture disconnect(ChannelPromise promise) {
		return pipeline().disconnect(promise);
	}
	
	@Override
	default ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
		return pipeline().connect(remoteAddress, localAddress, promise);
	}
	
	@Override
    default ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        return pipeline().connect(remoteAddress, promise);
    }

    @Override
    default ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        return pipeline().bind(localAddress, promise);
    }

    @Override
    default ChannelFuture deregister() {
        return pipeline().deregister();
    }

    @Override
    default ChannelFuture close() {
        return pipeline().close();
    }

    @Override
    default ChannelFuture disconnect() {
        return pipeline().disconnect();
    }

    @Override
    default ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return pipeline().connect(remoteAddress, localAddress);
    }

    @Override
    default ChannelFuture connect(SocketAddress remoteAddress) {
        return pipeline().connect(remoteAddress);
    }

    @Override
    default ChannelFuture bind(SocketAddress localAddress) {
        return pipeline().bind(localAddress);
    }

    @Override
    default ChannelPromise newPromise() {
        return pipeline().newPromise();
    }

    @Override
    default ChannelProgressivePromise newProgressivePromise() {
        return pipeline().newProgressivePromise();
    }

    @Override
    default ChannelFuture newSucceededFuture() {
        return pipeline().newSucceededFuture();
    }

    @Override
    default ChannelFuture newFailedFuture(Throwable cause) {
        return pipeline().newFailedFuture(cause);
    }

    @Override
    default ChannelPromise voidPromise() {
        return pipeline().voidPromise();
    }
	
    interface Unsafe {
    	RecvByteBufAllocator.Handle recvBufAllocHandle();
    	
    	SocketAddress localAddress();
    	
    	SocketAddress remoteAddress();
    	
    	void register(EventLoop eventLoop, ChannelPromise promise);
    	
    	void bind(SocketAddress localAddress, ChannelPromise promise);
    	
    	void connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise);
    	
    	void disconnect(ChannelPromise promise);
    	
    	void close(ChannelPromise promise);
    	
    	void closeForcibly();
    	
    	void deregister(ChannelPromise promise);
    	
    	void beginRead();
    	
    	void write(Object msg, ChannelPromise promise);
    	
    	void flush();
    	
    	ChannelPromise voidPromise();
    	
    	ChannelOutboundBuffer outboundBuffer();
    }
}
