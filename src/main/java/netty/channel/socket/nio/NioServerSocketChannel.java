package netty.channel.socket.nio;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.List;
import java.util.Map;

import netty.channel.ChannelException;
import netty.channel.ChannelMetadata;
import netty.channel.ChannelOption;
import netty.channel.ChannelOutboundBuffer;
import netty.channel.nio.AbstractNioMessageChannel;
import netty.channel.socket.DefaultServerSocketChannelConfig;
import netty.channel.socket.ServerSocketChannelConfig;
import netty.channel.socket.SocketProtocolFamily;
import netty.common.util.internal.SocketUtils;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public class NioServerSocketChannel extends AbstractNioMessageChannel
			implements netty.channel.socket.ServerSocketChannel {
	
	private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
	private static final SelectorProvider DEFAULT_SELECTOR_PROVIDER = SelectorProvider.provider();
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioServerSocketChannel.class);
	private static final Method OPEN_SERVER_SOCKET_CHANNEL_WITH_FAMILY = 
			SelectorProviderUtil.findOpenMethod("openServerSocketChannel");
	
	private static ServerSocketChannel newChannel(SelectorProvider provider, SocketProtocolFamily family) {
		try {
			ServerSocketChannel channel =
					SelectorProviderUtil.newChannel(OPEN_SERVER_SOCKET_CHANNEL_WITH_FAMILY, provider, family);
			return channel == null ? provider.openServerSocketChannel() : channel;
		} catch (IOException e) {
			throw new ChannelException("Failed to open a socket.", e);
		}
	}
	
	private final ServerSocketChannelConfig config;
	
	public NioServerSocketChannel() {
		this(DEFAULT_SELECTOR_PROVIDER);
	}
	
	public NioServerSocketChannel(SelectorProvider provider) {
		this(provider, (SocketProtocolFamily) null);
	}
	
	public NioServerSocketChannel(SelectorProvider provider, SocketProtocolFamily family) {
		this(newChannel(provider, family));
	}
	
	public NioServerSocketChannel(ServerSocketChannel channel) {
		super(null, channel, SelectionKey.OP_ACCEPT);
		config = new NioServerSocketChannelConfig(this, javaChannel().socket());
	}
	
	@Override
    public InetSocketAddress localAddress() {
        return (InetSocketAddress) super.localAddress();
    }

    @Override
    public ChannelMetadata metadata() {
        return METADATA;
    }

    @Override
    public ServerSocketChannelConfig config() {
        return config;
    }
    
    @Override
    public boolean isActive() {
    	return isOpen() && javaChannel().socket().isBound();
    }
    
    @Override
    public InetSocketAddress remoteAddress() {
    	return null;
    }
    
    @Override
    protected ServerSocketChannel javaChannel() {
    	return (ServerSocketChannel) super.javaChannel();
    }
    
    @Override
    protected SocketAddress localAddress0() {
        return SocketUtils.localSocketAddress(javaChannel().socket());
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        javaChannel().bind(localAddress, config.getBacklog());
    }

    @Override
    protected void doClose() throws Exception {
        javaChannel().close();
    }

    @Override
    protected int doReadMessages(List<Object> buf) throws Exception {
        SocketChannel ch = SocketUtils.accept(javaChannel());

        try {
            if (ch != null) {
                buf.add(new NioSocketChannel(this, ch));
                return 1;
            }
        } catch (Throwable t) {
            logger.warn("Failed to create a new channel from an accepted socket.", t);

            try {
                ch.close();
            } catch (Throwable t2) {
                logger.warn("Failed to close a socket.", t2);
            }
        }

        return 0;
    }

    @Override
    protected boolean doConnect(
            SocketAddress remoteAddress, SocketAddress localAddress) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doFinishConnect() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return null;
    }

    @Override
    protected void doDisconnect() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean doWriteMessage(Object msg, ChannelOutboundBuffer in) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected final Object filterOutboundMessage(Object msg) throws Exception {
        throw new UnsupportedOperationException();
    }

    private final class NioServerSocketChannelConfig extends DefaultServerSocketChannelConfig {
        private NioServerSocketChannelConfig(NioServerSocketChannel channel, ServerSocket javaSocket) {
            super(channel, javaSocket);
        }

        @Override
        protected void autoReadCleared() {
            clearReadPending();
        }

        @Override
        public <T> boolean setOption(ChannelOption<T> option, T value) {
            if (option instanceof NioChannelOption) {
                return NioChannelOption.setOption(jdkChannel(), (NioChannelOption<T>) option, value);
            }
            return super.setOption(option, value);
        }

        @Override
        public <T> T getOption(ChannelOption<T> option) {
            if (option instanceof NioChannelOption) {
                return NioChannelOption.getOption(jdkChannel(), (NioChannelOption<T>) option);
            }
            return super.getOption(option);
        }

        @Override
        public Map<ChannelOption<?>, Object> getOptions() {
            return getOptions(super.getOptions(), NioChannelOption.getOptions(jdkChannel()));
        }

        private ServerSocketChannel jdkChannel() {
            return ((NioServerSocketChannel) channel).javaChannel();
        }
    }

    @Override
    protected boolean closeOnReadError(Throwable cause) {
        return super.closeOnReadError(cause);
    }
}
