package netty.channel;

public interface ChannelFactory<T extends Channel> extends netty.bootstrap.ChannelFactory<T> {
	@Override
	T newChannel();
}
