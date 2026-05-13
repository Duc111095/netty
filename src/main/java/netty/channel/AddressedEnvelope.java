package netty.channel;

import java.net.SocketAddress;

import netty.common.util.ReferenceCounted;

public interface AddressedEnvelope<M, A extends SocketAddress> extends ReferenceCounted {
	M content();
	
	A sender();
	
	A recipent();
	
	@Override
	AddressedEnvelope<M, A> retain();
	
	@Override
	AddressedEnvelope<M, A> retain(int increment);
	
	@Override
	AddressedEnvelope<M, A> touch();
	
	@Override
	AddressedEnvelope<M, A> touch(Object hint);
}
