package netty.channel.socket;

import java.net.InetSocketAddress;

import netty.buffer.ByteBuf;
import netty.buffer.ByteBufHolder;
import netty.channel.DefaultAddressedEnvelope;

public class DatagramPacket 
	extends DefaultAddressedEnvelope<ByteBuf, InetSocketAddress> implements ByteBufHolder {

	public DatagramPacket(ByteBuf data, InetSocketAddress recipient) {
		super(data, recipient);
	}
	
	public DatagramPacket(ByteBuf data, InetSocketAddress recipient, InetSocketAddress sender) {
		super(data, recipient, sender);
	}
	
	@Override
	public DatagramPacket copy() {
		return replace(content().copy());
	}
	
	@Override
	public DatagramPacket duplicate() {
		return replace(content().duplicate());
	}
	
	@Override
	public ByteBufHolder retainedDuplicate() {
		return replace(content().retainedDuplicate());
	}
	
	@Override
	public DatagramPacket replace(ByteBuf content) {
		return new DatagramPacket(content, recipient(), sender());
	}
	
	@Override
	public DatagramPacket retain() {
		super.retain();
		return this;
	}
	
	@Override
	public DatagramPacket retain(int increment) {
		super.retain(increment);
		return this;
	}
	
	@Override
    public DatagramPacket touch() {
        super.touch();
        return this;
    }

    @Override
    public DatagramPacket touch(Object hint) {
        super.touch(hint);
        return this;
    }
}
