package netty.buffer;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.StringUtil;

public class DefaultByteBufHolder implements ByteBufHolder {
	private final ByteBuf data;
	
	public DefaultByteBufHolder(ByteBuf data) {
		this.data = ObjectUtil.checkNotNull(data, "data");
	}
	
	@Override
	public ByteBuf content() {
		return ByteBufUtil.ensureAccessible(data);
	}
	
	@Override
	public ByteBufHolder copy() {
		return replace(data.copy());
	}
	
	@Override
	public ByteBufHolder duplicate() {
		return replace(data.duplicate());
	}
	
	@Override
	public ByteBufHolder retainedDuplicate() {
		return replace(data.retainedDuplicate());
	}
	
	@Override
	public ByteBufHolder replace(ByteBuf content) {
		return new DefaultByteBufHolder(content);
	}
	
	@Override
	public int refCnt() {
		return data.refCnt();
	}
	
	@Override
	public ByteBufHolder retain() {
		data.retain();
		return this;
	}
	
	@Override
	public ByteBufHolder retain(int increment) {
		data.retain(increment);
		return this;
	}
	
	@Override
	public ByteBufHolder touch() {
		data.touch();
		return this;
	}
	
	@Override
    public ByteBufHolder touch(Object hint) {
        data.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return data.release();
    }

    @Override
    public boolean release(int decrement) {
        return data.release(decrement);
    }
    
    protected final String contentToString() {
    	return data.toString();
    }
    
    @Override
    public String toString() {
    	return StringUtil.simpleClassName(this) + '(' + contentToString() + ')';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o != null && getClass() == o.getClass()) {
            return data.equals(((DefaultByteBufHolder) o).data);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }
}
