package netty.common.util;

public interface AttributeMap {
	
	<T> Attribute<T> attr(AttributeKey<T> key);
	
	<T> boolean hasAttr(AttributeKey<T> key);
}
