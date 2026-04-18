package netty.common.util;

public interface Constant<T extends Constant<T>> extends Comparable<T> {

	int id();
	
	String name();
}
