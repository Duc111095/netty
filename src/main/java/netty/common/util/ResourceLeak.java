package netty.common.util;

public interface ResourceLeak {
	void record();
	void record(Object hint);
	boolean close();
}
