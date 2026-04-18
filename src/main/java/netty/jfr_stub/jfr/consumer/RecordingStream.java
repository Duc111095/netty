package netty.jfr_stub.jfr.consumer;

import java.util.function.Consumer;

import netty.jfr_stub.jfr.Event;
import netty.jfr_stub.jfr.EventSettings;

@SuppressWarnings("Since15")
public class RecordingStream implements AutoCloseable{
	public RecordingStream() {
		throw new UnsupportedOperationException("Stub should only be used at compile time");
	}
	
	public void startAsync() {
		throw new UnsupportedOperationException("Stub should only be used at compile time");
	}
	
	@Override
	public void close() {
		throw new UnsupportedOperationException("Stub should only be used at compile time");
	}
	
	public EventSettings enable(String s) {
		throw new UnsupportedOperationException("Stub should only be used at compile time");
	}
	
	public EventSettings enable(Class<? extends Event> c) {
		throw new UnsupportedOperationException("Stub should only be used at compile time");
	}
	
	public EventSettings disable(String s) {
		throw new UnsupportedOperationException("Stub should only be used at compile time");
	}
	
	public void onEvent(String name, Consumer<RecordedEvent> consumer) {
		throw new UnsupportedOperationException("Stub should only be used at compile time");
	}
}
