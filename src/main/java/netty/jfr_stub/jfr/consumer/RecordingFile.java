package netty.jfr_stub.jfr.consumer;

import java.io.IOException;
import java.nio.file.Path;

public class RecordingFile implements AutoCloseable{
	
	public RecordingFile(Path ignore) throws IOException {
		throw new UnsupportedOperationException("Stub should only be used at compile time");
	}
	
	public boolean hasMoreEvents() {
		throw new UnsupportedOperationException("Stub should only be used at compile time");
	}
	
	public RecordedEvent readEvent() throws IOException {
		throw new UnsupportedOperationException("Stub should only be used at compile time");
	}
	
	@Override
	public void close() throws IOException {
		throw new UnsupportedOperationException("Stub should only be used at compile time");
	}
}
