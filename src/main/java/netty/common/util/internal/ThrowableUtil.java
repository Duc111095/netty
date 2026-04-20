package netty.common.util.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

public final class ThrowableUtil {
	
	private ThrowableUtil() {
		
	}
	
	public static <T extends Throwable> T unknownStackTrace(T cause, Class<?> clazz, String method) {
		cause.setStackTrace(new StackTraceElement[] {new StackTraceElement(clazz.getName(), method, null, -1)});
		return cause;
	}
	
	public static String stackTraceToString(Throwable cause) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		PrintStream pout = new PrintStream(out);
		cause.printStackTrace(pout);
		pout.flush();
		try {
			return out.toString();
		} finally {
			try {
				out.close();
			} catch (IOException ignore) {
				
			}
		}
	}
	
	public static boolean haveSuppressed() {
		return true;
	}
	
	public static void addSuppressed(Throwable target, Throwable suppressed) {
		if (suppressed != null) {
			target.addSuppressed(suppressed);
		}
	}
	
	public static void addSuppressed(Throwable target, List<Throwable> suppressed) {
		for (Throwable t : suppressed) {
			addSuppressed(target, t);
		}
	}
	
	public static void addSuppressedAndClear(Throwable target, List<Throwable> suppressed) {
		addSuppressed(target, suppressed);
		suppressed.clear();
	}
	
	public static Throwable[] getSuppressed(Throwable source) {
		return source.getSuppressed();
	}
	
	public static void interruptedAndAttachAsyncStackTrace(Thread thread, Throwable cause) {
		StackTraceElement[] stackTrace = thread.getStackTrace();
		InterruptedException asyncIE = new InterruptedException(
				"Asynchronous interruption: " + thread);
		thread.interrupt();
		asyncIE.setStackTrace(stackTrace);
		addSuppressed(cause, asyncIE);
	}
}
