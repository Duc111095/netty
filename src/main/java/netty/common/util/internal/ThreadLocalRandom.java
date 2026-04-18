package netty.common.util.internal;

import java.lang.Thread.UncaughtExceptionHandler;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static netty.common.util.internal.ObjectUtil.checkPositive;

@SuppressWarnings("all")
public class ThreadLocalRandom extends Random{
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(ThreadLocalRandom.class);
	private static final AtomicLong seedUniquifier = new AtomicLong();
	private static volatile long initialSeedUniquifier;
	private static final Thread seedGeneratorThread;
	private static final BlockingQueue<Long> seedQueue;
	private static final long seedGeneratorStartTime;
	private static volatile long seedGeneratorEndTime;
	
	static {
		initialSeedUniquifier = SystemPropertyUtil.getLong("io.netty.initialSeedUniquifier", 0);
		if (initialSeedUniquifier == 0) {
			boolean secureRandom = SystemPropertyUtil.getBoolean("java.util.secureRandomSeed", false);
			if (secureRandom) {
				seedQueue = new LinkedBlockingQueue<Long>();
				seedGeneratorStartTime = System.nanoTime();
				
				seedGeneratorThread = new Thread("initialSeedUniquifierGenerator") {
					@Override
					public void run() {
						final SecureRandom random = new SecureRandom();
						final byte[] seed = random.generateSeed(8);
						seedGeneratorEndTime = System.nanoTime();
						long s = ((long) seed[0] & 0xff) << 56 |
								((long) seed[1] & 0xff) << 48 |
								((long) seed[2] & 0xff) << 40 |
								((long) seed[3] & 0xff) << 32 |
								((long) seed[4] & 0xff) << 24 |
								((long) seed[5] & 0xff) << 16 |
								((long) seed[6] & 0xff) << 8 |
								((long) seed[7] & 0xff);
						seedQueue.add(s);
					}
				};
				seedGeneratorThread.setDaemon(true);
				seedGeneratorThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

					@Override
					public void uncaughtException(Thread t, Throwable e) {
						logger.debug("An exception has been raised by {}", t.getName(), e);
					}
				});
				seedGeneratorThread.start();
			} else {
				initialSeedUniquifier = mix64(System.currentTimeMillis()) ^ mix64(System.nanoTime());
				seedGeneratorThread = null;
				seedQueue = null;
				seedGeneratorStartTime = 0L;
			}
		} else {
			seedGeneratorThread = null;
			seedQueue = null;
			seedGeneratorStartTime = 0L;
		}
	}
	
	public static void setInitialSeedUniquifier(long initialSeedUniquier) {
		ThreadLocalRandom.initialSeedUniquifier = initialSeedUniquifier;
	}
	
	public static long getInitialSeedUniquifier() {
		long initialSeedUniquifier = ThreadLocalRandom.initialSeedUniquifier;
		if (initialSeedUniquifier != 0) {
			return initialSeedUniquifier;
		}
		
		synchronized (ThreadLocalRandom.class) {
			initialSeedUniquifier = ThreadLocalRandom.initialSeedUniquifier;
			if (initialSeedUniquifier != 0) {
				return initialSeedUniquifier;
			}
			
			final long timeoutSeconds = 3;
			final long deadLine = seedGeneratorStartTime + TimeUnit.SECONDS.toNanos(timeoutSeconds);
			boolean interrupted = false;
			for (;;) {
				final long waitTime = deadLine - System.nanoTime();
				try {
					final Long seed;
					if (waitTime <= 0) {
						seed = seedQueue.poll();
					} else {
						seed = seedQueue.poll(timeoutSeconds, TimeUnit.NANOSECONDS);
					}
					
					if (seed != null) {
						initialSeedUniquifier = seed;
						break;
					}
				} catch (InterruptedException e) {
					interrupted = true;
					logger.warn("Failed to generate a seed from SecureRandom due to an InterruptedException");
					break;
				}
				
				if (waitTime <= 0) {
					seedGeneratorThread.interrupt();
					logger.warn(
							"Failed to generate a seed from SecureRandom within {} seconds. " +
							"Not enough entropy?", timeoutSeconds);
					break;
				}
			}
			
			initialSeedUniquifier ^= 0x3255ecdc33bae119L;
			initialSeedUniquifier ^= Long.reverse(System.nanoTime());
			
			ThreadLocalRandom.initialSeedUniquifier = initialSeedUniquifier;
			
			if (interrupted) {
				Thread.currentThread().interrupt();
				seedGeneratorThread.interrupt();
			}
			
			if (seedGeneratorEndTime == 0) {
				seedGeneratorEndTime = System.nanoTime();
			}
			return initialSeedUniquifier;
		}
	}
	
	private static long newSeed() {
		for (;;) {
			final long current = seedUniquifier.get();
			final long actualCurrent = current != 0 ? current : getInitialSeedUniquifier();
			
			final long next = actualCurrent * 181783497276652981L;
			
			if (seedUniquifier.compareAndSet(current, next)) {
				if (current == 0 && logger.isDebugEnabled()) {
					if (seedGeneratorEndTime != 0) {
						logger.debug(String.format(
								"-Dio.netty.initialSeedUniquifier : 0x%016x (took %d ms)", 
								actualCurrent,
								TimeUnit.NANOSECONDS.toMillis(seedGeneratorEndTime - seedGeneratorStartTime)));
					} else {
						logger.debug(String.format("-Dio.netty.initialSeedUniquifier: 0x%016x", actualCurrent));
					}
				}
				return next ^ System.nanoTime();
			}
		}
	}
	
	private static long mix64(long z) {
		z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
		z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
		return z ^ (z >>> 33);
	}
	
	private static final long multiplier = 0x5DEECE66DL;
	private static final long addend = 0xBL;
	private static final long mask = (1L << 48) - 1;
	
	private long rnd;
	
	boolean initialized;
	
	private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;
	
	ThreadLocalRandom() {
		super(newSeed());
		initialized = true;
	}
	
	public static ThreadLocalRandom current() {
		return InternalThreadLocalMap.get().random();
	}
	
	@Override
	public void setSeed(long seed) {
		if (initialized) {
			throw new UnsupportedOperationException();
		}
		rnd = (seed ^ multiplier) & mask;
	}
	
	@Override
	protected int next(int bits) {
		rnd = (rnd * multiplier + addend) & mask;
		return (int) (rnd >>> (48 - bits));
	}
	
	public int nextInt(int least, int bound) {
		if (least >= bound) {
			throw new IllegalArgumentException();
		}
		return nextInt(bound - least) + least;
	}
	
	public long nextLong(long n) {
		checkPositive(n, "n");
		
		long offset = 0;
		while (n >= Integer.MAX_VALUE) {
			int bits = next(2);
			long half = n >>> 1;
			long nextn = ((bits & 2) == 0)  ? half : n - half;
			if ((bits & 1) == 0) {
				offset += n - nextn;
			}
			n = nextn;
		}
		return offset + nextInt((int) n);
	}
	
	public long nextLong(long least, long bound) {
		if (least >= bound) {
			throw new IllegalArgumentException();
		}
		return nextLong(bound - least) + least;
	}
	
	public double nextDouble(double n) {
		checkPositive(n, "n");
		return nextDouble() * n;
	}
	
	public double nextDouble(double least, double bound) {
		if (least >= bound) {
			throw new IllegalArgumentException();
		}
		return nextDouble() * (bound - least) + least;
	}
	
	private static final long serialVersionUID = -5851777807851030925L;
}
