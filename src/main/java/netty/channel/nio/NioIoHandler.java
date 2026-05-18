package netty.channel.nio;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import netty.channel.ChannelException;
import netty.channel.ChannelHandlerContext;
import netty.channel.DefaultSelectStrategyFactory;
import netty.channel.IoHandle;
import netty.channel.IoHandler;
import netty.channel.IoHandlerContext;
import netty.channel.IoHandlerFactory;
import netty.channel.IoOps;
import netty.channel.IoRegistration;
import netty.channel.SelectStrategy;
import netty.channel.SelectStrategyFactory;
import netty.common.util.IntSupplier;
import netty.common.util.concurrent.ThreadAwareExecutor;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.ReflectionUtil;
import netty.common.util.internal.StringUtil;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public final class NioIoHandler implements IoHandler {
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioIoHandler.class);
	
	private static final int CLEANUP_INTERVAL = 256;
	
	private static final boolean DISABLE_KEY_SET_OPTIMIZATION = 
			SystemPropertyUtil.getBoolean("io.netty.noKeySetOptimization", false);
	
	private static final int MIN_PREMATURE_SELECTOR_RETURNS = 3;
	private static final int SELECTOR_AUTO_REBUILD_THRESHOLD;
	
	private final IntSupplier selectNonSupplier = new IntSupplier() {
		@Override
		public int get() throws Exception {
			return selectNow();
		}
	};
	
	static {
		int selectorAutoRebuildThreshold = SystemPropertyUtil.getInt("io.netty.selectorAutoRebuildThreshold", 512);
		if (selectorAutoRebuildThreshold < MIN_PREMATURE_SELECTOR_RETURNS) {
			selectorAutoRebuildThreshold = 0;
		}
		
		SELECTOR_AUTO_REBUILD_THRESHOLD = selectorAutoRebuildThreshold;
		
		if (logger.isDebugEnabled()) {
			logger.debug("-Dio.netty.noKeySetOptimization: {}", DISABLE_KEY_SET_OPTIMIZATION);
			logger.debug("-Dio.netty.selectorAutoRebuildThreshold: {}", SELECTOR_AUTO_REBUILD_THRESHOLD);
		}
	}
	
	private Selector selector;
	private Selector unwrappedSelector;
	private SelectedSelectionKeySet selectedKeys;
	
	private final SelectorProvider provider;
	
	private final AtomicBoolean wakenUp = new AtomicBoolean();
	
	private final SelectStrategy selectStrategy;
	private final ThreadAwareExecutor executor;
	private int cancelledKeys;
	private boolean needsToSelectAgain;
	
	private NioIoHandler(ThreadAwareExecutor executor, SelectorProvider selectorProvider,
			SelectStrategy strategy) {
		this.executor = ObjectUtil.checkNotNull(executor, "executor");
		this.provider = ObjectUtil.checkNotNull(selectorProvider, "selectorProvider");
		this.selectStrategy = ObjectUtil.checkNotNull(strategy, "selectStrategy");
		final SelectorTuple selectorTuple = openSelector();
		this.selector = selectorTuple.selector;
		this.unwrappedSelector = selectorTuple.unwrappedSelector;
	}
	
	private static final class SelectorTuple {
		final Selector unwrappedSelector;
		final Selector selector;
		
		SelectorTuple(Selector unwrappedSelector) {
			this.unwrappedSelector = unwrappedSelector;
			this.selector = unwrappedSelector;
		}
		
		SelectorTuple(Selector unwrappedSelector, Selector selector) {
			this.unwrappedSelector = unwrappedSelector;
			this.selector = selector;
		}
	}
	
	private SelectorTuple openSelector() {
		final Selector unwrappedSelector;
		try {
			unwrappedSelector = provider.openSelector();
		} catch (IOException e) {
			throw new ChannelException("failed to open a new selector", e);
		}
		
		if (DISABLE_KEY_SET_OPTIMIZATION)  {
			return new SelectorTuple(unwrappedSelector);
		}
		
		@SuppressWarnings({ "deprecation", "removal" })
		Object maybeSelectorImplClass = AccessController.doPrivileged(new PrivilegedAction<Object>() {
			@Override
			public Object run() {
				try {
					return Class.forName(
							"sun.nio.ch.SelectorImpl",
							false,
							PlatformDependent.getSystemClassLoader());
				} catch (Throwable cause) {
					return cause;
				}
			}
		});
		
		if (!(maybeSelectorImplClass instanceof Class) ||
				!((Class<?>) maybeSelectorImplClass).isAssignableFrom(unwrappedSelector.getClass())) {
			if (maybeSelectorImplClass instanceof Throwable) {
				Throwable t = (Throwable) maybeSelectorImplClass;
				logger.trace("failed to instrument a special java.util.Set into: {}", unwrappedSelector, t);
			}
			return new SelectorTuple(unwrappedSelector);
		}
		
		final Class<?> selectorImplClass = (Class<?>) maybeSelectorImplClass;
		final SelectedSelectionKeySet selectedKeySet = new SelectedSelectionKeySet();
		
		@SuppressWarnings({ "removal", "deprecation" })
		Object maybeException = AccessController.doPrivileged(new PrivilegedAction<Object>() {
			@Override
			public Object run() {
				try {
					Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
					Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");
					
					if (PlatformDependent.javaVersion() >= 9 && PlatformDependent.hasUnsafe()) {
						long selectedKeysFieldOffset = PlatformDependent.objectFieldOffset(selectedKeysField);
						long publicSelectedKeysFieldOffset = 
								PlatformDependent.objectFieldOffset(publicSelectedKeysField);
						if (selectedKeysFieldOffset != -1 && publicSelectedKeysFieldOffset != -1) {
							PlatformDependent.putObject(
									unwrappedSelector, selectedKeysFieldOffset, selectedKeySet);
							PlatformDependent.putObject(
									unwrappedSelector, publicSelectedKeysFieldOffset, selectedKeySet);
							return null;
						}
					}
					Throwable cause = ReflectionUtil.trySetAccessible(selectedKeysField, true);
					if (cause != null) {
						return cause;
					}
					cause = ReflectionUtil.trySetAccessible(publicSelectedKeysField, true);
					if (cause != null) {
						return cause;
					}
					
					selectedKeysField.set(unwrappedSelector, selectedKeySet);
					publicSelectedKeysField.set(unwrappedSelector, selectedKeySet);
					return null;
						
				} catch (NoSuchFieldException | IllegalAccessException e) {
						return e;
				}
			}
		});
		
		if (maybeException instanceof Exception) {
			selectedKeys = null;
			Exception e = (Exception) maybeException;
			logger.trace("failed to instrument a special java.util.Set into: {}", unwrappedSelector, e);
			return new SelectorTuple(unwrappedSelector);
		}
		selectedKeys = selectedKeySet;
		logger.trace("instrument a special java.util.Set into: {}", unwrappedSelector);
		return new SelectorTuple(unwrappedSelector,
				new SelectedSelectionKeySetSelector(unwrappedSelector, selectedKeySet));
	}
	
	public SelectorProvider selectorProvider() {
		return provider;
	}
	
	Selector selector() {
		return selector;
	}
	
	int numRegistered() {
		return selector().keys().size() - cancelledKeys;
	}
	
	Set<SelectionKey> registeredSet() {
		return selector().keys();
	}
	
	void rebuildSelector0() {
		final Selector oldSelector = selector;
		final SelectorTuple newSelectorTuple;
		
		if (oldSelector == null) {
			return;
		}
		
		try {
			newSelectorTuple = openSelector();
		} catch (Exception e) {
			logger.warn("Failed to create a new Selector.", e);
			return;
		}
		
		int nChannels = 0;
		for (SelectionKey key : oldSelector.keys()) {
			DefaultNioRegistration handle = (DefaultNioRegistration) key.attachment();
			try {
				if (!key.isValid() || key.channel().keyFor(newSelectorTuple.unwrappedSelector) != null) {
					continue;
				}
				
				handle.register(newSelectorTuple.unwrappedSelector);
				nChannels++;
			} catch (Exception e) {
				logger.warn("Failed to re-register a NioHandle to the new Selector.", e);
				handle.cancel();
			}
		}
		
		selector = newSelectorTuple.selector;
		unwrappedSelector = newSelectorTuple.unwrappedSelector;
		
		try {
			oldSelector.close();
		} catch (Throwable t) {
			if (logger.isWarnEnabled()) {
				logger.warn("Failed to close the old selector.", t);
			}
		}
		
		if (logger.isInfoEnabled()) {
			logger.info("Migrated " + nChannels + " channel(s) to the new Selector.");
		}
	}
	
	private static NioIoHandle nioHandle(IoHandle handle) {
		if (handle instanceof NioIoHandle) {
			return (NioIoHandle) handle;
		}
		throw new IllegalArgumentException("IoHandle of type " + StringUtil.simpleClassName(handle) + " not supported");
	}
	
	private static NioIoOps cast(IoOps ops) {
		if (ops instanceof NioIoOps) {
			return (NioIoOps) ops;
		}
		throw new IllegalArgumentException("IoOps of type " + StringUtil.simpleClassName(ops) + " not supported");
	}
	
	final class DefaultNioRegistration implements IoRegistration {
		private final AtomicBoolean canceled = new AtomicBoolean();
		private final NioIoHandle handle;
		private volatile SelectionKey key;
		
		DefaultNioRegistration(ThreadAwareExecutor executor, NioIoHandle handle, NioIoOps initialOps, Selector selector) 
				throws IOException {
			this.handle = handle;
			key = handle.selectableChannel().register(selector, initialOps.value, this);
		}
		
		NioIoHandle handle() {
			return handle;
		}
		
		void register(Selector selector) throws IOException {
			SelectionKey newKey = handle.selectableChannel().register(selector, key.interestOps(), this);
			key.cancel();
			key = newKey;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public <T> T attachment() {
			return (T) key;
		}
		
		@Override
		public boolean isValid() {
			return !canceled.get() && key.isValid();
		}
		
		@Override
		public long submit(IoOps ops) {
			if (!isValid()) {
				return -1;
			}
			int v = cast(ops).value;
			key.interestOps(v);
			return v;
		}
		
		@Override
		public boolean cancel() {
			if (!canceled.compareAndSet(false, true)) {
				return false;
			}
			key.cancel();
			cancelledKeys++;
			if (cancelledKeys >= CLEANUP_INTERVAL) {
				cancelledKeys = 0;
				needsToSelectAgain = true;
			}
			handle.unregistered();
			return true;
		}
		
		void close() {
			cancel();
			try {
				handle.close();
			} catch (Exception e) {
				logger.debug("Exception during closing " + handle, e);
			}
		}
		
		void handle(int ready) {
			if (!isValid()) {
				return;
			}
			handle.handle(this, NioIoOps.eventOf(ready));
		}
	}
	
	@Override
	public IoRegistration register(IoHandle handle)
			throws IOException {
		NioIoHandle nioHandle = nioHandle(handle);
		NioIoOps ops = NioIoOps.NONE;
		boolean selected = false;
		for (;;) {
			try {
				IoRegistration registration = new DefaultNioRegistration(executor, nioHandle, ops, unwrappedSelector());
				handle.registered();
				return registration;
			} catch (CancelledKeyException e) {
				if (!selected) {
					selectNow();
					selected = true;
				} else {
					throw e;
				}
			}
		}
	}
	
	@Override
	public int run(IoHandlerContext context) {
		int handled = 0;
		try {
			try {
				switch (selectStrategy.calculateStrategy(selectNonSupplier, !context.canBlock())) {
				case SelectStrategy.CONTINUE:
					if (context.shouldReportActiveIoTime()) {
						context.reportActiveIoTime(0);
					}
					return 0;
				
				case SelectStrategy.BUSY_WAIT:
				
				case SelectStrategy.SELECT:
					select(context, wakenUp.getAndSet(false));
					
					if (wakenUp.get()) {
						selector.wakeup();
					}
				default:
				}
			} catch (IOException e) {
				rebuildSelector0();
				handleLoopException(e);
				return 0;
			}
			
			cancelledKeys = 0;
			needsToSelectAgain = false;
			
			if (context.shouldReportActiveIoTime()) {
				long activeIoStartTimeNanos = System.nanoTime();
				handled = processSelectedKeys();
				long activeIoEndTimeNanos = System.nanoTime();
				context.reportActiveIoTime(activeIoEndTimeNanos - activeIoStartTimeNanos);
			} else {
				handled = processSelectedKeys();
			}
		} catch (Error e) {
			throw e;
		} catch (Throwable t ) {
			handleLoopException(t);
		}
		return handled;
	}
	
	private static void handleLoopException(Throwable t) {
		logger.warn("Unexpected exception in the selector loop.", t);
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			
		}
	}
	
	private int processSelectedKeys() {
		if (selectedKeys != null) {
			return processSelectedKeysOptimized();
		} else {
			return processSelectedKeysPlain(selector.selectedKeys());
		}
	}
	
	@Override
	public void destroy() {
		try {
			selector.close();
		} catch (IOException e) {
			logger.warn("Failed to close a selector", e);
		}
	}
	
	private int processSelectedKeysPlain(Set<SelectionKey> selectedKeys) {
		if (selectedKeys.isEmpty()) {
			return 0;
		}
		
		Iterator<SelectionKey> i = selectedKeys.iterator();
		int handled = 0;
		for (;;) {
			final SelectionKey k = i.next();
			i.remove();
			
			processSelectedKey(k);
			++handled;
			
			if (!i.hasNext()) {
				break;
			}
			
			if (needsToSelectAgain) {
				selectAgain();
				selectedKeys = selector.selectedKeys();
				
				if (selectedKeys.isEmpty()) {
					break;
				} else {
					i = selectedKeys.iterator();
				}
			}
		}
		return handled;
	}
	
	private int processSelectedKeysOptimized() {
		int handled = 0;
		for (int i = 0; i < selectedKeys.size; ++i) {
			final SelectionKey k = selectedKeys.keys[i];
			selectedKeys.keys[i] = null;
			
			processSelectedKey(k);
			++handled;
			
			if (needsToSelectAgain) {
				selectedKeys.reset(i + 1);
				
				selectAgain();
				i = -1;
			}
		}
		return handled;
	}
	
	private void processSelectedKey(SelectionKey k) {
		final DefaultNioRegistration registration = (DefaultNioRegistration) k.attachment();
		if (!registration.isValid()) {
			try {
				registration.handle.close();
			} catch (Exception e) {
				logger.debug("Exception during closing " + registration.handle, e);
			}
			return;
		}
		registration.handle(k.readyOps());
	}
	
	@Override
	public void prepareToDestroy() {
		selectAgain();
		Set<SelectionKey> keys = selector.keys();
		Collection<DefaultNioRegistration> registrations = new ArrayList<>(keys.size());
		for (SelectionKey k : keys) {
			DefaultNioRegistration handle = (DefaultNioRegistration) k.attachment();
			registrations.add(handle);
		}
		
		for (DefaultNioRegistration reg : registrations) {
			reg.close();
		}
	}
	
	@Override
	public void wakeup() {
		if (!executor.isExecutorThread(Thread.currentThread()) && wakenUp.compareAndSet(false, true)) {
			selector.wakeup();
		}
	}
	
	@Override
	public boolean isCompatible(Class<? extends IoHandle> handleType) {
		return NioIoHandle.class.isAssignableFrom(handleType);
	}
	
	Selector unwrappedSelector() {
		return unwrappedSelector;
	}
	
	private void select(IoHandlerContext runner, boolean oldWakenUp) throws IOException {
		Selector selector = this.selector;
		try {
			int selectCnt = 0;
			long currentTimeNanos = System.nanoTime();
			final long delayNanos = runner.delayNanos(currentTimeNanos);
			long selectDeadLineNanos = Long.MAX_VALUE;
			if (delayNanos != Long.MAX_VALUE) {
				selectDeadLineNanos = currentTimeNanos + runner.delayNanos(currentTimeNanos);
			}
			for (;;) {
				final long timeoutMillis;
				if (delayNanos != Long.MAX_VALUE) {
					long millisBeforeDeadline = millisBeforeDeadline(selectDeadLineNanos, currentTimeNanos);
					if (millisBeforeDeadline <= 0) {
						if (selectCnt == 0) {
							selector.selectNow();
							selectCnt = 1;
						}
						break;
					}
					timeoutMillis = millisBeforeDeadline;
				} else {
					timeoutMillis = 0;
				}
				if  (!runner.canBlock() && wakenUp.compareAndSet(false, true)) {
					selector.selectNow();
					selectCnt = 1;
					break;
				}
				
				int selectedKeys = selector.select(timeoutMillis);
				selectCnt++;
				
				if (selectedKeys != 0 || oldWakenUp || wakenUp.get() || !runner.canBlock()) {
					break;
				}
				if (Thread.interrupted()) {
					if (logger.isDebugEnabled()) {
						logger.debug("Selector.select() returned prematurely because " + 
								"Thread.currentThread().interrupt() was called. Use " + 
								"NioHandler.shutdownGracefully() to shutdown the NioHandler.");
					}
					selectCnt = 1;
					break;
				}
				
				long time = System.nanoTime();
				if (time - TimeUnit.MILLISECONDS.toNanos(timeoutMillis) >= currentTimeNanos) {
					selectCnt = 1;
				} else if (SELECTOR_AUTO_REBUILD_THRESHOLD > 0 &&
						selectCnt >= SELECTOR_AUTO_REBUILD_THRESHOLD) {
					selector = selectRebuildSelector(selectCnt);
					selectCnt = 1;
					break;
				}
				currentTimeNanos = time;
			}
			
			if (selectCnt > MIN_PREMATURE_SELECTOR_RETURNS) {
				if (logger.isDebugEnabled()) {
					logger.debug("Selector.select() returned prematurely {} times in a row for Selector {}.",
							selectCnt - 1, selector);
				}
			}
		} catch (CancelledKeyException e) {
			if (logger.isDebugEnabled()) {
				logger.debug(CancelledKeyException.class.getSimpleName() + " raised by a Selector {} - JDK bug?",
						selector, e);
			}
		}
	}
	
	private static long millisBeforeDeadline(long selectDeadLineNanos, long currentTimeNanos) {
		assert selectDeadLineNanos != Long.MAX_VALUE;
		long nanosBeforeDeadline = selectDeadLineNanos = currentTimeNanos;
		if (nanosBeforeDeadline >= Long.MAX_VALUE - 500_000L) {
			return Long.MAX_VALUE / 1_000_000L;
		}
		return (nanosBeforeDeadline + 500_000L) / 1_000_000L;
	}
	
	int selectNow() throws IOException {
		try {
			return selector.selectNow();
		} finally {
			if (wakenUp.get()) {
				selector.wakeup();
			}
		}
	}
	
	private Selector selectRebuildSelector(int selectCnt) throws IOException {
		logger.warn(
				"Selector.select() returned prematurely {} times in a row; rebuilding Selector {}",
				selectCnt, selector);
		rebuildSelector0();
		Selector selector = this.selector;
		
		selector.selectNow();
		return selector;
	}
	
	private void selectAgain() {
		needsToSelectAgain = false;
		try {
			selector.selectNow();
		} catch (Throwable t) {
			logger.warn("Failed to update SelectionKeys.", t);
		}
	}
	
	public static IoHandlerFactory newFactory() {
		return newFactory(SelectorProvider.provider(), DefaultSelectStrategyFactory.INSTANCE);
	}
	
	public static IoHandlerFactory newFactory(SelectorProvider selectorProvider) {
		return newFactory(selectorProvider, DefaultSelectStrategyFactory.INSTANCE);
	}
	
	public static IoHandlerFactory newFactory(final SelectorProvider selectorProvider,
			final SelectStrategyFactory selectStrategyFactory) {
		ObjectUtil.checkNotNull(selectorProvider, "selectorProvider");
		ObjectUtil.checkNotNull(selectStrategyFactory, "selectStrategyFactory");
		return new IoHandlerFactory() {
			@Override
			public IoHandler newHandler(ThreadAwareExecutor executor) {
				return new NioIoHandler(executor, selectorProvider, selectStrategyFactory.newSelectStrategy());
			}
			
			@Override
			public boolean isChangingThreadSupported() {
				return true;
			}
		};
	}
}
