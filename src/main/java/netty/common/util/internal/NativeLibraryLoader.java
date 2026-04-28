package netty.common.util.internal;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.security.AccessController;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;

import netty.common.util.CharsetUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public final class NativeLibraryLoader {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(NativeLibraryLoader.class);
	
	private static final String NATIVE_RESOURCE_HOME = "META-INF/native/";
	private static final File WORKDIR;
	private static final boolean DELETE_NATIVE_LIB_AFTER_LOADING;
	private static final boolean TRY_TO_PATCH_SHADED_ID;
	private static final boolean DETECT_NATIVE_LIBRARY_DUPLICATES;
	
	private static final byte[] UNIQUE_ID_BYTES = 
			"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes(CharsetUtil.US_ASCII);
	static {
		String workdir = SystemPropertyUtil.get("io.netty.native.workdir");
		if (workdir != null) {
			File f = new File(workdir);
			if (!f.exists() && !f.mkdirs()) {
				throw new ExceptionInInitializerError(
						new IOException("Custom native workdir mkdirs failed: " + workdir));
			}
			
			try {
				f = f.getAbsoluteFile();
			} catch (Exception ignored) {
				
			}
			WORKDIR = f;
			logger.debug("-Dio.netty.native.workdir: " + WORKDIR);
		} else {
			WORKDIR = PlatformDependent.tmpdir();
			logger.debug("-Dio.netty.native.workdir: " + WORKDIR + " (io.netty.tmpdir)");
		}
		
		DELETE_NATIVE_LIB_AFTER_LOADING = SystemPropertyUtil.getBoolean(
				"-io.netty.native.deleteLibAfterLoading", true);
		logger.debug("-Dio.netty.native.deleteLibAfterLoading: {}", DELETE_NATIVE_LIB_AFTER_LOADING);
		
		TRY_TO_PATCH_SHADED_ID = SystemPropertyUtil.getBoolean(
				"-io.netty.navite.tryPatchShadedId", true);
		logger.debug("-Dio.netty.native.tryPatchShadedId: {}", TRY_TO_PATCH_SHADED_ID);
		
		DETECT_NATIVE_LIBRARY_DUPLICATES = SystemPropertyUtil.getBoolean(
				"-io,netty.native.detectNativeLibraryDuplicates", true);
		logger.debug("-Dio.netty.detectNativeLibraryDuplicates: {}", DETECT_NATIVE_LIBRARY_DUPLICATES);
	}
	
	public static void loadFirstAvailable(ClassLoader loader, String... names) {
		List<Throwable> suppressed = new ArrayList<Throwable>();
		for (String name : names) {
			try {
				load(name, loader);
				logger.debug("Loaded library with name: '{}'", name);
				return;
			} catch (Throwable t) {
				suppressed.add(t);
			}
		}
		
		IllegalArgumentException iae = 
				new IllegalArgumentException("Failed to load any of the given libraries: " + Arrays.toString(names));
		ThrowableUtil.addSuppressedAndClear(iae, suppressed);
		throw iae;
	}
	
	private static String calculateMangledPackagePrefix() {
		String maybeShaded = NativeLibraryLoader.class.getName();
		String expected = "io!netty!util!internal!NativeLibraryLoader".replace('!', '.');
		if (!maybeShaded.endsWith(expected)) {
			throw new UnsatisfiedLinkError(String.format(
					"could not find prefix added to %s to get %s. When shading, only adding a "
					+ "package prefix is supported", expected, maybeShaded));
		}
		return maybeShaded.substring(0, maybeShaded.length() - expected.length())
				.replace("_", "_1")
				.replace('.', '_');
	}
	
	public static void load(String originalName, ClassLoader loader) {
		String managedPackagePrefix = calculateMangledPackagePrefix();
		String name = managedPackagePrefix + originalName;
		List<Throwable> suppressed = new ArrayList<>();
		try {
			loadLibrary(loader, name, false);
			return;
		} catch (Throwable ex) {
			suppressed.add(ex);
		}
		
		String libname = System.mapLibraryName(name);
		String path = NATIVE_RESOURCE_HOME + libname;
		
		File tmpFile = null;
		URL url = getResource(path, loader);
		try {
			if (url == null) {
				if (PlatformDependent.isOsx()) {
					String filename = path.endsWith(".jnilib") ? NATIVE_RESOURCE_HOME + "lib" + name + ".dynlib" : 
						NATIVE_RESOURCE_HOME + "lib" + name + ".jnilib";
					url = getResource(filename, loader);
					if (url == null) {
						FileNotFoundException fnf = new FileNotFoundException(filename);
						ThrowableUtil.addSuppressedAndClear(fnf, suppressed);
						throw fnf;
					}
				} else {
					FileNotFoundException fnf = new FileNotFoundException(path);
					ThrowableUtil.addSuppressedAndClear(fnf, suppressed);
					throw fnf;
				}
			}
			
			int index = libname.lastIndexOf('.');
			String prefix = libname.substring(0, index);
			String suffix = libname.substring(index);
			
			tmpFile = PlatformDependent.createTempFile(prefix, suffix, WORKDIR);
			try (InputStream in = url.openStream();
				OutputStream out = new FileOutputStream(tmpFile)) {
					byte[] buffer = new byte[8192];
					int length;
					while ((length = in.read(buffer)) > 0) {
						out.write(buffer, 0, length);
					}
					out.flush();
					
					if (shouldSharedLibraryIdBePatched(managedPackagePrefix)) {
						tryPatchShadedLibraryIdAndSign(tmpFile, originalName);
				}
			}
			
			loadLibrary(loader, tmpFile.getPath(), true);
		} catch (UnsatisfiedLinkError e) {
			try {
				if (tmpFile != null && tmpFile.isFile() && tmpFile.canRead() &&
						!NoexecVolumeDetector.canExecuteExecutable(tmpFile)) {
					String message = String.format(
							"%s exists but cannot be executed even when execute permissions set; " +
									"check volume for \"noexec\" flag; use -D%s=[path] " + 
									"to set native working directory separately.", 
									tmpFile.getPath(), "io.netty.native.workdir");
					logger.info(message);
					suppressed.add(ThrowableUtil.unknownStackTrace(
							new UnsatisfiedLinkError(message), NativeLibraryLoader.class, "load"));
				}
			} catch (Throwable t) {
				suppressed.add(t);
				logger.debug("Error checking if {} is on a file store mounted with noexec", tmpFile, t);
			}
			ThrowableUtil.addSuppressedAndClear(e, suppressed);
			throw e;
		} catch (Exception e) {
			UnsatisfiedLinkError ule = new UnsatisfiedLinkError("could not load a native library: " + name);
			ule.initCause(e);
			ThrowableUtil.addSuppressedAndClear(ule, suppressed);
			throw ule;
		} finally {
			if (tmpFile != null && (!DELETE_NATIVE_LIB_AFTER_LOADING || !tmpFile.delete())) {
				tmpFile.deleteOnExit();
			}
		}
	}
	
	private static URL getResource(String path, ClassLoader loader) {
		final Enumeration<URL> urls;
		try {
			if (loader == null) {
				urls = ClassLoader.getSystemResources(path);
			} else {
				urls = loader.getResources(path);
			} 
		} catch (IOException iox) {
			throw new RuntimeException("An error occurred while getting the resources for " + path, iox);
		}
		
		List<URL> urlsList = Collections.list(urls);
		int size = urlsList.size();
		switch (size) {
		case 0:
			return null;
		case 1:
			return urlsList.get(0);
		default:
			if (DETECT_NATIVE_LIBRARY_DUPLICATES) {
				try {
					MessageDigest md = MessageDigest.getInstance("SHA-256");
					URL url = urlsList.get(0);
					byte[] digest = digest(md, url);
					boolean allSame = true;
					if (digest != null) {
						for (int i =1; i < size; i++) {
							byte[] digest2 = digest(md, urlsList.get(i));
							if (digest2 == null || !Arrays.equals(digest, digest2)) {
								allSame = false;
								break;
							}
						}
					} else {
						allSame = false;
					}
					if (allSame) {
						return url;
					}
				} catch (NoSuchAlgorithmException e) {
					logger.debug("Don't support SHA-256, can't check if resources have same content.", e);
				}
				
				throw new IllegalStateException(
						"Multiple resources found for '" + path + "' with different content: " + urlsList);
			} else { 
				logger.warn("Multiple resource found for '" + path + "' with differrent content: " + 
						urlsList + ". Please fix your dependency graph.");
				return urlsList.get(0);
			}
		}
	}
	
	private static byte[] digest(MessageDigest digest, URL url) {
		try (InputStream in = url.openStream()) {
			byte[] bytes = new byte[8192];
			int i;
			while ((i = in.read(bytes)) != -1) {
				digest.update(bytes, 0, i);
			}
			return digest.digest();
		} catch (IOException e) {
			logger.debug("Can't read resource.", e);
			return null;
		}
	}
	
	static void tryPatchShadedLibraryIdAndSign(File libraryFile, String originalName) {
		if (!new File("/Library/Developer/CommandLineTools").exists()) {
			logger.debug("Can't patch shaded library id as CommandLineTools are not installed." + 
					" Consider installing CommandLineTools with 'xcode-slect --install'");
			return;
		}
		
		String newId = new String(generateUniqueId(originalName.length()), CharsetUtil.UTF_8);
		if (!tryExec("install_name_tool -id " + newId + " " + libraryFile.getAbsolutePath())) {
			return;
		}
		tryExec("codesign -s - " + libraryFile.getAbsolutePath());
	}
	
	private static boolean tryExec(String cmd) {
		try {
			int exitValue = Runtime.getRuntime().exec(cmd).waitFor();
			if (exitValue != 0) {
				logger.debug("Execution of '{}' failed: ", cmd, exitValue);
				return false;
			}
			logger.debug("Execution of '{}' succeed: {}", cmd, exitValue);
			return true;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (IOException e) {
			logger.info("Execution of '{}' failed.", cmd, e);
		} catch (SecurityException e) {
			logger.error("Execution of '{}' failed.", cmd, e);
		}
		return false;
	}
	
	private static boolean shouldSharedLibraryIdBePatched(String packagePrefix) {
		return TRY_TO_PATCH_SHADED_ID && PlatformDependent.isOsx() && !packagePrefix.isEmpty();
	}
	
	private static byte[] generateUniqueId(int length) {
		byte[] idBytes = new byte[length];
		for (int i = 0; i < idBytes.length; i++) {
			idBytes[i] =  UNIQUE_ID_BYTES[ThreadLocalRandom.current()
			                              .nextInt(UNIQUE_ID_BYTES.length)];
		}
		return idBytes;
	}
	
	private static void loadLibrary(final ClassLoader loader, final String name, final boolean absolute) {
		Throwable suppressed = null;
		try {
			try {
				final Class<?> newHelper = tryToLoadClass(loader, NativeLibraryUtil.class);
				loadLibraryByHelper(newHelper, name, absolute);
				logger.debug("Successfully loaded the library {}", name);
				return;
			} catch(UnsatisfiedLinkError e) {
				suppressed = e;
			} catch (Exception e) {
				suppressed = e;
			}
			NativeLibraryUtil.loadLibrary(name, absolute);
			logger.debug("Successfully loaded the library {}", name);
		} catch (NoSuchMethodError nsme) {
			if (suppressed != null) {
				ThrowableUtil.addSuppressed(nsme, suppressed);
			}
			throw new LinkageError(
					"Possible multiple incompatible native libraries on the classpath for '" + name + "'?", nsme);
		} catch (UnsatisfiedLinkError ule) {
			if (suppressed != null) {
				ThrowableUtil.addSuppressed(ule, suppressed);
			}
			throw ule;
		}
	}
	
	private static void loadLibraryByHelper(final Class<?> helper, final String name, final boolean absolute)
		throws UnsatisfiedLinkError {
		Object ret = AccessController.doPrivileged(new PrivilegedAction<Object>() {
			@Override
			public Object run() {
				try {
					Method method = helper.getMethod("loadLibrary", String.class, boolean.class);
					method.setAccessible(true);
					return method.invoke(null, name, absolute);
				} catch (Exception e) {
					return e;
				}
			}
		});
		if (ret instanceof Throwable) {
			Throwable t = (Throwable) ret;
			assert !(t instanceof UnsatisfiedLinkError) : t + " should be a wrapped throwable";
			Throwable cause = t.getCause();
			if (cause instanceof UnsatisfiedLinkError) {
				throw (UnsatisfiedLinkError) cause;
			}
			UnsatisfiedLinkError ule = new UnsatisfiedLinkError(t.getMessage());
			ule.initCause(t);
			throw ule;
		}
	}
	
	private static Class<?> tryToLoadClass(final ClassLoader loader, final Class<?> helper)
		throws ClassNotFoundException {
		try {
			return Class.forName(helper.getName(), false, loader);
		} catch (ClassNotFoundException e1) {
			if (loader == null) {
				throw e1;
			}
			try {
				final byte[] classBinary =  classToByteArray(helper);
				return AccessController.doPrivileged(new PrivilegedAction<Class<?>>() {
					@Override
					public Class<?> run() {
						try {
							Method defineClass = ClassLoader.class.getDeclaredMethod("defineClass", String.class,
									byte[].class, int.class, int.class);
							defineClass.setAccessible(true);
							return (Class<?>) defineClass.invoke(loader, helper.getName(), classBinary, 0,
									classBinary.length);
						} catch (Throwable e) {
							throw new IllegalStateException("Define class failed!", e);
						}
					}
				});
			} catch (ClassNotFoundException | RuntimeException | Error e2) {
				ThrowableUtil.addSuppressed(e2, e1);
				throw e2;
			}
		}
	}
	
	private static byte[] classToByteArray(Class<?> clazz) throws ClassNotFoundException {
		String fileName = clazz.getName();
		int lastDot = fileName.lastIndexOf('.');
		if (lastDot > 0) {
			fileName = fileName.substring(lastDot + 1);
		}
		URL classUrl = clazz.getResource(fileName + ".class");
		if (classUrl == null) {
			throw new ClassNotFoundException(clazz.getName());
		}
		byte[] buf = new byte[1024];
		ByteArrayOutputStream out = new ByteArrayOutputStream(4096);
		try (InputStream in = classUrl.openStream()) {
			for (int r; (r = in.read(buf)) != -1;) {
				out.write(buf, 0, r);
			}
			return out.toByteArray();
		} catch (IOException ex) {
			throw new ClassNotFoundException(clazz.getName(), ex);
		}
	}
	
	private NativeLibraryLoader() {
		
	}
	
	private static final class NoexecVolumeDetector {
		private static boolean canExecuteExecutable(File file) throws IOException {
			if (file.canExecute()) {
				return true;
			}
			Set<PosixFilePermission> existingFilePermissions = Files.getPosixFilePermissions(file.toPath());
			Set<PosixFilePermission> executePermissions = 
					EnumSet.of(PosixFilePermission.OWNER_EXECUTE,
							PosixFilePermission.GROUP_EXECUTE,
							PosixFilePermission.OTHERS_EXECUTE);
			if (existingFilePermissions.containsAll(executePermissions)) {
				return false;
			}
			Set<PosixFilePermission> newPermissions = EnumSet.copyOf(existingFilePermissions);
			newPermissions.addAll(executePermissions);
			Files.setPosixFilePermissions(file.toPath(), newPermissions);
			return file.canExecute();
		}
		
		private NoexecVolumeDetector() {
			
		}
	}
}
