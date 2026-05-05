package netty.buffer;

import java.nio.ByteBuffer;

public class CompositeByteBuf extends AbstractReferenceCountedByteBuf implements Iterable<ByteBuf> {
	
	
	
	private static final class Component {
		final ByteBuf srcBuf;
		final ByteBuf buf;
		final AbstractByteBuf abuf;
		
		int srcAdjustment;
		int adjustment;
		
		int offset;
		int endOffset;
		
		private ByteBuf slice;
		
		Component(ByteBuf srcBuf, int srcOffset, ByteBuf buf, int bufOffset,
				int offset, int len, ByteBuf slice) {
			this.srcBuf = srcBuf;
			this.srcAdjustment = srcOffset - offset;
			this.buf = buf;
			this.abuf = buf instanceof AbstractByteBuf ? (AbstractByteBuf) buf : null;
			this.adjustment = bufOffset - offset;
			this.offset = offset;
			this.endOffset = offset + len;
			this.slice = slice;
		}
		
		int srcIdx(int index) {
			return index + srcAdjustment;
		}
		
		int idx(int index) {
			return index + adjustment;
		}
		
		int length() {
			return endOffset - offset;
		}
		
		void reposition(int newOffset) {
			int move = newOffset - offset;
			endOffset += move;
			srcAdjustment -= move;
			adjustment -= move;
			offset = newOffset;
		}
		
		void transferTo(ByteBuf dst) {
			dst.writeBytes(buf, idx(offset), length());
			free();
		}
		
		ByteBuf slice() {
			ByteBuf s = slice;
			if (s == null) {
				slice = s = srcBuf.slice(srcIdx(offset), length());
			}
			return s;
		}
		
		ByteBuf duplicate() {
			return srcBuf.duplicate();
		}
		
		ByteBuffer internalNioBuffer(int index, int length) {
			return srcBuf.internalNioBuffer(srcIdx(index), length);
		}
		
		void free() {
			slice = null;
			srcBuf.release();
		}
	}
}
