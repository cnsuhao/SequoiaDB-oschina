package com.sequoiadb.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
//import java.nio.ByteBuffer;

import org.apache.mina.core.buffer.IoBuffer;
import org.bson.io.OutputBuffer;

public class ByteOutputBuffer1 extends OutputBuffer {

	private IoBuffer ioBuffer = null;

	public ByteOutputBuffer1(IoBuffer bufferImpl) {
		ioBuffer = bufferImpl;
	}

	@Override
	public void write(byte[] b) {
		ioBuffer.put(b);
	}

	@Override
	public void write(byte[] b, int off, int len) {
		ioBuffer.put(b, off, len);
	}

	@Override
	public void write(int b) {
		//_ensure(1);
		ioBuffer.put((byte) (0xFF & b));
	}

	@Override
	public void writeInt(int x) {
		ioBuffer.putInt(x);
	}

	private static final int swapInt32(int i) {
		return (i & 0xFF) << 24 
				| (i >> 8 & 0xFF) << 16 
				| (i >> 16 & 0xFF) << 8
				| (i >> 24 & 0xFF);
	}

	@Override
	public void writeIntBE(int x) {
		ByteOrder order = ioBuffer.order();
		int xBE = ByteOrder.LITTLE_ENDIAN == order ? swapInt32(x) : x;
		ioBuffer.putInt(xBE);
	}

	@Override
	public void writeInt(int pos, int x) {
		final int save = getPosition();
		setPosition(pos);
		writeInt(x);
		setPosition(save);
	}

	@Override
	public void writeLong(long x) {
		ioBuffer.putLong(x);
	}

	@Override
	public void writeDouble(double x) {
		ioBuffer.putDouble(x);
	}

	@Override
	public int getPosition() {
		return ioBuffer.position();
	}

	@Override
	public void setPosition(int position) {
		ioBuffer.position(position);
	}

	@Override
	public void seekEnd() {
		ioBuffer.position(ioBuffer.capacity());

	}

	@Override
	public void seekStart() {
		ioBuffer.position(0);

	}

	@Override
	public int size() {
		return ioBuffer.capacity();
	}

	@Override
	public int pipe(OutputStream out) throws IOException {
		out.write(ioBuffer.array());
		return ioBuffer.capacity();
	}

}
