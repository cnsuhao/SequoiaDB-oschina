package com.sequoiadb.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bson.io.OutputBuffer;

public class ByteOutputBuffer extends OutputBuffer {

	private ByteBuffer byteBuffer = null;

	public ByteOutputBuffer(ByteBuffer bufferImpl) {
		byteBuffer = bufferImpl;
	}

	@Override
	public void write(byte[] b) {
		byteBuffer.put(b);
	}

	@Override
	public void write(byte[] b, int off, int len) {
		byteBuffer.put(b, off, len);
	}

	@Override
	public void write(int b) {
		//_ensure(1);
		byteBuffer.put((byte) (0xFF & b));
	}

	@Override
	public void writeInt(int x) {
		byteBuffer.putInt(x);
	}

	private static final int swapInt32(int i) {
		return (i & 0xFF) << 24 
				| (i >> 8 & 0xFF) << 16 
				| (i >> 16 & 0xFF) << 8
				| (i >> 24 & 0xFF);
	}

	@Override
	public void writeIntBE(int x) {
		int xBE = swapInt32(x);
		byteBuffer.putInt(xBE);
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
		byteBuffer.putLong(x);
	}

	@Override
	public void writeDouble(double x) {
		byteBuffer.putDouble(x);
	}

	@Override
	public int getPosition() {
		return byteBuffer.position();
	}

	@Override
	public void setPosition(int position) {
		byteBuffer.position(position);
	}

	@Override
	public void seekEnd() {
		byteBuffer.position(byteBuffer.capacity());

	}

	@Override
	public void seekStart() {
		byteBuffer.position(0);

	}

	@Override
	public int size() {
		return byteBuffer.capacity();
	}

	@Override
	public int pipe(OutputStream out) throws IOException {
		out.write(byteBuffer.array());
		return byteBuffer.capacity();
	}

}
