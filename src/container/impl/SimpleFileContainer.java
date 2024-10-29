package container.impl;

import container.Container;
import io.FixedSizeSerializer;
import util.MetaData;
import util.ContainerRuntimeException;  // Ensure this is your custom runtime exception

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.NoSuchElementException;

public class SimpleFileContainer<Value> implements Container<Long, Value> {

	private final Path dataFilePath;
	private final Path metadataFilePath;
	private final FixedSizeSerializer<Value> serializer;
	private FileChannel dataChannel;
	private FileChannel metadataChannel;
	private MetaData metaData;
	private long nextKey;
	private boolean isOpen;

	public SimpleFileContainer(Path directory, String filenamePrefix, FixedSizeSerializer<Value> serializer) {
		this.dataFilePath = directory.resolve(filenamePrefix + "_data.bin");
		this.metadataFilePath = directory.resolve(filenamePrefix + "_metadata.bin");
		this.serializer = serializer;
		this.metaData = new MetaData();
		this.nextKey = 0;
		this.isOpen = false;
	}

	@Override
	public void open() {
		if (!isOpen) {
			try {
				dataChannel = FileChannel.open(dataFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
				metadataChannel = FileChannel.open(metadataFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
				loadMetaData();
				isOpen = true;
			} catch (IOException e) {
				throw new ContainerRuntimeException("Failed to open container files.", e);
			}
		}
	}

	@Override
	public void close() {
		if (isOpen) {
			try {
				dataChannel.close();
				metadataChannel.close();
				isOpen = false;
			} catch (IOException e) {
				throw new ContainerRuntimeException("Failed to close container files.", e);
			}
		}
	}

	@Override
	public MetaData getMetaData() {
		if (!isOpen) {
			throw new IllegalStateException("Container is closed.");
		}
		return metaData;
	}

	private void loadMetaData() {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		try {
			if (metadataChannel.size() >= Long.BYTES) {
				metadataChannel.read(buffer, 0);
				buffer.flip();
				nextKey = buffer.getLong();
			}
		} catch (IOException e) {
			throw new ContainerRuntimeException("Failed to load metadata.", e);
		}
	}

	private void saveMetaData() {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(nextKey);
		buffer.flip();
		try {
			metadataChannel.write(buffer, 0);
		} catch (IOException e) {
			throw new ContainerRuntimeException("Failed to save metadata.", e);
		}
	}

	@Override
	public Long reserve() {
		if (!isOpen) {
			throw new IllegalStateException("Container is closed.");
		}
		long reservedKey = nextKey++;
		saveMetaData();
		return reservedKey;
	}

	@Override
	public Value get(Long key) {
		if (!isOpen) {
			throw new IllegalStateException("Container is closed.");
		}
		try {
			long position = key * (serializer.getSerializedSize() + 1);
			ByteBuffer buffer = ByteBuffer.allocate(serializer.getSerializedSize() + 1);
			dataChannel.read(buffer, position);
			buffer.flip();
			byte deletedMarker = buffer.get();
			if (deletedMarker == 1) {
				throw new NoSuchElementException("Key " + key + " has been deleted.");
			}
			return serializer.deserialize(buffer);
		} catch (IOException e) {
			throw new NoSuchElementException("Failed to read the value.");
		}
	}

	@Override
	public void update(Long key, Value value) {
		if (!isOpen) {
			throw new IllegalStateException("Container is closed.");
		}
		long position = key * (serializer.getSerializedSize() + 1);
		ByteBuffer buffer = ByteBuffer.allocate(serializer.getSerializedSize() + 1);
		buffer.put((byte) 0); // 0 indicates the entry is not deleted
		serializer.serialize(value, buffer);
		buffer.flip();
		try {
			dataChannel.write(buffer, position);
		} catch (IOException e) {
			throw new IllegalStateException("Failed to update the value.");
		}
	}

	@Override
	public void remove(Long key) {
		if (!isOpen) {
			throw new IllegalStateException("Container is closed.");
		}
		long position = key * (serializer.getSerializedSize() + 1);
		ByteBuffer buffer = ByteBuffer.allocate(1);
		buffer.put((byte) 1); // 1 indicates the entry is deleted
		buffer.flip();
		try {
			dataChannel.write(buffer, position);
		} catch (IOException e) {
			throw new IllegalStateException("Failed to mark the value as deleted.");
		}
	}
}
