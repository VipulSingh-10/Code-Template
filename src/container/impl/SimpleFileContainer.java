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
	final Path FileDest;
	final Path MetaDataFileDest;

	MetaData MetaData;
	long next;
	boolean IfOpen;



	private final FixedSizeSerializer<Value> serializer;
	FileChannel StorageChannel;
	FileChannel MetaDataStorageChannel;


	public SimpleFileContainer(Path directory, String filenamePrefix, FixedSizeSerializer<Value> serializer) {
		this.FileDest = directory.resolve(filenamePrefix + "StorageData.bin");
		this.MetaDataFileDest = directory.resolve(filenamePrefix + "Metadata.bin");

		this.IfOpen = false; //initially false

		this.serializer = serializer;
		this.MetaData = new MetaData();
		this.next = 0;
	}

	@Override
	public void open() {
		if (!IfOpen) {
			try {
				StorageChannel = FileChannel.open(FileDest, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
				MetaDataStorageChannel = FileChannel.open(MetaDataFileDest, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
				loadMetaData();
				IfOpen = true;
			} catch (IOException e) {
				throw new ContainerRuntimeException("Unable to open", e);
			}
		}
	}

	@Override
	public void close() {
		if (IfOpen) {
			try {
				StorageChannel.close();
				MetaDataStorageChannel.close();
				IfOpen = false;
			} catch (IOException e) {
				throw new ContainerRuntimeException("Unable to close", e);
			}
		}
	}

	@Override
	public MetaData getMetaData() {
		if (!IfOpen) {
			throw new IllegalStateException("Container is closed.");
		}
		return MetaData;
	}

	private void loadMetaData() {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		try {
			if (MetaDataStorageChannel.size() >= Long.BYTES) {
				MetaDataStorageChannel.read(buffer, 0);
				buffer.flip();
				next = buffer.getLong();
			}
		} catch (IOException e) {
			throw new ContainerRuntimeException("No Metadata loaded", e);
		}
	}

	private void saveMetaData() {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(next);
		buffer.flip();
		try {
			MetaDataStorageChannel.write(buffer, 0);
		} catch (IOException e) {
			throw new ContainerRuntimeException("Saving failed", e);
		}
	}

	@Override
	public Long reserve() {
		if (!IfOpen) {
			throw new IllegalStateException("Container is closed.");
		}
		long reservedKey = next++;
		saveMetaData();
		return reservedKey;
	}

	@Override
	public Value get(Long key) {
		if (!IfOpen) {
			throw new IllegalStateException("Container is closed.");
		}
		try {
			long position = key * (serializer.getSerializedSize() + 1);
			ByteBuffer buffer = ByteBuffer.allocate(serializer.getSerializedSize() + 1);
			StorageChannel.read(buffer, position);
			buffer.flip();
			byte deletedMarker = buffer.get();
			if (deletedMarker == 1) {
				throw new NoSuchElementException( key + " Deleted");
			}
			return serializer.deserialize(buffer);
		} catch (IOException e) {
			throw new NoSuchElementException("Unable to read");
		}
	}

	@Override
	public void update(Long key, Value value) {
		if (!IfOpen) {
			throw new IllegalStateException("Container is closed.");
		}
		long position = key * (serializer.getSerializedSize() + 1);
		ByteBuffer buffer = ByteBuffer.allocate(serializer.getSerializedSize() + 1);
		buffer.put((byte) 0);
		serializer.serialize(value, buffer);
		buffer.flip();
		try {
			StorageChannel.write(buffer, position);
		} catch (IOException e) {
			throw new IllegalStateException("Unable to update");
		}
	}

	@Override
	public void remove(Long key) {
		if (!IfOpen) {
			throw new IllegalStateException("Container is closed.");
		}
		long position = key * (serializer.getSerializedSize() + 1);
		ByteBuffer buffer = ByteBuffer.allocate(1);
		buffer.put((byte) 1); // 1 for deleted
		buffer.flip();
		try {
			StorageChannel.write(buffer, position);
		} catch (IOException e) {
			throw new IllegalStateException("Failed");
		}
	}
}
