package container.impl;

import container.Container;
import util.MetaData;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class MapContainer<Value> implements Container<Long, Value> {

	private  final Map< Long ,Value> Data;
	private final MetaData metaData;

	private long nextKeystatus ;
	Boolean ifOpen;

	public MapContainer() {
		this.Data = new HashMap<>();
		this.metaData = new MetaData();
		this.nextKeystatus = 0;
		this.ifOpen = false;

	}
	
	@Override
	public MetaData getMetaData() {
		if(ifOpen == false) {
			throw new IllegalStateException("Container is closed");
		}
		else
			return metaData;
	}
	
	@Override
	public void open() {
		ifOpen = true;

	}

	@Override
	public void close() {
		ifOpen = false;
	}
	
	@Override
	public Long reserve() throws IllegalStateException {

		if (ifOpen==false) {
			throw new IllegalStateException("The container is not open");
		}
		return nextKeystatus++;
	}
	

	@Override
	public Value get(Long key) throws NoSuchElementException {
		if(ifOpen == false) {
			throw new IllegalStateException("The container is not open");
		}
		if(!Data.containsKey(key)) {
			throw new NoSuchElementException("No such elements");
		}

		return Data.get(key);
	}

	@Override
	public void update(Long key, Value value) throws NoSuchElementException {
		if(ifOpen == false) {
			throw new IllegalStateException("The container is not open");
		}
		if(!Data.containsKey(key) && key>= nextKeystatus) {
			throw new NoSuchElementException("No such elements to update");
		}
		Data.put(key, value);
	}

	@Override
	public void remove(Long key) throws NoSuchElementException {
		if(ifOpen == false) {
			throw new IllegalStateException("The container is not open");

		}
		if(!Data.containsKey(key)) {
			throw new NoSuchElementException("No such key there to remove");
		}
		Data.remove(key);
	}
}
