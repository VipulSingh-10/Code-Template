package stocks;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class StockEntryIterator implements Iterator<StockEntry> {

    private long pos;
    private final RandomAccessFile file;

    public StockEntryIterator(RandomAccessFile file) {
        this.file = file;
        this.pos = 0; //beg of the file to position 0
    }

    @Override
    public boolean hasNext() {
        try {
            return pos < file.length();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    //Without using repetive gets
    public StockEntry next() {
        try {
            file.seek(pos);
            byte[] buffer = new byte[10]; //reading ID and the namelength here
            file.read(buffer, 0, 10);
            ByteBuffer byteb = ByteBuffer.wrap(buffer);
            long id = byteb.getLong();
            short nameLength = byteb.getShort();
            byte[] entryBuffer = new byte[nameLength + 8 + 8];
            file.read(entryBuffer, 0, nameLength + 16);
            ByteBuffer entryData = ByteBuffer.allocate(10 + entryBuffer.length);
            entryData.putLong(id).putShort(nameLength).put(entryBuffer);
            //wrapping the data
            StockEntry entry = new StockEntry(ByteBuffer.wrap(entryData.array()));

            pos = file.getFilePointer();//moving to next position

            return entry;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
