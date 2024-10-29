package stocks;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class Stocks implements Iterable<StockEntry> {

    private final RandomAccessFile file;
    public Stocks(String path) throws FileNotFoundException {
        this.file = new RandomAccessFile(path, "r"); //read only mode
    }

    public StockEntry get(int i) {
        try {
            file.seek(0);

            int index = 0;
            while (index < i) {

                byte[] buffer = new byte[10];
                file.read(buffer, 0, 10);
                ByteBuffer byteb = ByteBuffer.wrap(buffer);
                long id = byteb.getLong();
                short nameLength = byteb.getShort();


                file.seek(file.getFilePointer() + nameLength + 16);

                index++;
            }return readCurrentEntry();

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    //helper method
    private StockEntry readCurrentEntry() throws IOException {
        byte[] buffer = new byte[10];
        file.read(buffer, 0, 10);
        ByteBuffer byteb = ByteBuffer.wrap(buffer);
        long id = byteb.getLong();
        short nameLength = byteb.getShort();


        byte[] entryBuffer = new byte[nameLength + 8 + 8];
        file.read(entryBuffer, 0, nameLength + 16);
        ByteBuffer entryData = ByteBuffer.allocate(10 + entryBuffer.length);
        entryData.putLong(id).putShort(nameLength).put(entryBuffer);
        return new StockEntry(ByteBuffer.wrap(entryData.array()));
    }

    @Override
    public Iterator<StockEntry> iterator() {
        return new StockEntryIterator(file);
    }
}
