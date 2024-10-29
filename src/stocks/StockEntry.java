package stocks;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StockEntry {
    private final long id;
    private final String name;
    private final long ts;
    private final double value;


    public StockEntry(long id, String name, long timestamp, double market_value) {
        this.id = id;
        this.name = name;
        this.ts = timestamp;
        this.value = market_value;
    }

    //deserialization here
    public StockEntry(ByteBuffer byteb) {

        this.id = byteb.getLong();


        short nameLength = byteb.getShort();


        byte[] nameBytes = new byte[nameLength];
        byteb.get(nameBytes);
        this.name = new String(nameBytes, StandardCharsets.UTF_8);


        this.ts = byteb.getLong();


        this.value = byteb.getDouble();
    }

    //serialization here
    public ByteBuffer getBytes() {
        byte[] nameBytes = this.name.getBytes(StandardCharsets.UTF_8);
        int nameLength = nameBytes.length;


        ByteBuffer byteb = ByteBuffer.allocate(8 + 2 + nameLength + 8 + 8);
        byteb.putLong(this.id);
        byteb.putShort((short) nameLength);


        byteb.put(nameBytes);


        byteb.putLong(this.ts);
        byteb.putDouble(this.value);

        byteb.flip();

        return byteb;
    }

    public long getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public long getTimeStamp() {
        return this.ts;
    }

    public double getMarketValue() {
        return this.value;
    }


    public int getSerializedLength() {
        return 3 * 8 + 2 + name.getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public String toString() {
        return id + " " + name + " " + ts + " " + value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StockEntry) {
            StockEntry entry = (StockEntry) obj;
            return id == entry.id && name.equals(entry.name) && ts == entry.ts && value == entry.value;
        }
        return false;
    }
}
