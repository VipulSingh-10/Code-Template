package container.impl;

import io.IntSerializer;
import org.junit.jupiter.api.*;
import util.MetaData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class SimpleFileContainerTest {

    private static Path tempDir;
    private SimpleFileContainer<Integer> container;
    private Path dataFilePath;
    private Path metadataFilePath;

    @BeforeAll
    static void setUpClass() throws IOException {
        tempDir = Files.createTempDirectory("SimpleFileContainerTest");
    }

    @AfterAll
    static void tearDownClass() throws IOException {
        Files.walk(tempDir).map(Path::toFile).forEach(file -> file.deleteOnExit());
    }

    @BeforeEach
    void setUp() throws IOException {
        container = new SimpleFileContainer<>(tempDir, "test", new IntSerializer());
        dataFilePath = tempDir.resolve("test_data.bin");
        metadataFilePath = tempDir.resolve("test_metadata.bin");
    }

    @AfterEach
    void tearDown() {
        container.close();
    }

    @Test
    void testUpdateAndFileContent() throws IOException {
        container.open();
        Long key = container.reserve();
        container.update(key, 42);

        // Validate data in the data file
        try (FileChannel dataChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(5); // 1 byte marker + 4 bytes for int (value)
            dataChannel.read(buffer, key * 5);
            buffer.flip();

            assertEquals(0, buffer.get(), "First byte should indicate entry is not deleted.");
            assertEquals(42, buffer.getInt(), "The stored integer should be 42.");
        }
    }

    @Test
    void testMetadataFileAfterReserve() throws IOException {
        container.open();
        container.reserve();

        // Validate data in the metadata file
        try (FileChannel metadataChannel = FileChannel.open(metadataFilePath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            metadataChannel.read(buffer, 0);
            buffer.flip();

            assertEquals(1, buffer.getLong(), "Next key should be incremented to 1.");
        }
    }


    @Test
    void testReopenAndRetrieveData() throws IOException {
        container.open();
        Long key = container.reserve();
        container.update(key, 99);
        container.close();

        // Reopen container and retrieve data to ensure persistence
        container.open();
        assertEquals(99, container.get(key), "Value should persist and be 99 after reopening.");
    }

    @Test
    void testRemoveAndFileContent() throws IOException {
        container.open();
        Long key = container.reserve();
        container.update(key, 25);
        container.remove(key);

        // Verify that the entry is marked as deleted in the data file
        try (FileChannel dataChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(1); // Only need the deletion marker
            dataChannel.read(buffer, key * 5); // Each entry is 5 bytes (1 byte marker + 4 bytes value)
            buffer.flip();

            assertEquals(1, buffer.get(), "First byte should indicate entry is marked as deleted.");
        }
    }
}
