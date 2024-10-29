package container.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import util.MetaData;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class MapContainerTest {

    private MapContainer<Integer> container;

    @BeforeEach
    void setUp() {
        container = new MapContainer<>();
    }

    @Test
    void testOpenClose() {
        assertFalse(container.ifOpen, "Container should initially be closed.");

        container.open();
        assertTrue(container.ifOpen, "Container should be open after calling open().");

        container.close();
        assertFalse(container.ifOpen, "Container should be closed after calling close().");
    }

    @Test
    void testReserveKeyIncrements() {
        container.open();
        Long firstKey = container.reserve();
        Long secondKey = container.reserve();

        assertEquals(0L, firstKey, "First reserved key should be 0.");
        assertEquals(1L, secondKey, "Second reserved key should be 1.");
    }

    @Test
    void testGetMetaData() {
        container.open();
        MetaData metaData = container.getMetaData();

        assertNotNull(metaData, "MetaData should not be null.");
    }

    @Test
    void testReserveWithoutOpening() {
        assertThrows(IllegalStateException.class, container::reserve, "Should throw IllegalStateException if container is closed.");
    }

    @Test
    void testUpdateAndRetrieveValue() {
        container.open();
        Long key = container.reserve();

        container.update(key, 100);
        Integer retrievedValue = container.get(key);

        assertEquals(100, retrievedValue, "Retrieved value should match the updated value.");
    }

    @Test
    void testGetNonExistentKey() {
        container.open();
        assertThrows(NoSuchElementException.class, () -> container.get(999L), "Should throw NoSuchElementException for non-existent key.");
    }

    @Test
    void testUpdateNonReservedKey() {
        container.open();
        assertThrows(NoSuchElementException.class, () -> container.update(999L, 100), "Should throw NoSuchElementException for non-reserved key.");
    }

    @Test
    void testRemoveExistingKey() {
        container.open();
        Long key = container.reserve();
        container.update(key, 200);

        container.remove(key);

        assertThrows(NoSuchElementException.class, () -> container.get(key), "Should throw NoSuchElementException after removing a key.");
    }

    @Test
    void testRemoveNonExistentKey() {
        container.open();
        assertThrows(NoSuchElementException.class, () -> container.remove(999L), "Should throw NoSuchElementException for non-existent key.");
    }

    @Test
    void testGetMetaDataWhenClosed() {
        assertThrows(IllegalStateException.class, container::getMetaData, "Should throw IllegalStateException if container is closed.");
    }

    @Test
    void testUpdateWhenClosed() {
        Long key = 1L;
        assertThrows(IllegalStateException.class, () -> container.update(key, 100), "Should throw IllegalStateException if container is closed.");
    }

    @Test
    void testGetWhenClosed() {
        Long key = 1L;
        assertThrows(IllegalStateException.class, () -> container.get(key), "Should throw IllegalStateException if container is closed.");
    }

    @Test
    void testRemoveWhenClosed() {
        Long key = 1L;
        assertThrows(IllegalStateException.class, () -> container.remove(key), "Should throw IllegalStateException if container is closed.");
    }
}
