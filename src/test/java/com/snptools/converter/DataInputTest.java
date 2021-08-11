package com.snptools.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
//import org.junit.jupiter.params.ParameterizedTest;
//import org.junit.jupiter.params.provider.CsvSource;

/**
 * Unit test for DataInput.
 */
public class DataInputTest {

    /**
     * JUnit5 setup check.
     */
    @Test
    @DisplayName("shouldAnswerWithTrue")
    public void shouldAnswerWithTrue() {
        assertTrue(true);
    }

    @Test
    void shouldCheckAllInputArguments() {
        List<String> numbers = List.of("0", "./InputFolder", "./OutputFolder");
        Assertions.assertAll(
            () -> assertEquals("0", numbers.get(0)),
            () -> assertEquals("./InputFolder", numbers.get(1)),
            () -> assertEquals("./OutputFolder", numbers.get(2)),
            () -> assertEquals(3, numbers.size())
        );
    }

}
