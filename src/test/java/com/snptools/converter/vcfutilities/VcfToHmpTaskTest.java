package com.snptools.converter.vcfutilities;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A set of unit tests for VcfToHmpTask.
 */
public class VcfToHmpTaskTest {

    final String TEST_INPUT_VCF = "./src/test/resources/test.vcf";
    final String TEST_OUTPUT_VCF = "./src/test/resources/testVcf.csv";
    final int START_LINE = 12; // double check this
    final int END_LINE = 17; // double check this
    final int TOTAL_LINES = END_LINE - START_LINE;
    final int COLUMN_PAIRS = 281; // is equal to half the number of data columns.

    final int START_COLUMN = 9; // Where this worker should start - GT column present. // double check this
    final int END_COLUMN = 290; // Where this worker should end.
    final int NUMBER_OF_COLUMNS = END_COLUMN - START_COLUMN; // The number of columns that should be kept by this worker.
    final int COLUMN_WIDTH = 3; // <--- Depends on ploidiness. Diploid=3.

    final String[] alleles = new String[10];
    final String[] outputLineHeaders = new String[10];

    /**
     * Tests whether the VcfToHmpTask worker was created successfully.
     * The VcfToHmpTask should be used on normalized data.
     */
    @Test
    @DisplayName("shouldConstructVcfToHmpTask")
    public void shouldConstructVcfToHmpTask() {
        VcfToHmpTask vcfToHmpTask = new VcfToHmpTask(TEST_INPUT_VCF, TEST_OUTPUT_VCF, START_LINE, END_LINE, NUMBER_OF_COLUMNS, alleles, outputLineHeaders);
        assertNotNull(vcfToHmpTask);
    }
}
