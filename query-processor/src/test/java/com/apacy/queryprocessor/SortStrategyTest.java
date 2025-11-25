package com.apacy.queryprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.Row;
import com.apacy.queryprocessor.execution.SortStrategy;
import com.apacy.queryprocessor.mocks.MockStorageManager;

class SortStrategyTest {

    private MockStorageManager mockSM;
    private List<Row> rows;
    private Row r1, r2, r3, r4, r5;

    @BeforeEach
    void setUp() {
        mockSM = new MockStorageManager();
        rows = mockSM.readBlock(null); // mock ignores the parameter
        r1 = rows.get(0);
        r2 = rows.get(1);
        r3 = rows.get(2);
        r4 = rows.get(3);
        r5 = rows.get(4);
    }

    @Test
    void testSortByNameAscending() {
        List<Row> sorted = SortStrategy.sort(rows, "name", true);
        printRows(sorted, "Sorted by name ASC");
        assertEquals(5, sorted.size());
        // Expected order: Bayu, Farrel, Kinan, Naufarrel, Weka
        assertSame(r5, sorted.get(0));
        assertSame(r4, sorted.get(1));
        assertSame(r3, sorted.get(2));
        assertSame(r1, sorted.get(3));
        assertSame(r2, sorted.get(4));
    }

    @Test
    void testSortBySalaryDescending() {
        List<Row> sorted = SortStrategy.sort(rows, "salary", false); // descending
        printRows(sorted, "Sorted by salary DESC");
        assertEquals(5, sorted.size());
        // Expected order: 60000, 50000, 40000, 30000, 20000
        assertSame(r5, sorted.get(0));
        assertSame(r4, sorted.get(1));
        assertSame(r3, sorted.get(2));
        assertSame(r2, sorted.get(3));
        assertSame(r1, sorted.get(4));
    }

    @Test
    void testSortNullInputDoesNotThrow() {
        assertDoesNotThrow(() -> SortStrategy.sort(null, "id", true));
    }

    @Test
    void testSortSingleElementPreserved() {
        List<Row> single = List.of(r1);
        List<Row> result = SortStrategy.sort(single, "id", true);
        printRows(result, "Single element result");
        assertEquals(1, result.size());
        assertSame(r1, result.get(0));
    }

    @Test
    void testSortMultipleColumns_nameAsc_salaryDesc() {
        Row asep = new Row(Map.of("id", 10, "name", "Asep", "salary", 1000));
        Row budiLow = new Row(Map.of("id", 11, "name", "Budi", "salary", 2500));
        Row budiHigh = new Row(Map.of("id", 12, "name", "Budi", "salary", 4000));
        Row cepot = new Row(Map.of("id", 13, "name", "Cepot", "salary", 500));

        List<Row> list = new ArrayList<>();
        list.add(budiLow);
        list.add(cepot);
        list.add(budiHigh);
        list.add(asep);

        String[] cols = new String[] { "name", "salary" };
        boolean[] asc = new boolean[] { true, false };

        List<Row> sorted = SortStrategy.sortMultiple(list, cols, asc);
        printRows(sorted, "Multi-column sort (name ASC, salary DESC)");

        // Expected order: Asep, Budi (4000), Budi (2500), Cepot
        assertSame(asep, sorted.get(0));
        assertSame(budiHigh, sorted.get(1));
        assertSame(budiLow, sorted.get(2));
        assertSame(cepot, sorted.get(3));
    }

    // Helper buat print rows
    private void printRows(List<Row> list, String label) {
        System.out.println("---- " + label + " ----");
        if (list == null) {
            System.out.println("null");
            return;
        }
        for (int i = 0; i < list.size(); i++) {
            Row r = list.get(i);
            if (r == null) {
                System.out.printf("%d: null%n", i);
                continue;
            }
            Object id = r.get("id");
            Object name = r.get("name");
            Object salary = r.get("salary");
            System.out.printf("%d: id=%s, name=%s, salary=%s%n", i, id, name, salary);
        }
        System.out.println("-------------------------");
    }
}