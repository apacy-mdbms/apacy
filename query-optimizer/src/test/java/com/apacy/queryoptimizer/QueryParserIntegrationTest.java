package com.apacy.queryoptimizer;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.ParsedQuery;

/**
 * Integration test: parse a SELECT and run it on H2.
 */
public class QueryParserIntegrationTest {

    static Connection conn;

    @BeforeAll
    static void setupDb() throws Exception {
        conn = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT, email VARCHAR(200))");
            s.execute("INSERT INTO users (id, name, age, email) VALUES (1,'Alice',30,'a@example.com')");
            s.execute("INSERT INTO users (id, name, age, email) VALUES (2,'Bob',25,'b@example.com')");
            s.execute("INSERT INTO users (id, name, age, email) VALUES (3,'Carol',35,'c@example.com')");
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    @Test
    void parseAndRunSimpleSelect() throws Exception {
        String sql = "SELECT name, email FROM users WHERE age > 26 ORDER BY name LIMIT 10;";

        QueryParser parser = new QueryParser();
        ParsedQuery pq = parser.parse(sql);

        // basic checks on parsed query
        assertEquals("SELECT", pq.queryType());
        assertEquals(java.util.List.of("users"), pq.targetTables());
        assertEquals(java.util.List.of("name", "email"), pq.targetColumns());
        assertNotNull(pq.whereClause(), "whereClause should be parsed");
        assertEquals("name", pq.orderByColumn());

        // Execute the original SQL against H2
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(sql);

            // count rows and check first row
            int count = 0;
            String firstName = null;
            String firstEmail = null;
            while (rs.next()) {
                if (count == 0) {
                    firstName = rs.getString(1);
                    firstEmail = rs.getString(2);
                }
                count++;
            }

            // ages > 26 -> Alice (30) and Carol (35) -> 2 rows; ORDER BY name => Alice, Carol
            assertEquals(2, count, "Expected 2 rows where age > 26");
            assertEquals("Alice", firstName);
            assertEquals("a@example.com", firstEmail);
        }
    }

}
