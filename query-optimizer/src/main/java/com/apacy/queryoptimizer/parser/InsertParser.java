package com.apacy.queryoptimizer.parser;

import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;

/**
 * Parser for INSERT query
 */
public class InsertParser extends AbstractParser {

    public InsertParser(List<Token> tokens) {
        super(tokens);
    }

    /**
     * Mem-parsing grammar: INSERT INTO Identifier ( ColumnList ) VALUES ( ValueList );
     */
    @Override
    public ParsedQuery parse() {
        consume(TokenType.INSERT);
        consume(TokenType.INTO);

        // Ambil nama tabel
        Token tableToken = consume(TokenType.IDENTIFIER);
        String tableName = tableToken.getValue();

        // Parsing ( ColumnList )
        consume(TokenType.LPARENTHESIS);
        List<String> columns = parseColumnList();
        consume(TokenType.RPARENTHESIS);

        // Parsing VALUES ( ValueList )
        consume(TokenType.VALUES);
        consume(TokenType.LPARENTHESIS);
        List<Object> values = parseValueList(); 
        consume(TokenType.RPARENTHESIS);

        // (Opsional) Cek semicolon di akhir
        if (match(TokenType.SEMICOLON)) {
            // Semicolon ada, bagus
        }

        // Validasi
        if (columns.size() != values.size()) {
            throw new RuntimeException("Jumlah kolom tidak cocok dengan jumlah nilai untuk INSERT.");
        }

        // Buat objek ParsedQuery sesuai DTO yang baru
        //
        return new ParsedQuery(
            "INSERT",            // queryType
            List.of(tableName),  // targetTables
            columns,             // targetColumns
            values,              // values 
            null,                // joinConditions
            null,                // whereClause
            null,                // orderByColumn
            false,               // isDescending
            false                // isOptimized
        );
    }

    /**
     * Implementasi untuk method 'validate()' yang baru ditambahkan di AbstractParser
     */
    @Override
    public boolean validate() {
        int originalPosition = this.position; // Simpan posisi awal
        try {
            parse();
            this.position = originalPosition; // Kembalikan posisi jika berhasil
            return true;
        } catch (Exception e) {
            this.position = originalPosition; // Kembalikan posisi jika gagal
            return false;
        }
    }

    /**
     * Helper untuk mem-parsing: col1, col2, col3
     */
    private List<String> parseColumnList() {
        List<String> columns = new ArrayList<>();
        
        // Ambil kolom pertama
        columns.add(consume(TokenType.IDENTIFIER).getValue());

        // Cek jika ada kolom berikutnya (dipisah koma)
        while (match(TokenType.COMMA)) {
            columns.add(consume(TokenType.IDENTIFIER).getValue());
        }

        return columns;
    }

    /**
     * Helper untuk mem-parsing: 'val1', 123, 'val3'
     * Ini akan menerima NUMBER_LITERAL atau STRING_LITERAL
     */
    private List<Object> parseValueList() {
        List<Object> values = new ArrayList<>();

        // Ambil nilai pertama
        values.add(parseLiteral());

        // Cek jika ada nilai berikutnya (dipisah koma)
        while (match(TokenType.COMMA)) {
            values.add(parseLiteral());
        }

        return values;
    }
    
    /**
     * Helper untuk mengambil nilai literal (String atau Angka)
     */
    private Object parseLiteral() {
        if (peek().getType() == TokenType.STRING_LITERAL) {
            return consume(TokenType.STRING_LITERAL).getValue();
        } else if (peek().getType() == TokenType.NUMBER_LITERAL) {
            return consume(TokenType.NUMBER_LITERAL).getValue();
        }
        
        throw new RuntimeException("Diharapkan STRING_LITERAL atau NUMBER_LITERAL, tapi ditemukan: " + peek().getType());
    }
}