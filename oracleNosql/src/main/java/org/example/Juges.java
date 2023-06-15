package org.example;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.nosql.driver.*;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.values.MapValue;
import org.json.simple.parser.JSONParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Juges {
    private String storeName = "tournoisDB";
    private String hostName = "localhost";
    private int port = 5000;
    private static final String tableTournoisName = "TournoisTable";
    private String tournoisFilePath = "C:\\Users\\18117\\Desktop\\Githubbbb\\nosqlProjetMongoOracle\\nosqlMongoAndOracle\\jsonDossier\\";

    private String tournoisCsvFileName = "juges.csv";
    private String jugesJsonArrayFileName = "juges.json";
    public static void main(String[] args)  throws Exception{
        // Press Alt+Entrée with your caret at the highlighted text to see how
        // IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome Oracle NoSQL!");
        Juges jures= new Juges();
//        configuration conect de KVstore
        KVStoreConfig kconfig = new KVStoreConfig(jures.storeName, jures.hostName + ":" + jures.port);
        // creer KVStore connect
        KVStore store = KVStoreFactory.getStore(kconfig);

        Table table = store.getTableAPI().getTable(jures.tableTournoisName);
//        upload json dans store
        try {
            loadJsonFileToKVStore(store,table,jures.tournoisFilePath+jures.jugesJsonArrayFileName, jures.tableTournoisName);
            System.out.println("JSON data loaded successfully to Oracle NoSQL KVStore.");
        } catch (IOException | FaultException e) {
            e.printStackTrace();
        } finally {
            store.close();
        }
    }

    private static void loadJsonFileToKVStore(KVStore store,Table table, String filePath, String tableName) throws IOException {
//        JSONParser jsonParser = new JSONParser();
        // Read JSON file content
        String jsonData = readJsonFile(filePath);

        // Create a primary key
        String primaryKey = "id_juge";

        // Create a row
        Row row = table.createRow();
        row.put("id_juge", primaryKey);
        row.put("id_t", jsonData);

        // Put the row into the table
        store.getTableAPI().put(row, null, null);
    }

    // Read JSON file content
    private static String readJsonFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        return new String(Files.readAllBytes(path));
    }



}