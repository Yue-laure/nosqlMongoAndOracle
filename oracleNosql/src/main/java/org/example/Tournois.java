package org.example;

import com.sun.jdi.Value;
import oracle.kv.*;
import oracle.nosql.driver.*;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;




// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Tournois {

    private static final String tableTournoisName = "TournoisTable";
    private String tournoisFilePath = "C:\\Users\\18117\\Desktop\\Githubbbb\\nosqlProjetMongoOracle\\nosqlMongoAndOracle\\jsonDossier\\";

    private String tournoisCsvFileName = "tournois.csv";
    private String tournoisJsonArrayFileName = "tournois.json";
    public static void main(String[] args)  throws Exception{
        // Press Alt+Entrée with your caret at the highlighted text to see how
        // IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome Oracle NoSQL!");

        /* Set up an endpoint URL */
        Region region = getRegion(args);
        System.out.println("Using region: " + region);

        /*
         * Put your credentials in $HOME/.oci/config.
         */
        AuthorizationProvider ap = new SignatureProvider();

        /* Create a NoSQL handle to access the cloud service */
        NoSQLHandleConfig config = new NoSQLHandleConfig(region, ap);
        NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config);
        try
        {
            /* Create a table and run operations. Be sure to close the handle */
            if (isDrop(args)) {
                dropTable(handle); // -drop was specified
            } else {
                Tournois(handle);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a table and do some operations.
     */
    private static void Tournois(NoSQLHandle handle) throws Exception {

        /*
         * Create a simple table with an integer key and a single string data
         * field and set your desired table capacity.
         */
        String createTableDDL = "CREATE TABLE IF NOT EXISTS " + tableTournoisName +
                "(id INTEGER, name STRING, " +
                "PRIMARY KEY(id))";

        TableLimits limits = new TableLimits(1, 2, 1);
        TableRequest tableRequest =
                new TableRequest().setStatement(createTableDDL).
                        setTableLimits(limits);
        System.out.println("Creating table " + tableTournoisName);
        /* this call will succeed or throw an exception */
        handle.doTableRequest(tableRequest,
                60000, /* wait up to 60 sec */
                1000); /* poll once per second */
        System.out.println("Table " + tableTournoisName + " is active");

        /* Make a row and write it */
        MapValue value = new MapValue().put("id", 29).put("name", "Tracy");
        PutRequest putRequest = new PutRequest().setValue(value)
                .setTableName(tableTournoisName);

        PutResult putResult = handle.put(putRequest);
        if (putResult.getVersion() != null) {
            System.out.println("Wrote " + value);
        } else {
            System.out.println("Put failed");
        }

        /* Make a key and read the row */
        MapValue key = new MapValue().put("id", 29);
        GetRequest getRequest = new GetRequest().setKey(key)
                .setTableName(tableTournoisName);

        GetResult getRes = handle.get(getRequest);
        System.out.println("Read " + getRes.getValue());

        /* At this point, you can see your table in the Identity Console */
    }

    /** Remove the table. */
    private static void dropTable(NoSQLHandle handle) throws Exception {

        /* Drop the table and wait for the table to move to dropped state */
        System.out.println("Dropping table " + tableTournoisName);
        TableRequest tableRequest = new TableRequest().setStatement
                ("DROP TABLE IF EXISTS " + tableTournoisName);
        /* this call will succeed or throw an exception */
        handle.doTableRequest(tableRequest,
                60000, /* wait up to 60 sec */
                1000); /* poll once per second */
        System.out.println("Table " + tableTournoisName + " has been dropped");
    }

    /** Get the end point from the arguments */
    private static Region getRegion(String[] args) {
        if (args.length > 0) {
            return Region.fromRegionId(args[0]);
        }
        System.err.println
                ("Usage: java -cp .:oracle-nosql-java-sdk-x.y.z/lib/* " +
                        " HelloWorld <region> [-drop]\n");
        System.exit(1);
        return null;
    }

    /** Return true if -drop is specified */
    private static boolean isDrop(String[] args) {
        if (args.length < 2) {
            return false;
        }
        return args[1].equalsIgnoreCase("-drop");
    }
}