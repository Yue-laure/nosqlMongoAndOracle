package org.example;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Juges {
    private MongoDatabase database;
    private String dbName = "tournoisDB";
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ubase";
    private String passWord = "UPass";
    private String jugesCollectionName = "colJuge";

//    private String colCollectionName = "colInscrits";

    private String jugesFilePath = "C:\\Users\\18117\\Desktop\\Githubbbb\\nosqlProjetMongoOracle\\nosqlMongoAndOracle\\jsonDossier\\";

    private String jugesCsvFileName = "juges.csv";
    private String jugesJsonArrayFileName = "juges.json";
    private String destJugesCsvFileName = "juges_out.csv";

    public static void main(String[] args) {
        // Press Alt+Entrée with your caret at the highlighted text to see how
        // IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome to Mongotest!");
        try{
            Juges juges=new Juges();
            juges.dropCollectionJuges(juges.jugesCollectionName);
            juges.createCollectionJuges(juges.jugesCollectionName);
            juges.deleteJuges(juges.jugesCollectionName,new Document());

            juges.testInsertOneJuge();
            juges.testInsertManyJuges();

            juges.loadJugesFromJsonArrayFile(
                juges.jugesCollectionName,
                juges.jugesFilePath,
                juges.jugesJsonArrayFileName
            );
//afficher tous les Juges sans tri ni projection
            juges.getJuges(
                juges.jugesCollectionName,
                new Document(),
                new Document(),
                new Document()
            );
            juges.updateJuges(
                juges.jugesCollectionName,
                new Document("id_juge",5),
                new Document ("$set",new Document("id_t", 8)),
                new UpdateOptions()
            );
            juges.deleteJuges(juges.jugesCollectionName, new Document("id_juge", 20));
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
    /**
     * FC1 : Constructeur Juges.
     * Dans ce constructeur sont effectuées les activités suivantes:
     * - Création d'une instance du client MongoClient
     * - Création d'une BD Mongo appelé RH
     * - Création d'un utilisateur appelé
     * - Chargement du pointeur vers la base RH
     */
    Juges() {
        // Creating a Mongo juges
        MongoClient mongoClient = new MongoClient(hostName, port);
        // Creating Credentials
        // RH : Ressources Humaines
        MongoCredential credential;
        credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray());
        System.out.println("Connected to the database successfully");
        System.out.println("Credentials ::" + credential);
        // Accessing the database
        database = mongoClient.getDatabase(dbName);
    }
    /**
     * FC2 : Cette fonction permet de créer une collection
     * de nom nomCollection.
     */
    public void createCollectionJuges(String nomCollection) {
        //Creating a collection
        database.createCollection(nomCollection);
        System.out.println("Collection Juges created successfully");
    }
    /**
     * FC3 : Cette fonction permet de supprimer une collection
     * connaissant son nom.
     */
    public void dropCollectionJuges(String nomCollection) {
        //Drop a collection
        MongoCollection<Document> colJuges = null;
        System.out.println("\n\n\n*********** dans dropCollectionJuges *****************");
        System.out.println("!!!! Collection Juges : " + colJuges);
        colJuges = database.getCollection(nomCollection);
        System.out.println("!!!! Collection Juges : " + colJuges);
        colJuges.drop();
        System.out.println("Collection colJuges removed successfully !!!");
    }
    /**
     * FC4 : Cette fonction permet d'insérer un juges dans une collection.
     */
    public void insertOneJuge(String nomCollection, Document juges) {
        //Drop a collection
        MongoCollection<Document> colJuges = database.getCollection(nomCollection);
        colJuges.insertOne(juges);
        System.out.println("Document inserted successfully");
    }
    /**
     * FC5 : Cette fonction permet de tester la méthode Juges.
     */
    public void testInsertOneJuge() {
        Document juges = new Document("id_juge", 1999).append("id_t", 10);
        this.insertOneJuge(this.jugesCollectionName, juges);
        System.out.println("Document inserted successfully");
    }
    /**
     * FC6 : Cette fonction permet d'insérer plusieurs juges dans une collection
     */
    public void insertManyJuges(String nomCollection, List<Document> juges) {
        //Drop a collection
        MongoCollection<Document> colJuges = database.getCollection(nomCollection);
        colJuges.insertMany(juges);
        System.out.println("Many Documents inserted successfully");
    }
    /**
     * FC7 : Cette fonction permet de tester la fonction insertManyJuges
     */
    public void testInsertManyJuges() {
        List<Document> juges = Arrays.asList(
                new Document("id_juge", 2000).append("id_t", 10),
                new Document("id_juge", 2001).append("id_t", 10),
                new Document("id_juge", 2002).append("id_t", 10)
                );
        this.insertManyJuges(this.jugesCollectionName, juges);
    }
    /**
     * FC8 : Cette fonction permet de rechercher un tournoi dans une collection
     * connaissant son id.
     */
    public void getJugesById(String nomCollection, Integer JugesId) {
        //Drop a collection
        System.out.println("\n\n\n*********** dans getJugesById *****************");
        MongoCollection<Document> colJuges = database.getCollection(nomCollection);

        //BasicDBObject whereQuery = new BasicDBObject();
        Document whereQuery = new Document();
        whereQuery.put("id_juge", JugesId);
        //DBCursor cursor = colJuges.find(whereQuery);
        FindIterable<Document> listJuges = colJuges.find(whereQuery);

        // Getting the iterator
        Iterator it = listJuges.iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }
    /**
     * FC9 : Cette fonction permet de rechercher des juges dans une collection.
     * Le paramètre whereQuery : permet de passer des conditions de rechercher
     * Le paramètre projectionFields : permet d'indiquer les champs à afficher
     * Le paramètre sortFields : permet d'indiquer les champs de tri.
     */
    public void getJuges(String nomCollection, Document whereQuery, Document projectionFields, Document sortFields) {
        //Drop a collection
        System.out.println("\n\n\n*********** dans getJuges *****************");
        MongoCollection<Document> colJuges = null;
        System.out.println("Dans getJuges 2.1 *****************" + colJuges);
        System.out.println("Dans getJuges 2.2 : database:" + database);
        colJuges = database.getCollection(nomCollection);
        System.out.println("Dans getJuges 2.1 *****************" + colJuges);

        FindIterable<Document> listJuges = colJuges.find(whereQuery).sort(sortFields).projection(projectionFields);
        //System.out.println("colJuges.count():"+colJuges.count());
        // Getting the iterator
        Iterator it = listJuges.iterator();
        System.out.println("Dans getJuges 2.1 *****************");
        while (it.hasNext()) {
            System.out.println("Dans getJuges 2.3 *****************");
            System.out.println(it.next());
        }
    }
    /**
     * FC10 : Cette fonction permet de modifier des Juges dans une collection.
     * Le paramètre whereQuery : permet de passer des conditions de recherche
     * Le paramètre updateExpressions : permet d'indiquer les champs à modifier
     * Le paramètre UpdateOptions : permet d'indiquer les options de mise à jour :
     * .upSert : insère si le document n'existe pas
     */
    public void updateJuges(String nomCollection, Document whereQuery, Document updateExpressions, UpdateOptions updateOptions) {
        //Drop a collection
        System.out.println("\n\n\n*********** dans updateJuges *****************");
        MongoCollection<Document> colJuges = database.getCollection(nomCollection);
        UpdateResult updateResult = colJuges.updateMany(whereQuery, updateExpressions);
        System.out.println("\nRésultat update : " + "getUpdate id: " + updateResult + " getMatchedCount : " + updateResult.getMatchedCount() + " getModifiedCount : " + updateResult.getModifiedCount());
    }
    /**
     * FC11 : Cette fonction permet de supprimer des juges dans une collection.
     * Le paramètre filters : permet de passer des conditions de recherche des employés à supprimer
     */
    public void deleteJuges(String nomCollection, Document filters) {
        System.out.println("\n\n\n*********** dans deleteJuges *****************");
        FindIterable<Document> listTournoi;
        Iterator it;
        MongoCollection<Document> colJuges = database.getCollection(nomCollection);

        listTournoi = colJuges.find(filters).sort(new Document("_id", 1));
        it = listTournoi.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteJuges: avant suppression");

        colJuges.deleteMany(filters);
        listTournoi = colJuges.find(filters).sort(new Document("_id", 1));
        it = listTournoi.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteJuges: Apres suppression");
    }
    /**
     * FC12 : Parcours un itérateur et affiche les documents qui s'y trouvent
     */
    public void displayIterator(Iterator it, String message) {
        System.out.println(" \n #### " + message + " ################################");
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }

    public void loadJugesFromJsonArrayFile(String collectionName, String filePath, String fileName) {
        System.out.println(" \n ####  DANS loadJugesFromJsonArrayFile ################################");
        System.out.println(filePath + fileName);
        JSONParser jsonParser = new JSONParser();
        MongoCollection<Document> colJuges = database.getCollection(collectionName);
        try {
            JSONArray jsonJugesArray = (JSONArray) jsonParser.parse(new FileReader(filePath + fileName));
            List<Document> jugesList = new ArrayList<Document>();

            for (Object jugeObj : jsonJugesArray) {
                JSONObject juge = (JSONObject) jugeObj;
                Document docJuges = Document.parse(juge.toString());
                jugesList.add(docJuges);
            }
            colJuges.insertMany(jugesList);

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}