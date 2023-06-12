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
public class Inscrits {
    private MongoDatabase database;
    private String dbName = "tournoisDB";
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ubase";
    private String passWord = "UPass";
    private String inscritsCollectionName = "colInscrits";

//    private String colCollectionName = "colInscrits";

    private String inscritsFilePath = "C:\\Users\\18117\\Desktop\\coursMIAGE\\SGBD\\mongoProjetTournois\\jsonDossier\\";

    private String inscritsCsvFileName = "inscrits.csv";
    private String inscritsJsonArrayFileName = "inscrits.json";
    private String destInscritsCsvFileName = "inscrits_out.csv";

    public static void main(String[] args) {
        // Press Alt+Entrée with your caret at the highlighted text to see how
        // IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome to Mongotest!");
        try{
            Inscrits inscrits=new Inscrits();
            inscrits.dropCollectionInscrits(inscrits.inscritsCollectionName);
            inscrits.createCollectionInscrits(inscrits.inscritsCollectionName);
            inscrits.deleteInscrits(inscrits.inscritsCollectionName,new Document());

            inscrits.testInsertOneInscrits();
            inscrits.testInsertManyInscrits();

            inscrits.loadInscritsFromJsonArrayFile(
                inscrits.inscritsCollectionName,
                inscrits.inscritsFilePath,
                inscrits.inscritsJsonArrayFileName
            );
//afficher tous les toirnois sans tri ni projection
            inscrits.getInscrits(
                inscrits.inscritsCollectionName,
                new Document(),
                new Document(),
                new Document()
            );
            inscrits.updateInscrits(
                inscrits.inscritsCollectionName,
                new Document("id_inscrit",3),
                new Document ("$set",new Document("nom_i", "Loic").append("date_i", "14/07/1995")),
                new UpdateOptions()
            );
            inscrits.deleteInscrits(inscrits.inscritsCollectionName, new Document("id_inscrits", 20));
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
    /**
     * FC1 : Constructeur Inscrits.
     * Dans ce constructeur sont effectuées les activités suivantes:
     * - Création d'une instance du client MongoClient
     * - Création d'une BD Mongo appelé RH
     * - Création d'un utilisateur appelé
     * - Chargement du pointeur vers la base RH
     */
    Inscrits() {
        // Creating a Mongo inscrits
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
    public void createCollectionInscrits(String nomCollection) {
        //Creating a collection
        database.createCollection(nomCollection);
        System.out.println("Collection Inscrits created successfully");
    }
    /**
     * FC3 : Cette fonction permet de supprimer une collection
     * connaissant son nom.
     */
    public void dropCollectionInscrits(String nomCollection) {
        //Drop a collection
        MongoCollection<Document> colInscrits = null;
        System.out.println("\n\n\n*********** dans dropCollectionInscrits *****************");
        System.out.println("!!!! Collection Inscrits : " + colInscrits);
        colInscrits = database.getCollection(nomCollection);
        System.out.println("!!!! Collection Inscrits : " + colInscrits);
        colInscrits.drop();
        System.out.println("Collection colInscrits removed successfully !!!");
    }
    /**
     * FC4 : Cette fonction permet d'insérer un inscrits dans une collection.
     */
    public void insertOneInscrit(String nomCollection, Document inscrits) {
        //Drop a collection
        MongoCollection<Document> colInscrits = database.getCollection(nomCollection);
        colInscrits.insertOne(inscrits);
        System.out.println("Document inserted successfully");
    }
    /**
     * FC5 : Cette fonction permet de tester la méthode Inscrits.
     */
    public void testInsertOneInscrits() {
        Document inscrits = new Document("id_j", "5").append("id_p", "1005").append("note", 8);
        this.insertOneInscrit(this.inscritsCollectionName, inscrits);
        System.out.println("Document inserted successfully");
    }
    /**
     * FC6 : Cette fonction permet d'insérer plusieurs inscrits dans une collection
     */
    public void insertManyInscrits(String nomCollection, List<Document> inscrits) {
        //Drop a collection
        MongoCollection<Document> colInscrits = database.getCollection(nomCollection);
        colInscrits.insertMany(inscrits);
        System.out.println("Many Documents inserted successfully");
    }
    /**
     * FC7 : Cette fonction permet de tester la fonction insertManyInscrits
     */
    public void testInsertManyInscrits() {
        List<Document> inscrits = Arrays.asList(
                new Document("id_j", "5").append("id_p", "1007").append("note", 8),
                new Document("id_j", "5").append("id_p", "1008").append("note", 8.6),
                new Document("id_j", "5").append("id_p", "1009").append("note", 9.8)
                );
        this.insertManyInscrits(this.inscritsCollectionName, inscrits);
    }
    /**
     * FC8 : Cette fonction permet de rechercher un Inscrit dans une collection
     * connaissant son id.
     */
    public void getInscritsById(String nomCollection, Integer InscritsId) {
        //Drop a collection
        System.out.println("\n\n\n*********** dans getInscritsById *****************");
        MongoCollection<Document> colInscrits = database.getCollection(nomCollection);

        //BasicDBObject whereQuery = new BasicDBObject();
        Document whereQuery = new Document();
        whereQuery.put("id_inscrits", InscritsId);
        //DBCursor cursor = colInscrits.find(whereQuery);
        FindIterable<Document> listInscrits = colInscrits.find(whereQuery);

        // Getting the iterator
        Iterator it = listInscrits.iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }
    /**
     * FC9 : Cette fonction permet de rechercher des inscrits dans une collection.
     * Le paramètre whereQuery : permet de passer des conditions de rechercher
     * Le paramètre projectionFields : permet d'indiquer les champs à afficher
     * Le paramètre sortFields : permet d'indiquer les champs de tri.
     */
    public void getInscrits(String nomCollection, Document whereQuery, Document projectionFields, Document sortFields) {
        //Drop a collection
        System.out.println("\n\n\n*********** dans getInscrits *****************");
        MongoCollection<Document> colInscrits = null;
        System.out.println("Dans getInscrits 2.1 *****************" + colInscrits);
        System.out.println("Dans getInscrits 2.2 : database:" + database);
        colInscrits = database.getCollection(nomCollection);
        System.out.println("Dans getInscrits 2.1 *****************" + colInscrits);

        FindIterable<Document> listInscrits = colInscrits.find(whereQuery).sort(sortFields).projection(projectionFields);
        //System.out.println("colInscrits.count():"+colInscrits.count());
        // Getting the iterator
        Iterator it = listInscrits.iterator();
        System.out.println("Dans getInscrits 2.1 *****************");
        while (it.hasNext()) {
            System.out.println("Dans getInscrits 2.3 *****************");
            System.out.println(it.next());
        }
    }
    /**
     * FC10 : Cette fonction permet de modifier des Inscrits dans une collection.
     * Le paramètre whereQuery : permet de passer des conditions de recherche
     * Le paramètre updateExpressions : permet d'indiquer les champs à modifier
     * Le paramètre UpdateOptions : permet d'indiquer les options de mise à jour :
     * .upSert : insère si le document n'existe pas
     */
    public void updateInscrits(String nomCollection, Document whereQuery, Document updateExpressions, UpdateOptions updateOptions) {
        //Drop a collection
        System.out.println("\n\n\n*********** dans updateInscrits *****************");
        MongoCollection<Document> colInscrits = database.getCollection(nomCollection);
        UpdateResult updateResult = colInscrits.updateMany(whereQuery, updateExpressions);
        System.out.println("\nRésultat update : " + "getUpdate id: " + updateResult + " getMatchedCount : " + updateResult.getMatchedCount() + " getModifiedCount : " + updateResult.getModifiedCount());
    }
    /**
     * FC11 : Cette fonction permet de supprimer des inscrits dans une collection.
     * Le paramètre filters : permet de passer des conditions de recherche des employés à supprimer
     */
    public void deleteInscrits(String nomCollection, Document filters) {
        System.out.println("\n\n\n*********** dans deleteInscrits *****************");
        FindIterable<Document> listInscrit;
        Iterator it;
        MongoCollection<Document> colInscrits = database.getCollection(nomCollection);

        listInscrit = colInscrits.find(filters).sort(new Document("_id", 1));
        it = listInscrit.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteInscrits: avant suppression");

        colInscrits.deleteMany(filters);
        listInscrit = colInscrits.find(filters).sort(new Document("_id", 1));
        it = listInscrit.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteInscrits: Apres suppression");
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

    public void loadInscritsFromJsonArrayFile(String collectionName, String filePath, String fileName) {
        System.out.println(" \n ####  DANS loadInscritsFromJsonArrayFile ################################");
        System.out.println(filePath + fileName);
        JSONParser jsonParser = new JSONParser();
        MongoCollection<Document> colInscrits = database.getCollection(collectionName);
        try {
            JSONArray jsonInscritsArray = (JSONArray) jsonParser.parse(new FileReader(filePath + fileName));
            List<Document> inscritsList = new ArrayList<Document>();

            for (Object inscritObj : jsonInscritsArray) {
                JSONObject inscrit = (JSONObject) inscritObj;
                Document docInscrits = Document.parse(inscrit.toString());
                inscritsList.add(docInscrits);
            }
            colInscrits.insertMany(inscritsList);

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}