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
public class Tournois {
    private MongoDatabase database;
    private String dbName = "tournoisDB";
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ubase";
    private String passWord = "UPass";
    private String tournoisCollectionName = "colTournois";
    // private String colCollectionName = "colInscrits";

    private String tournoisFilePath = "C:\\Users\\18117\\Desktop\\Githubbbb\\nosqlProjetMongoOracle\\nosqlMongoAndOracle\\jsonDossier\\";

    private String tournoisCsvFileName = "tournois.csv";
    private String tournoisJsonArrayFileName = "tournois.json";
    private String destTournoisCsvFileName = "tournois_out.csv";

    public static void main(String[] args) {
        // Press Alt+Entrée with your caret at the highlighted text to see how
        // IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome to Mongotest!");
        try{
            Tournois tournois=new Tournois();
            tournois.dropCollectionTournois(tournois.tournoisCollectionName);
            tournois.createCollectionTournois(tournois.tournoisCollectionName);

            tournois.testInsertOneTournoi();
            tournois.testInsertManyTournois();

            tournois.loadTournoisFromJsonArrayFile(
                    tournois.tournoisCollectionName,
                    tournois.tournoisFilePath,
                    tournois.tournoisJsonArrayFileName
            );
            //afficher tous les toirnois sans tri ni projection
            tournois.getTournois(
                    tournois.tournoisCollectionName,
                    new Document(),
                    new Document(),
                    new Document()
            );
            tournois.updateTournois(
                    tournois.tournoisCollectionName,
                    new Document("id_tournois","20"),
                    new Document ("$set",new Document("nom_t", "Trampoline Spring").append("date_t", "03/02/2023").append("adresse_t", " Lille stade").append("organisateur_t", "OPO-586")),
                    new UpdateOptions()
            );
            tournois.deleteTournois(tournois.tournoisCollectionName, new Document("id_tournois", 20));

        }catch (Exception e) {
            e.printStackTrace();
        }

    }
    /**
     * FC1 : Constructeur Tournois.
     * Dans ce constructeur sont effectuées les activités suivantes:
     * - Création d'une instance du client MongoClient
     * - Création d'une BD Mongo appelé RH
     * - Création d'un utilisateur appelé
     * - Chargement du pointeur vers la base RH
     */
    Tournois() {
        // Creating a Mongo tournois
        MongoClient mongoClient = new MongoClient(hostName, port);
        // Creating Credentials
        // RH : Ressources Humaines
        MongoCredential credential;
        credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray());
        System.out.println("FC1 Connected to the database successfully");
        System.out.println("Credentials ::" + credential);
        // Accessing the database
        database = mongoClient.getDatabase(dbName);
    }
    /**
     * FC2 : Cette fonction permet de créer une collection
     * de nom nomCollection.
     */
    public void createCollectionTournois(String nomCollection) {
        //Creating a collection
        database.createCollection(nomCollection);
        System.out.println("FC2 Collection Tournois created successfully");
    }
    /**
     * FC3 : Cette fonction permet de supprimer une collection
     * connaissant son nom.
     */
    public void dropCollectionTournois(String nomCollection) {
        //Drop a collection
        MongoCollection<Document> colTournois = null;
        System.out.println("\n\n\n***********FC3 dans dropCollectionTournois *****************");
        System.out.println("!!!! Collection Tournois : " + colTournois);
        colTournois = database.getCollection(nomCollection);
        System.out.println("!!!! Collection Tournois : " + colTournois);
        colTournois.drop();
        System.out.println("Collection colTournois removed successfully !!!");
    }
    /**
     * FC4 : Cette fonction permet d'insérer un tournois dans une collection.
     */
    public void insertOneTournois(String nomCollection, Document tournois) {
        //Drop a collection
        MongoCollection<Document> colTournois = database.getCollection(nomCollection);
        colTournois.insertOne(tournois);
        System.out.println("FC4 Document inserted successfully");
    }
    /**
     * FC5 : Cette fonction permet de tester la méthode Tournois.
     */
    public void testInsertOneTournoi() {
        Document tournois = new Document("id_tournois", "20").append("nom_t", "Bond Trampoline").append("date_t", "03/02/2022").append("adresse_t", "Paris Fitness").append("organisateur_t", "PPP-331");
        this.insertOneTournois(this.tournoisCollectionName, tournois);
        System.out.println("FC5 Document inserted successfully");
    }
    /**
     * FC6 : Cette fonction permet d'insérer plusieurs tournois dans une collection
     */
    public void insertManyTournois(String nomCollection, List<Document> tournois) {
        //Drop a collection
        MongoCollection<Document> colTournois = database.getCollection(nomCollection);
        colTournois.insertMany(tournois);
        System.out.println("FC6 Many Documents inserted successfully");
    }
    /**
     * FC7 : Cette fonction permet de tester la fonction insertManyTournois
     */
    public void testInsertManyTournois() {
        List<Document> tournois = Arrays.asList(
                new Document("id_tournois", "21").append("nom_t", "Trampoline Spring").append("date_t", "03/02/2022").append("adresse_t", "Allée de la Chapelle Saint-Pierre").append("organisateur_t", "APA-586"),
                new Document("id_tournois", "22").append("nom_t", "Trampoline Summer").append("date_t", "03/06/2022").append("adresse_t", "Allée de la Chapelle Saint-Pierr").append("organisateur_t", "SPA-331"),
                new Document("id_tournois", "23").append("nom_t", "Trampoline Automn").append("date_t", "03/09/2022").append("adresse_t", "Paris Fitness").append("organisateur_t", "APP-331"),
                new Document("id_tournois", "24").append("nom_t", "Trampoline Winter").append("date_t", "03/011/2022").append("adresse_t", "Paris Fitness").append("organisateur_t", "HJG-685")
        );
        this.insertManyTournois(this.tournoisCollectionName, tournois);
    }
    /**
     * FC8 : Cette fonction permet de rechercher un tournoi dans une collection
     * connaissant son id.
     */
    public void getTournoisById(String nomCollection, Integer TournoisId) {
        //Drop a collection
        System.out.println("\n\n\n***********FC8 dans getTournoisById *****************");
        MongoCollection<Document> colTournois = database.getCollection(nomCollection);

        //BasicDBObject whereQuery = new BasicDBObject();
        Document whereQuery = new Document();
        whereQuery.put("id_tournois", TournoisId);
        //DBCursor cursor = colTournois.find(whereQuery);
        FindIterable<Document> listTournois = colTournois.find(whereQuery);

        // Getting the iterator
        Iterator it = listTournois.iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }
    /**
     * FC9 : Cette fonction permet de rechercher des tournois dans une collection.
     * Le paramètre whereQuery : permet de passer des conditions de rechercher
     * Le paramètre projectionFields : permet d'indiquer les champs à afficher
     * Le paramètre sortFields : permet d'indiquer les champs de tri.
     */
    public void getTournois(String nomCollection, Document whereQuery, Document projectionFields, Document sortFields) {
        //Drop a collection
        System.out.println("\n\n\n***********FC9 dans getTournois *****************");
        MongoCollection<Document> colTournois = null;
        System.out.println("Dans getTournois 2.1 *****************" + colTournois);
        System.out.println("Dans getTournois 2.2 : database:" + database);
        colTournois = database.getCollection(nomCollection);
        System.out.println("Dans getTournois 2.1 *****************" + colTournois);

        FindIterable<Document> listTournois = colTournois.find(whereQuery).sort(sortFields).projection(projectionFields);
        //System.out.println("colTournois.count():"+colTournois.count());
        // Getting the iterator
        Iterator it = listTournois.iterator();
        System.out.println("Dans getTournois 2.1 *****************");
        while (it.hasNext()) {
            System.out.println("Dans getTournois 2.3 *****************");
            System.out.println(it.next());
        }
    }
    /**
     * FC10 : Cette fonction permet de modifier des Tournois dans une collection.
     * Le paramètre whereQuery : permet de passer des conditions de recherche
     * Le paramètre updateExpressions : permet d'indiquer les champs à modifier
     * Le paramètre UpdateOptions : permet d'indiquer les options de mise à jour :
     * .upSert : insère si le document n'existe pas
     */
    public void updateTournois(String nomCollection, Document whereQuery, Document updateExpressions, UpdateOptions updateOptions) {
        //Drop a collection
        System.out.println("\n\n\n***********FC10 dans updateTournois *****************");
        MongoCollection<Document> colTournois = database.getCollection(nomCollection);
        UpdateResult updateResult = colTournois.updateMany(whereQuery, updateExpressions);
        System.out.println("\nRésultat update : " + "getUpdate id: " + updateResult + " getMatchedCount : " + updateResult.getMatchedCount() + " getModifiedCount : " + updateResult.getModifiedCount());
    }
    /**
     * FC11 : Cette fonction permet de supprimer des tournois dans une collection.
     * Le paramètre filters : permet de passer des conditions de recherche des employés à supprimer
     */
    public void deleteTournois(String nomCollection, Document filters) {
        System.out.println("\n\n\n***********FC11 dans deleteTournois *****************");
        FindIterable<Document> listTournoi;
        Iterator it;
        MongoCollection<Document> colTournois = database.getCollection(nomCollection);

        listTournoi = colTournois.find(filters).sort(new Document("_id", 1));
        it = listTournoi.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteTournois: avant suppression");

        colTournois.deleteMany(filters);
        listTournoi = colTournois.find(filters).sort(new Document("_id", 1));
        it = listTournoi.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteTournois: Apres suppression");
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

    public void loadTournoisFromJsonArrayFile(String collectionName, String filePath, String fileName) {
        System.out.println(" \n ####FC12  DANS loadTournoisFromJsonArrayFile ################################");
        System.out.println(filePath + fileName);
        JSONParser jsonParser = new JSONParser();
        MongoCollection<Document> colTournois = database.getCollection(collectionName);
        try {
            JSONArray jsonTournoisArray = (JSONArray) jsonParser.parse(new FileReader(filePath + fileName));
            List<Document> tournoisList = new ArrayList<Document>();

            for (Object tournoiObj : jsonTournoisArray) {
                JSONObject tournoi = (JSONObject) tournoiObj;
                Document docTournois = Document.parse(tournoi.toString());
                tournoisList.add(docTournois);
            }
            colTournois.insertMany(tournoisList);

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}