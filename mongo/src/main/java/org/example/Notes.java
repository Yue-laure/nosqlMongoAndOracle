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
public class Notes {
    private MongoDatabase database;
    private String dbName = "tournoisDB";
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ubase";
    private String passWord = "UPass";
    private String notesCollectionName = "colNotes";

//    private String colCollectionName = "colNotes";

    private String notesFilePath = "C:\\Users\\18117\\Desktop\\Githubbbb\\nosqlProjetMongoOracle\\nosqlMongoAndOracle\\jsonDossier\\";

    private String notesCsvFileName = "notes.csv";
    private String notesJsonArrayFileName = "notes.json";
    private String destNotesCsvFileName = "notes_out.csv";

    public static void main(String[] args) {
        // Press Alt+Entrée with your caret at the highlighted text to see how
        // IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome to Mongotest!");
        try{
            Notes notes=new Notes();
            notes.dropCollectionNotes(notes.notesCollectionName);
            notes.createCollectionNotes(notes.notesCollectionName);
            notes.deleteNotes(notes.notesCollectionName,new Document());

            notes.testInsertOneNotes();
            notes.testInsertManyNotes();

            notes.loadNotesFromJsonArrayFile(
                notes.notesCollectionName,
                notes.notesFilePath,
                notes.notesJsonArrayFileName
            );
//afficher tous les Notes sans tri ni projection
            notes.getNotes(
                notes.notesCollectionName,
                new Document(),
                new Document(),
                new Document()
            );
            notes.updateNotes(
                notes.notesCollectionName,
                new Document("id_j",3),
                new Document ("$set",new Document("id_p", 3).append("id_t", 10).append("note",8)),
                new UpdateOptions()
            );
            notes.deleteNotes(notes.notesCollectionName, new Document("id_notes", 20));
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
    /**
     * FC1 : Constructeur Notes.
     * Dans ce constructeur sont effectuées les activités suivantes:
     * - Création d'une instance du client MongoClient
     * - Création d'une BD Mongo appelé RH
     * - Création d'un utilisateur appelé
     * - Chargement du pointeur vers la base RH
     */
    Notes() {
        // Creating a Mongo notes
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
    public void createCollectionNotes(String nomCollection) {
        //Creating a collection
        database.createCollection(nomCollection);
        System.out.println("Collection Notes created successfully");
    }
    /**
     * FC3 : Cette fonction permet de supprimer une collection
     * connaissant son nom.
     */
    public void dropCollectionNotes(String nomCollection) {
        //Drop a collection
        MongoCollection<Document> colNotes = null;
        System.out.println("\n\n\n*********** dans dropCollectionNotes *****************");
        System.out.println("!!!! Collection Notes : " + colNotes);
        colNotes = database.getCollection(nomCollection);
        System.out.println("!!!! Collection Notes : " + colNotes);
        colNotes.drop();
        System.out.println("Collection colNotes removed successfully !!!");
    }
    /**
     * FC4 : Cette fonction permet d'insérer un notes dans une collection.
     */
    public void insertOneNotes(String nomCollection, Document notes) {
        //Drop a collection
        MongoCollection<Document> colNotes = database.getCollection(nomCollection);
        colNotes.insertOne(notes);
        System.out.println("Document inserted successfully");
    }
    /**
     * FC5 : Cette fonction permet de tester la méthode Notes.
     */
    public void testInsertOneNotes() {
        Document notes = new Document("id_j", 2000).append("id_p", 2000).append("id_t", 10).append("note",8);
        this.insertOneNotes(this.notesCollectionName, notes);
        System.out.println("Document inserted successfully");
    }
    /**
     * FC6 : Cette fonction permet d'insérer plusieurs notes dans une collection
     */
    public void insertManyNotes(String nomCollection, List<Document> notes) {
        //Drop a collection
        MongoCollection<Document> colNotes = database.getCollection(nomCollection);
        colNotes.insertMany(notes);
        System.out.println("Many Documents inserted successfully");
    }
    /**
     * FC7 : Cette fonction permet de tester la fonction insertManyNotes
     */
    public void testInsertManyNotes() {
        List<Document> notes = Arrays.asList(
                new Document("id_j", 2222).append("id_p", 2222).append("id_t", 10).append("note",8),
                new Document("id_j", 2223).append("id_p", 2223).append("id_t", 10).append("note",9),
                new Document("id_j", 2224).append("id_p", 2224).append("id_t", 10).append("note",8.8)

                );
        this.insertManyNotes(this.notesCollectionName, notes);
    }
    /**
     * FC8 : Cette fonction permet de rechercher un tournoi dans une collection
     * connaissant son id.
     */
    public void getNotesById(String nomCollection, Integer NotesId) {
        //Drop a collection
        System.out.println("\n\n\n*********** dans getNotesById *****************");
        MongoCollection<Document> colNotes = database.getCollection(nomCollection);

        //BasicDBObject whereQuery = new BasicDBObject();
        Document whereQuery = new Document();
        whereQuery.put("id_notes", NotesId);
        //DBCursor cursor = colNotes.find(whereQuery);
        FindIterable<Document> listNotes = colNotes.find(whereQuery);

        // Getting the iterator
        Iterator it = listNotes.iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }
    /**
     * FC9 : Cette fonction permet de rechercher des notes dans une collection.
     * Le paramètre whereQuery : permet de passer des conditions de rechercher
     * Le paramètre projectionFields : permet d'indiquer les champs à afficher
     * Le paramètre sortFields : permet d'indiquer les champs de tri.
     */
    public void getNotes(String nomCollection, Document whereQuery, Document projectionFields, Document sortFields) {
        //Drop a collection
        System.out.println("\n\n\n*********** dans getNotes *****************");
        MongoCollection<Document> colNotes = null;
        System.out.println("Dans getNotes 2.1 *****************" + colNotes);
        System.out.println("Dans getNotes 2.2 : database:" + database);
        colNotes = database.getCollection(nomCollection);
        System.out.println("Dans getNotes 2.1 *****************" + colNotes);

        FindIterable<Document> listNotes = colNotes.find(whereQuery).sort(sortFields).projection(projectionFields);
        //System.out.println("colNotes.count():"+colNotes.count());
        // Getting the iterator
        Iterator it = listNotes.iterator();
        System.out.println("Dans getNotes 2.1 *****************");
        while (it.hasNext()) {
            System.out.println("Dans getNotes 2.3 *****************");
            System.out.println(it.next());
        }
    }
    /**
     * FC10 : Cette fonction permet de modifier des Notes dans une collection.
     * Le paramètre whereQuery : permet de passer des conditions de recherche
     * Le paramètre updateExpressions : permet d'indiquer les champs à modifier
     * Le paramètre UpdateOptions : permet d'indiquer les options de mise à jour :
     * .upSert : insère si le document n'existe pas
     */
    public void updateNotes(String nomCollection, Document whereQuery, Document updateExpressions, UpdateOptions updateOptions) {
        //Drop a collection
        System.out.println("\n\n\n*********** dans updateNotes *****************");
        MongoCollection<Document> colNotes = database.getCollection(nomCollection);
        UpdateResult updateResult = colNotes.updateMany(whereQuery, updateExpressions);
        System.out.println("\nRésultat update : " + "getUpdate id: " + updateResult + " getMatchedCount : " + updateResult.getMatchedCount() + " getModifiedCount : " + updateResult.getModifiedCount());
    }
    /**
     * FC11 : Cette fonction permet de supprimer des notes dans une collection.
     * Le paramètre filters : permet de passer des conditions de recherche des employés à supprimer
     */
    public void deleteNotes(String nomCollection, Document filters) {
        System.out.println("\n\n\n*********** dans deleteNotes *****************");
        FindIterable<Document> listNotes;
        Iterator it;
        MongoCollection<Document> colNotes = database.getCollection(nomCollection);

        listNotes = colNotes.find(filters).sort(new Document("_id", 1));
        it = listNotes.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteNotes: avant suppression");

        colNotes.deleteMany(filters);
        listNotes = colNotes.find(filters).sort(new Document("_id", 1));
        it = listNotes.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteNotes: Apres suppression");
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

    public void loadNotesFromJsonArrayFile(String collectionName, String filePath, String fileName) {
        System.out.println(" \n ####  DANS loadNotesFromJsonArrayFile ################################");
        System.out.println(filePath + fileName);
        JSONParser jsonParser = new JSONParser();
        MongoCollection<Document> colNotes = database.getCollection(collectionName);
        try {
            JSONArray jsonNotesArray = (JSONArray) jsonParser.parse(new FileReader(filePath + fileName));
            List<Document> notesList = new ArrayList<Document>();

            for (Object noteObj : jsonNotesArray) {
                JSONObject note = (JSONObject) noteObj;
                Document docNotes = Document.parse(note.toString());
                notesList.add(docNotes);
            }
            colNotes.insertMany(notesList);

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}