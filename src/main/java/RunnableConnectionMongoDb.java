import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RunnableConnectionMongoDb {

    static boolean read = false;
    static AtomicLong start = new AtomicLong(0);
    static AtomicLong end = new AtomicLong(0);
    static AtomicInteger atomicInt = new AtomicInteger(0);

    static int numberOfConnections = 50;
    static int numberOfDocuments = 100;
    static double fillKb = 0;
    static int batchSize = 1;
    static String db = "mydb";
    static String coll = "mydata";
    static boolean readAllData = false;

    /*
        uri = "mongodb+srv://user:password@erbd-7dos8.mongodb.net/test?retryWrites=true&w=majority";
     */
    static String uri = "mongodb://localhost:27017";

    public static void main(String [] args)
    {

        if ( args.length > 0 )
        {
            for ( int i=0; i < args.length; i ++ )
            {
                if ( args[i].equalsIgnoreCase( "-b" ) ) {
                    batchSize = Integer.parseInt(args[i+1]);
                }
                if ( args[i].equalsIgnoreCase( "-u" ) ) {
                    uri = args[i+1];
                }
                if ( args[i].equalsIgnoreCase( "-t" ) ) {
                    numberOfConnections = Integer.parseInt(args[i+1]);
                }
                if ( args[i].equalsIgnoreCase( "-d" ) ) {
                    numberOfDocuments = Integer.parseInt(args[i+1]);
                }
                if ( args[i].equalsIgnoreCase( "-f" ) ) {
                    fillKb = Double.parseDouble(args[i+1]);
                }
                if ( args[i].equalsIgnoreCase( "-r" ) ) {
                    db = args[i+1];
                }
                if ( args[i].equalsIgnoreCase( "-x" ) ) {
                    coll = args[i+1];
                }
                if ( args[i].equalsIgnoreCase( "--read-all" ) ) {
                    System.out.println(i + " " + args[i]);
                    readAllData = true;
                }                
                if (    args[i].equalsIgnoreCase( "help" ) ||
                        args[i].equalsIgnoreCase( "-h" ))                                             
                {
                    System.out.println("\nUsage: java -jar THREADS.jar -t 10 -d 5000 -f 36 -b 100" +
                            "\n\n\t -u connection uri: e.g:  \"mongodb://localhost:27017,localhost:27018,localhost:27019\"" +
                            "\n\t -t 10 -> [number of threads / connections] " +
                            "\n\t -d 5000 -> [number of documents] " +
                            "\n\t -f 36 -> [filler size i.e. 36 --> Doc size of 36Kb] ( valu is a double e.g. 0.5 is 1/2 Kb )" +
                            "\n\t -b 100 -> [batch size i.e. 100] " +                           
                            "\n\t -r mydb -> [mongo database name] " +
                            "\n\t -x mycollection -> [mongodb collection name] " +
                            "\n\t --read-all Read all Data as a scan " +
                            "\n\t    e.g.: RunnableConnectionMongoDb -u \"mongodb://localhost:27017,localhost:27018,localhost:27019\" --read-all -c db2 -x bank" +
                            "\n\n\t default writes: -t 50 -d 100 -f 0 " +
                            "\n");
                    System.exit(0);
                }
            }
        }

        MongoClient mongoClient = MongoClients.create(uri);
        MongoDatabase database = mongoClient.getDatabase(db);
        // MongoCollection<Document> collection = 
        // database.getCollection(coll).withReadPreference(ReadPreference.primary());
        MongoCollection<Document> collection = 
            database.getCollection(coll).withReadPreference(ReadPreference.nearest());

        start.set( System.currentTimeMillis() );

        if ( readAllData == false ) {
            collection.deleteMany(new Document());
        }
        else
        {            
            FindIterable<Document> iterDoc = collection.find();
            Iterator<Document> it = iterDoc.iterator();
            while (it.hasNext()) {
                System.out.println(it.next());                       
            }
            end.set( System.currentTimeMillis() );
            System.out.println("Time taken to read all data: " + (end.get() - start.get()) + " milliseconds.");
            System.exit(0);
        }
    
        int numberOfDocsPerThread = numberOfDocuments / numberOfConnections;

        for ( int i=0; i < numberOfConnections; i++ )
        {
            RunnableConnection R1 = new RunnableConnection("MongoDB Connection: ".concat(Integer.toString(i)), mongoClient, numberOfDocsPerThread);
            R1.start();
        }
    }
}

class RunnableConnection implements Runnable {
    private Thread t;
    private String threadName;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    private int numberOfDocuments;

    RunnableConnection(String name,  MongoClient mongoClient, int numOfDocs) {
        threadName = name;
        this.mongoClient = mongoClient;
        this.numberOfDocuments = numOfDocs;
    }

    public void run() 
    {
        database = mongoClient.getDatabase(RunnableConnectionMongoDb.db);
        collection = database.getCollection(RunnableConnectionMongoDb.coll);

        RunnableConnectionMongoDb.atomicInt.incrementAndGet();

        char [] filler = new char[ (int)(1000 * RunnableConnectionMongoDb.fillKb)];
        Arrays.fill(filler,'a');
        String backpack = new String( filler);

        String[] alpha = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"};

        int num = numberOfDocuments;
        Document doc = new Document();

        List<WriteModel<Document>> ops = new ArrayList<>();

        for (int i = 1; i <= num; i++) {
            int random = new Random().nextInt(26);
            String someChar = alpha[random];
            int remainder = i % 3;

            // Batching with new Doc for each record ( cant just clear it )
            if ( RunnableConnectionMongoDb.batchSize > 1 ) {
                doc = new Document();
            }

            doc.append("rand", random)
                    .append("x", i)
                    .append("y", remainder)
                    .append("z", someChar + i)
                    .append("filler", backpack );

            // Batching
            if ( RunnableConnectionMongoDb.batchSize > 1 ) {
                ops.add( new InsertOneModel(doc) );
            }
            else {
                collection.insertOne(doc);
                doc.clear();
            }

            // Reached Batch size limit now send
            if ( ops.size() >= RunnableConnectionMongoDb.batchSize ) {
                collection.bulkWrite( ops, new BulkWriteOptions().ordered(false) );
                // System.out.println("Batched up " + ops.size() );
                ops.clear();
            }
        }

        // Batching remaining docs
        if ( RunnableConnectionMongoDb.batchSize > 1 && ops.size() > 0 ) {
            collection.bulkWrite( ops, new BulkWriteOptions().ordered(false) );
        }

        RunnableConnectionMongoDb.end.set( Math.max( System.currentTimeMillis(), RunnableConnectionMongoDb.end.get() ) );
        int currentNumThreads = RunnableConnectionMongoDb.atomicInt.getAndDecrement();
        System.out.println("Thread " +  threadName + " exiting. Threads running still: " + currentNumThreads);

        // String for stats either seconds or ms
        String outTime = "";
        long taken = (RunnableConnectionMongoDb.end.get() - RunnableConnectionMongoDb.start.get());
        if ( (RunnableConnectionMongoDb.end.get() - RunnableConnectionMongoDb.start.get()) <= 1000 )
            outTime = taken + " milliseconds.";
        else
            outTime = ( taken / 1000 ) + " seconds.";

        // If last known thread then spit timings
        if ( currentNumThreads == 1 )
            System.out.println(" -->> Time taken to write " +
                    RunnableConnectionMongoDb.numberOfDocuments +
                    " documents to MongoDB is " +
                    outTime
            );
    }

    public void start () {
        System.out.println("Starting " +  threadName );
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }
}
