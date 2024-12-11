import com.mongodb.client.*;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class MongoDB {
    public static final String DATABASE_NAME = "tpch";
    public static final String CUSTOMER_COLLECTION = "customer";
    public static final String ORDERS_COLLECTION = "orders";
    public static final String CUSTORDERS_COLLECTION = "custorders";

    private MongoClient mongoClient;
    private MongoDatabase db;

    public static void main(String[] args) throws Exception {
        MongoDB mongodb = new MongoDB();
        mongodb.connect();
        mongodb.load();
        mongodb.loadNest();

        // System.out.println(mongodb.query1(1000));
        // System.out.println(mongodb.query2(32));
        // System.out.println(mongodb.query2Nest(32));
        // System.out.println(mongodb.query3());
        // System.out.println(mongodb.query3Nest());



        System.out.println("Query 1: " + mongodb.query1(1));
        System.out.println("Query 2: " + mongodb.query2(32));
        System.out.println("Query 2 Nested: " + mongodb.query2Nest(32));
        System.out.println("Query 3: " + mongodb.query3());
        System.out.println("Query 3 Nested: " + mongodb.query3Nest());
        System.out.println("Query 4: " + mongodb.query4());
        System.out.println("Query 4 Nested: " + mongodb.query4Nest());
    




    }

    public void connect() {
        String connectionString = "mongodb+srv://admin:Admin123@cluster1.eoxjd.mongodb.net/?retryWrites=true&w=majority";
        mongoClient = MongoClients.create(connectionString);
        db = mongoClient.getDatabase(DATABASE_NAME);
    }

    public void load() throws Exception {
        MongoCollection<Document> customerCollection = db.getCollection(CUSTOMER_COLLECTION);
        MongoCollection<Document> ordersCollection = db.getCollection(ORDERS_COLLECTION);

        customerCollection.drop();
        ordersCollection.drop();

        // Load customer data
        try (BufferedReader br = new BufferedReader(new FileReader("customer.tbl"))) {
            String line;
            List<Document> customers = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\|");
                Document doc = new Document("custkey", Integer.parseInt(parts[0]))
                        .append("name", parts[1])
                        .append("address", parts[2])
                        .append("nationkey", Integer.parseInt(parts[3]))
                        .append("phone", parts[4])
                        .append("acctbal", Double.parseDouble(parts[5]))
                        .append("mktsegment", parts[6])
                        .append("comment", parts[7]);
                customers.add(doc);
            }
            customerCollection.insertMany(customers);
        }

        // Load orders data
        try (BufferedReader br = new BufferedReader(new FileReader("order.tbl"))) {
            String line;
            List<Document> orders = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\|");
                Document doc = new Document("orderkey", Integer.parseInt(parts[0]))
                        .append("custkey", Integer.parseInt(parts[1]))
                        .append("orderstatus", parts[2])
                        .append("totalprice", Double.parseDouble(parts[3]))
                        .append("orderdate", parts[4])
                        .append("orderpriority", parts[5])
                        .append("clerk", parts[6])
                        .append("shippriority", Integer.parseInt(parts[7]))
                        .append("comment", parts[8]);
                orders.add(doc);
            }
            ordersCollection.insertMany(orders);
        }
    }

    public void loadNest() throws Exception {
        MongoCollection<Document> custOrdersCollection = db.getCollection(CUSTORDERS_COLLECTION);
        custOrdersCollection.drop();

        MongoCollection<Document> customerCollection = db.getCollection(CUSTOMER_COLLECTION);
        MongoCollection<Document> ordersCollection = db.getCollection(ORDERS_COLLECTION);

        List<Document> customers = customerCollection.find().into(new ArrayList<>());

        for (Document customer : customers) {
            int custkey = customer.getInteger("custkey");
            List<Document> orders = ordersCollection.find(eq("custkey", custkey)).into(new ArrayList<>());
            customer.append("orders", orders);
            custOrdersCollection.insertOne(customer);
        }
    }

    public String query1(int custkey) {
        MongoCollection<Document> customerCollection = db.getCollection(CUSTOMER_COLLECTION);
        Document customer = customerCollection.find(eq("custkey", custkey)).first();
        return customer != null ? customer.getString("name") : null;
    }

    public String query2(int orderId) {
        MongoCollection<Document> ordersCollection = db.getCollection(ORDERS_COLLECTION);
        Document order = ordersCollection.find(eq("orderkey", orderId)).first();
        return order != null ? order.getString("orderdate") : null;
    }

    public String query2Nest(int orderId) {
        MongoCollection<Document> custOrdersCollection = db.getCollection(CUSTORDERS_COLLECTION);
        Document customer = custOrdersCollection.find(eq("orders.orderkey", orderId)).first();
        if (customer != null) {
            List<Document> orders = customer.getList("orders", Document.class);
            for (Document order : orders) {
                if (order.getInteger("orderkey") == orderId) {
                    return order.getString("orderdate");
                }
            }
        }
        return null;
    }

    public long query3() {
        MongoCollection<Document> ordersCollection = db.getCollection(ORDERS_COLLECTION);
        return ordersCollection.countDocuments();
    }

    public long query3Nest() {
        MongoCollection<Document> custOrdersCollection = db.getCollection(CUSTORDERS_COLLECTION);
        List<Document> customers = custOrdersCollection.find().into(new ArrayList<>());
        long count = 0;
        for (Document customer : customers) {
            count += customer.getList("orders", Document.class).size();
        }
        return count;
    }



public List<Document> query4() {
    MongoCollection<Document> ordersCollection = db.getCollection("orders");

    return ordersCollection.aggregate(
        List.of(
            new Document("$group", new Document("_id", "$custkey")
                .append("totalAmount", new Document("$sum", "$totalprice"))),
            new Document("$sort", new Document("totalAmount", -1)),
            new Document("$limit", 5)
        )
    ).into(new ArrayList<>());
}


public List<Document> query4Nest() {
    MongoCollection<Document> custOrdersCollection = db.getCollection("custorders");

    return custOrdersCollection.aggregate(
        List.of(
            new Document("$project", new Document("name", 1)
                .append("totalAmount", new Document("$sum", "$orders.totalprice"))),
            new Document("$sort", new Document("totalAmount", -1)),
            new Document("$limit", 5)
        )
    ).into(new ArrayList<>());
}
}