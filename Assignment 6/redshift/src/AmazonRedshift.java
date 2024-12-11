import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.*;


public class AmazonRedshift {
    private Connection con;

    public static void main(String[] args) {
        AmazonRedshift q = new AmazonRedshift();
        try {
            q.connect();
            q.drop();
            q.create();
           // q.insert();

            ResultSet rs1 = q.query1();
            System.out.println("Query 1 Results:\n" + resultSetToString(rs1, 10));

            ResultSet rs2 = q.query2();
            System.out.println("Query 2 Results:\n" + resultSetToString(rs2, 10));

            ResultSet rs3 = q.query3();
            System.out.println("Query 3 Results:\n" + resultSetToString(rs3, 10));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            q.close();
        }
    }

    public Connection connect() throws SQLException {
        String url = "jdbc:redshift://default-workgroup.498129898144.ap-south-1.redshift-serverless.amazonaws.com:5439/dev";
        String uid = "admin"; 
        String pw = "Admin123";  

        System.out.println("Connecting to database...");
        con = DriverManager.getConnection(url, uid, pw);
        return con;
    }

    public void close() {
        try {
            if (con != null && !con.isClosed()) {
                con.close();
                System.out.println("Connection closed.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void drop() {
        try (Statement stmt = con.createStatement()) {
            String dropSQL = "DROP SCHEMA IF EXISTS dev CASCADE;";
            stmt.executeUpdate(dropSQL);
            System.out.println("Dropped all tables.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void create() {
        System.out.println("Current working directory: " + System.getProperty("user.dir"));
        String[] ddlFiles = {"tpch_create.sql","lineitem.sql", "nation.sql", "partsupp.sql","supplier.sql","region.sql","part.sql","orders.sql","customer.sql"};
        for (String fileName : ddlFiles) {
            try {
                String ddl = readFile(fileName); // Read SQL file content
                try (Statement stmt = con.createStatement()) {
                    stmt.executeUpdate(ddl);
                    System.out.println("Executed DDL from " + fileName);
                }
            } catch (IOException | SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private String readFile(String fileName) throws IOException {
        File file = new File(fileName);
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }


    public ResultSet query1() throws SQLException {
        String sql = """
            SELECT
    o.O_ORDERKEY,
    o.O_TOTALPRICE,
    o.O_ORDERDATE
FROM
    ORDERS o
JOIN
    CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
JOIN
    NATION n ON c.C_NATIONKEY = n.N_NATIONKEY
WHERE
    n.N_NATIONKEY = '24'
ORDER BY
    o.O_ORDERDATE DESC
LIMIT 10;
        """;
        Statement stmt = con.createStatement();
        return stmt.executeQuery(sql);
    }

    public ResultSet query2() throws SQLException {
        String sql = """
            WITH largest_segment AS (
    SELECT
        C_MKTSEGMENT
    FROM
        CUSTOMER
    GROUP BY
        C_MKTSEGMENT
    ORDER BY
        COUNT(*) DESC
    LIMIT 1
)
SELECT
    c.C_CUSTKEY,
    SUM(o.O_TOTALPRICE) AS TOTAL_SPENT
FROM
    ORDERS o
JOIN
    CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
JOIN
    NATION n ON c.C_NATIONKEY = n.N_NATIONKEY
WHERE
    o.O_ORDERPRIORITY = '1-URGENT'
    AND o.O_ORDERSTATUS != 'F'
    AND n.N_REGIONKEY != '3'  -- Filtering out customers from Europe
    AND c.C_MKTSEGMENT = (SELECT C_MKTSEGMENT FROM largest_segment)
GROUP BY
    c.C_CUSTKEY
ORDER BY
    TOTAL_SPENT DESC;

        """;

        Statement stmt = con.createStatement();
        return stmt.executeQuery(sql);
    }

    public ResultSet query3() throws SQLException {
        String sql = """
            SELECT
    o.O_ORDERPRIORITY,
    COUNT(*) AS lineitem_count
FROM
    LINEITEM l
JOIN
    ORDERS o ON l.L_ORDERKEY = o.O_ORDERKEY
WHERE
    l.L_SHIPDATE BETWEEN '1997-04-01' AND '2003-04-01'
GROUP BY
    o.O_ORDERPRIORITY
ORDER BY
    o.O_ORDERPRIORITY ASC;
        """;
        Statement stmt = con.createStatement();
        return stmt.executeQuery(sql);
    }

    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
        StringBuilder buf = new StringBuilder(5000);
        int rowCount = 0;
        ResultSetMetaData meta = rst.getMetaData();

        buf.append("Total columns: ").append(meta.getColumnCount()).append('\n');
        if (meta.getColumnCount() > 0) buf.append(meta.getColumnName(1));

        for (int j = 2; j <= meta.getColumnCount(); j++) {
            buf.append(", ").append(meta.getColumnName(j));
        }
        buf.append('\n');

        while (rst.next()) {
            if (rowCount < maxrows) {
                for (int j = 0; j < meta.getColumnCount(); j++) {
                    buf.append(rst.getObject(j + 1));
                    if (j != meta.getColumnCount() - 1) buf.append(", ");
                }
                buf.append('\n');
            }
            rowCount++;
        }
        buf.append("Total results: ").append(rowCount);
        return buf.toString();
    }
}