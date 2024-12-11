import java.sql.*;

public class SQLonRDS {

    private Connection con;
    private String url = "mydb.cl2ycy4ks8pr.ap-south-1.rds.amazonaws.com:3306";
    private String uid = "admin";
    private String pw = "admin123";

    public static void main(String[] args) {
        SQLonRDS q = new SQLonRDS();
        try {
            q.connect();
            q.drop();
            q.create();
            q.insert();
            q.queryOne();
            q.queryTwo();
            q.queryThree();
            q.delete();
            q.close();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void connect() throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String jdbcUrl = "jdbc:mysql://" + url + "/mydb?user=" + uid + "&password=" + pw;
        System.out.println("Connecting to database...#####");
        con = DriverManager.getConnection(jdbcUrl);
        System.out.println("Connection Successful.");
    }

    public void drop() throws SQLException {
        Statement stmt = con.createStatement();
        stmt.execute("DROP TABLE IF EXISTS stockprice");
        stmt.execute("DROP TABLE IF EXISTS company");
        System.out.println("Tables dropped successfully.#####");
    }

    public void create() throws SQLException {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE company (" +
                     "id INT PRIMARY KEY, " +
                     "name VARCHAR(50), " +
                     "ticker CHAR(10), " +
                     "annualRevenue DECIMAL(15, 2), " +
                     "numEmployees INT)");

        stmt.execute("CREATE TABLE stockprice (" +
                     "companyId INT, " +
                     "priceDate DATE, " +
                     "openPrice DECIMAL(10, 2), " +
                     "highPrice DECIMAL(10, 2), " +
                     "lowPrice DECIMAL(10, 2), " +
                     "closePrice DECIMAL(10, 2), " +
                     "volume INT, " +
                     "PRIMARY KEY (companyId, priceDate), " +
                     "FOREIGN KEY (companyId) REFERENCES company(id))");
        System.out.println("Tables created successfully.#####");
    }

    public void insert() throws SQLException {
        Statement stmt = con.createStatement();

        // Insert company data
        stmt.execute("INSERT INTO company VALUES (1, 'Apple', 'AAPL', 387540000000.00, 154000)");
        stmt.execute("INSERT INTO company VALUES (2, 'GameStop', 'GME', 611000000.00, 12000)");
        stmt.execute("INSERT INTO company VALUES (3, 'Handy Repair', NULL, 2000000.00, 50)");
        stmt.execute("INSERT INTO company VALUES (4, 'Microsoft', 'MSFT', 198270000000.00, 221000)");
        stmt.execute("INSERT INTO company VALUES (5, 'StartUp', NULL, 50000.00, 3)");

        // Insert stock price data (sample rows, add the rest as needed)
        stmt.execute("INSERT INTO stockprice (companyId, priceDate, openPrice, highPrice, lowPrice, closePrice, volume) VALUES" +
                        "(1, '2022-08-15', 171.52, 173.39, 171.35, 173.19, 54091700),\n" + //
                        "(1, '2022-08-16', 172.78, 173.71, 171.66, 173.03, 56377100),\n" + //
                        "(1, '2022-08-17', 172.77, 176.15, 172.57, 174.55, 79542000),\n" + //
                        "(1, '2022-08-18', 173.75, 174.90, 173.12, 174.15, 62290100),\n" + //
                        "(1, '2022-08-19', 173.03, 173.74, 171.31, 171.52, 70211500),\n" + //
                        "(1, '2022-08-22', 169.69, 169.86, 167.14, 167.57, 69026800),\n" + //
                        "(1, '2022-08-23', 167.08, 168.71, 166.65, 167.23, 54147100),\n" + //
                        "(1, '2022-08-24', 167.32, 168.11, 166.25, 167.53, 53841500),\n" + //
                        "(1, '2022-08-25', 168.78, 170.14, 168.35, 170.03, 51218200),\n" + //
                        "(1, '2022-08-26', 170.57, 171.05, 163.56, 163.62, 78823500),\n" + //
                        "(1, '2022-08-29', 161.15, 162.90, 159.82, 161.38, 73314000),\n" + //
                        "(1, '2022-08-30', 162.13, 162.56, 157.72, 158.91, 77906200),\n" + //
                        "(2, '2022-08-15', 39.75, 40.39, 38.81, 39.68, 5243100),\n" + //
                        "(2, '2022-08-16', 39.17, 45.53, 38.60, 42.19, 23602800),\n" + //
                        "(2, '2022-08-17', 42.18, 44.36, 40.41, 40.52, 9766400),\n" + //
                        "(2, '2022-08-18', 39.27, 40.07, 37.34, 37.93, 8145400),\n" + //
                        "(2, '2022-08-19', 35.18, 37.19, 34.67, 36.49, 9525600),\n" + //
                        "(2, '2022-08-22', 34.31, 36.20, 34.20, 34.50, 5798600),\n" + //
                        "(2, '2022-08-23', 34.70, 34.99, 33.45, 33.53, 4836300),\n" + //
                        "(2, '2022-08-24', 34.00, 34.94, 32.44, 32.50, 5620300),\n" + //
                        "(2, '2022-08-25', 32.84, 32.89, 31.50, 31.96, 4726300),\n" + //
                        "(2, '2022-08-26', 31.50, 32.38, 30.63, 30.94, 4289500),\n" + //
                        "(2, '2022-08-29', 30.48, 32.75, 30.38, 31.55, 4292700),\n" + //
                        "(2, '2022-08-30', 31.62, 31.87, 29.42, 29.84, 5060200),\n" + //
                        "(4, '2022-08-15', 291.00, 294.18, 290.11, 293.47, 18085700),\n" + //
                        "(4, '2022-08-16', 291.99, 294.04, 290.42, 292.71, 18102900),\n" + //
                        "(4, '2022-08-17', 289.74, 293.35, 289.47, 291.32, 18253400),\n" + //
                        "(4, '2022-08-18', 290.19, 291.91, 289.08, 290.17, 17186200),\n" + //
                        "(4, '2022-08-19', 288.90, 289.25, 285.56, 286.15, 20557200),\n" + //
                        "(4, '2022-08-22', 282.08, 282.46, 277.22, 277.75, 25061100),\n" + //
                        "(4, '2022-08-23', 276.44, 278.86, 275.40, 276.44, 17527400),\n" + //
                        "(4, '2022-08-24', 275.41, 277.23, 275.11, 275.79, 18137000),\n" + //
                        "(4, '2022-08-25', 277.33, 279.02, 274.52, 278.85, 16583400),\n" + //
                        "(4, '2022-08-26', 279.08, 280.34, 267.98, 268.09, 27532500),\n" + //
                        "(4, '2022-08-29', 265.85, 267.40, 263.85, 265.23, 20338500),\n" + //
                        "(4, '2022-08-30', 266.67, 267.05, 260.66, 262.97, 22767100);\n" + //
                        "");
        // Add more stockprice records as required
        System.out.println("Data inserted successfully.#####");
    }

    public void delete() throws SQLException {
        Statement stmt = con.createStatement();
        stmt.execute("DELETE FROM stockprice WHERE priceDate < '2022-08-20' OR companyId = 2");
        System.out.println("Records deleted successfully.#####");
    }

    public void queryOne() throws SQLException {
        System.out.println("Query One #####");

        String query = "SELECT name, annualRevenue, numEmployees FROM company " +
                       "WHERE numEmployees > 10000 OR annualRevenue < 1000000 " +
                       "ORDER BY name ASC";
        ResultSet rs = con.createStatement().executeQuery(query);
        System.out.println(resultSetToString(rs, 50));
    }

    public void queryTwo() throws SQLException {
        System.out.println("Query Two #####");
        String query = "SELECT c.name, c.ticker, MIN(sp.lowPrice), MAX(sp.highPrice), " +
                       "AVG(sp.closePrice), AVG(sp.volume) " +
                       "FROM stockprice sp " +
                       "JOIN company c ON sp.companyId = c.id " +
                       "WHERE sp.priceDate BETWEEN '2022-08-22' AND '2022-08-26' " +
                       "GROUP BY c.name, c.ticker " +
                       "ORDER BY AVG(sp.volume) DESC";
        ResultSet rs = con.createStatement().executeQuery(query);
        System.out.println(resultSetToString(rs, 50));
    }

    public void queryThree() throws SQLException {
        System.out.println("Query Three #####");
        String query = "SELECT c.name, c.ticker, sp.closePrice FROM company c " +
                       "LEFT JOIN stockprice sp ON c.id = sp.companyId " +
                       "WHERE sp.priceDate = '2022-08-30' AND " +
                       "sp.closePrice <= 1.1 * (SELECT AVG(closePrice) FROM stockprice " +
                       "WHERE priceDate BETWEEN '2022-08-15' AND '2022-08-19') " +
                       "OR c.ticker IS NULL " +
                       "ORDER BY c.name ASC";
        ResultSet rs = con.createStatement().executeQuery(query);
        System.out.println(resultSetToString(rs, 50));
    }

    public void close() throws SQLException {
        System.out.println("Close #####");
        if (con != null) {
            con.close();
        }
    }

    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
        System.out.println("resultSetToString #####");
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;

        if (rst == null)
            return "ERROR: No ResultSet";

        ResultSetMetaData meta = rst.getMetaData();
        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');
        for (int j = 1; j <= meta.getColumnCount(); j++) {
            buf.append(meta.getColumnName(j));
            if (j < meta.getColumnCount())
                buf.append(", ");
        }
        buf.append('\n');

        while (rst.next() && rowCount < maxrows) {
            for (int j = 1; j <= meta.getColumnCount(); j++) {
                buf.append(rst.getObject(j));
                if (j < meta.getColumnCount())
                    buf.append(", ");
            }
            buf.append('\n');
            rowCount++;
        }
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }
}
