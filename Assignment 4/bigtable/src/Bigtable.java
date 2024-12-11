import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Bigtable {

    public final String projectId = "metal-contact-436518-d1"; 
    public final String instanceId = "my-bigtable-instance-c1"; 
    public final String COLUMN_FAMILY = "sensor";
    public final String tableId = "weather";
    public BigtableDataClient dataClient;
    public BigtableTableAdminClient adminClient;

    public static void main(String[] args) throws Exception {
        Bigtable testbt = new Bigtable();
        testbt.run();
    }

    public void connect() throws IOException {
        BigtableDataSettings dataSettings = BigtableDataSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .build();
        dataClient = BigtableDataClient.create(dataSettings);
        
        BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .build();
        adminClient = BigtableTableAdminClient.create(adminSettings);
        
        System.out.println("Connected to Bigtable successfully.");
    }

    public void run() throws Exception {
        connect();
        deleteTable();
        createTable();
        loadData();

        int temp = query1();
        System.out.println("Temperature at Vancouver on 2022-10-01 at 10 a.m.: " + temp);

        int windSpeed = query2();
        System.out.println("Highest wind speed in Portland in September 2022: " + windSpeed);

        ArrayList<Object[]> data = query3();
        System.out.println("Readings for SeaTac on October 2, 2022:");
        for (Object[] rowData : data) {
            for (Object field : rowData) {
                System.out.print(field + " ");
            }
            System.out.println();
        }

        int maxTemp = query4();
        System.out.println("Highest temperature in summer 2022: " + maxTemp);

        int hottestTemp = query5();
        System.out.println("Hottest day temperature: " + hottestTemp);

        close();
    }

    public void createTable() {
        try {
            CreateTableRequest request = CreateTableRequest.of(tableId)
                    .addFamily(COLUMN_FAMILY);
            adminClient.createTable(request);
            System.out.println("Table " + tableId + " created successfully.");
        } catch (Exception e) {
            System.err.println("Failed to create table: " + e.getMessage());
        }
    }

    public void loadData() throws Exception {
        loadCSVData("portland.csv", "PDX");
        loadCSVData("vancouver.csv", "YVR");
    }

    private void loadCSVData(String filePath, String stationId) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            BulkMutation bulkMutation = BulkMutation.create(tableId);
            
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(","); 
                String rowKey = stationId + "#" + fields[0] + "#" + fields[1]; 
                bulkMutation.add(rowKey, 
                    Mutation.create()
                            .setCell(COLUMN_FAMILY, "temperature", fields[2])
                            .setCell(COLUMN_FAMILY, "humidity", fields[3])
                            .setCell(COLUMN_FAMILY, "windspeed", fields[4]));
            }
            dataClient.bulkMutateRows(bulkMutation);
            System.out.println("Data loaded for " + stationId);
        }
    }

    public int query1() throws Exception {
        String rowKey = "YVR#2022-10-01#10";
        Row row = dataClient.readRow(tableId, rowKey);
        
        for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
            return Integer.parseInt(cell.getValue().toStringUtf8());
        }
        return 0;
    }

    public int query2() throws Exception {
        int maxWindSpeed = 0;
        ServerStream<Row> rows = dataClient.readRows(Query.create(tableId).range("PDX#2022-09", "PDX#2022-10"));

        for (Row row : rows) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "windspeed")) {
                int speed = Integer.parseInt(cell.getValue().toStringUtf8());
                if (speed > maxWindSpeed) {
                    maxWindSpeed = speed;
                }
            }
        }
        return maxWindSpeed;
    }

    public ArrayList<Object[]> query3() throws Exception {
        ArrayList<Object[]> data = new ArrayList<>();
        ServerStream<Row> rows = dataClient.readRows(Query.create(tableId).range("SEA#2022-10-02", "SEA#2022-10-03"));

        for (Row row : rows) {
            Object[] rowData = new Object[7];
            String[] rowKeyParts = row.getKey().toStringUtf8().split("#");
            rowData[0] = rowKeyParts[1];
            rowData[1] = rowKeyParts[2];

            for (RowCell cell : row.getCells()) {
                String column = cell.getQualifier().toStringUtf8();
                String value = cell.getValue().toStringUtf8();
                switch (column) {
                    case "temperature": rowData[2] = Integer.parseInt(value); break;
                    case "dewpoint": rowData[3] = Integer.parseInt(value); break;
                    case "humidity": rowData[4] = value; break;
                    case "windspeed": rowData[5] = value; break;
                    case "pressure": rowData[6] = value; break;
                }
            }
            data.add(rowData);
        }
        return data;
    }

    public int query4() throws Exception {
        int maxTemp = Integer.MIN_VALUE;
        ServerStream<Row> rows = dataClient.readRows(Query.create(tableId).range("SEA#2022-07", "SEA#2022-09"));

        for (Row row : rows) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                int temp = Integer.parseInt(cell.getValue().toStringUtf8());
                if (temp > maxTemp) {
                    maxTemp = temp;
                }
            }
        }
        return maxTemp;
    }

    public int query5() throws Exception {
        int maxTemp = Integer.MIN_VALUE;
        ServerStream<Row> rows = dataClient.readRows(Query.create(tableId).range("PDX#2022-01", "PDX#2022-12"));

        for (Row row : rows) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                int temp = Integer.parseInt(cell.getValue().toStringUtf8());
                if (temp > maxTemp) {
                    maxTemp = temp;
                }
            }
        }
        return maxTemp;
    }

    public void deleteTable() {
        try {
            adminClient.deleteTable(tableId);
            System.out.println("Table " + tableId + " deleted successfully.");
        } catch (NotFoundException e) {
            System.err.println("Table not found: " + e.getMessage());
        }
    }

    public void close() {
        dataClient.close();
        adminClient.close();
    }
}
