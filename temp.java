// Step 3: Convert the reference table to a Map (Fee_ID -> TableRow)
        PCollectionView<Map<String, TableRow>> referenceMapView = referenceTable.apply("Convert Reference Table to Map",
                ParDo.of(new ConvertToKV()).apply(View.asMap()));

// Step 4: Use the reference map to update/enrich the rows in Table 2
PCollection<TableRow> enrichedTable2 = table2.apply("Enrich Table 2",
       ParDo.of(new EnrichWithReferenceData(referenceMapView)).withSideInputs(referenceMapView));

// Static class for converting TableRow to KV pairs
    static class ConvertToKV extends DoFn<TableRow, KV<String, TableRow>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            String feeId = (String) row.get("Fee_ID");
            c.output(KV.of(feeId, row));
        }
    }

    // Static class for enriching Table 2 rows with reference data
    static class EnrichWithReferenceData extends DoFn<TableRow, TableRow> {
        private final PCollectionView<Map<String, TableRow>> referenceMapView;

        public EnrichWithReferenceData(PCollectionView<Map<String, TableRow>> referenceMapView) {
            this.referenceMapView = referenceMapView;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow table2Row = c.element();
            String feeId = (String) table2Row.get("Fee_ID");

            // Access the reference map
            Map<String, TableRow> referenceMap = c.sideInput(referenceMapView);

            // Find the corresponding reference row
            TableRow refRow = referenceMap.get(feeId);
            if (refRow != null) {
                table2Row.set("Fee_Category_1", refRow.get("Fee_category_1"));
                table2Row.set("Fee_Category_2", refRow.get("Fee_category_2"));
                table2Row.set("Fee_Category_3", refRow.get("Fee_category_3"));
            }

            c.output(table2Row);
        }
    }


 // Print the modified PCollection
        modifiedRows.apply("Print Output", ParDo.of(new DoFn<TableRow, Void>() {
            @ProcessElement
            public void processElement(@DoFn.Element TableRow row) throws IOException {
                System.out.println(row.toPrettyString());
            }
        }));





// Define the DoFn as a separate class
public class FormatTableRowFn extends DoFn<TableRow, KV<Void, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row = c.element();
        assert row != null;

        String value1 = (row.get("relationship_manager_id") == null) ? "NULL" : "'" + row.get("relationship_manager_id").toString() + "'";
        String value2 = (row.get("name") == null) ? "NULL" : "'" + row.get("name").toString() + "'";
        String value3 = (row.get("team") == null) ? "NULL" : "'" + row.get("team").toString() + "'";

        c.output(KV.of(null, String.format("(%s,%s, %s)", value1, value2, value3)));
    }
}
// Apply the ParDo in the pipeline
PCollection<KV<Void, String>> valueStrings = sourceData.apply(ParDo.of(new FormatTableRowFn()));




// Write to Cloud SQL
sqlQueries.apply("WriteToCloudSQL", ParDo.of(new WriteToCloudSQLFn()));
// DoFn for writing to Cloud SQL using HikariCP
    public static class WriteToCloudSQLFn extends DoFn<String, Void> {

        private HikariDataSource dataSource;

        @Setup
        public void setup() {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl("jdbc:mysql://your-cloud-sql-instance-url:3306/your_database");
            config.setUsername("your-username");
            config.setPassword("your-password");
            dataSource = new HikariDataSource(config);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String sqlQuery = c.element();

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(sqlQuery)) {
                pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Teardown
        public void teardown() {
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }
