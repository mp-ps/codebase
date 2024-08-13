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