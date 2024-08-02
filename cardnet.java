package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
//import org.example.curation.MyData;

import java.util.List;
import java.util.Objects;

public class cardnet {
    public static void main(String[] args) {
        // Create and initialize pipeline options
        String[] arguments = {"--runner=DirectRunner", "--tempLocation=gs://cardnet_2", "--project=burner-mihparma"};
        PipelineOptions options = PipelineOptionsFactory.fromArgs(arguments).withValidation().as(PipelineOptions.class);

        options.setTempLocation("gs://cardnet_1");
        Pipeline pipeline = Pipeline.create(options);
        // Define the BigQuery source and sink tables
        String sourceTable = "burner-mihparma:cardnet_c05.cardnet_account_attributes";
        String sinkTable = "burner-mihparma:cardnet_c05_2.TillYesterday_OutputTable";
        String Outputtable = "burner-mihparma:cardnet_c05.Output_1";

        //Reading previous data
        String Q1 = "SELECT Internal_MID,Parent_Internal_MID,_Merchant_Internal_MID,Account_Type," +
                " current_standard_mid_type AS previous_standard_mid_type" +
                " FROM" +
                " `burner-mihparma.cardnet_c05.previous_data`";
        // Reading today's data (New data)
        String Q2 = "SELECT *," +
                " NULL as `previous_standard_mid_type` " +
                " from" +
                " `burner-mihparma.cardnet_c05.cardnet_account_attributes`";
        // Union Q1, Q2. In case of duplicate internal_MID, Keep the data of New data, and give
        // it's previous_standard_mid_type from past table's row's current_standard_mid_type.

        String Q3 = "SELECT" +
                " COALESCE(t1.Internal_MID, t2.Internal_MID) AS Internal_MID," +
                " COALESCE(t1.Parent_Internal_MID,t2.Parent_Internal_MID) AS Parent_Internal_MID," +
                " COALESCE(t1._Merchant_Internal_MID,t2._Merchant_Internal_MID) AS _Merchant_Internal_MID," +
                " COALESCE(t1.Account_Type,t2.Account_Type) AS Account_Type," +
                " t2.previous_standard_mid_type AS previous_standard_mid_type" +
                " FROM" +
                " (" +
                Q2 +
                " )" +
                " t1" +
                " FULL OUTER JOIN" +
                " (" +
                Q1 +
                " )" +
                " t2" +
                " ON" +
                " t1.Internal_MID = t2.Internal_MID";
        // Simple labelling upto Chain_L1
        String Final_Query = "SELECT" +
                " *," +
                " CASE" +
                " WHEN Account_Type = 'INTL' THEN 'Internal'" +
                " WHEN Account_Type = 'AGNY' THEN 'Agency'" +
                " WHEN Account_Type = 'SING' THEN 'Outlet'" +
                " WHEN Account_Type = 'OUT' THEN 'Outlet'" +
                " WHEN Internal_MID = _Merchant_Internal_MID THEN 'Merchant'" +
                " WHEN Internal_MID IN (SELECT Parent_Internal_MID FROM " +
                " (" +
                Q3 +
                " )" +
                " WHERE Account_Type in ('OUT','SING')) THEN 'Chain_L1'\n" +
                " ELSE NULL" +
                " END AS Current_Standard_MID_Type," +
                " FROM" +
                " (" +
                Q3 +
                " )";

        System.out.println(Final_Query);

//STEP - 1: Read yesterday's table with current_standard_mid_type as previous_standard_mid_type
// column, union with today's table with condition that,
//# If any internal_id from yesterday's table is same as internal_id from today's table,
// keep today's table's row, assign matched row's current_standard_mid_type as previous_standard_mid_type
// remove yesterday's table's row.
//        Then Simple labelling till Chain_L1
        PCollection<TableRow> rows = pipeline.apply
                ("Read data from big query and apply simple labels", BigQueryIO.readTableRows()
                        .fromQuery(Final_Query)
                        .usingStandardSql());
        System.out.println("*******");

        for (int i = 0; i < 5; i++) {
            System.out.println("start");
            //Preparing Side Input
            PCollectionView<List<TableRow>> sideInputView = rows.apply("Create SideInput", View.asList());

            rows = rows.apply("Run " + (i + 1), ParDo.of(new ChangeNullToChainL(sideInputView)).withSideInputs(sideInputView));
            System.out.println(i);
        }

        //STEP 3 : Write output table:
        //Write the transformed data to the sink BigQuery table
        rows.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                .to(Outputtable)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        System.out.println("*******1242");
        // Run the pipeline
        pipeline.run();
    }

    static class ChangeNullToChainL extends DoFn<TableRow, TableRow> {
        private final PCollectionView<List<TableRow>> sideInputView;

        public ChangeNullToChainL(PCollectionView<List<TableRow>> sideInputView) {
            this.sideInputView = sideInputView;
        }
        @ProcessElement
        public void processElement(ProcessContext context) {
            TableRow row = context.element();
            String currentStandardMidType = (String) row.get("Current_Standard_MID_Type");
            if (currentStandardMidType == null) {
                String internalMid = (String) row.get("Internal_MID");
                System.out.println(internalMid);
                System.out.println("================");
                List<TableRow> sideInput = context.sideInput(sideInputView);
                for (TableRow otherRow : sideInput) {
                    String Current_Standard_MID_Type = (String) otherRow.get("Current_Standard_MID_Type");
                    if (Current_Standard_MID_Type != null && Current_Standard_MID_Type.contains("Chain_L")) {
                        // Perform the desired action if "Chain_L" is found
                        String parentInternalMid = (String) otherRow.get("Parent_Internal_MID");
                        if (internalMid.equals(parentInternalMid)) {
                            int number = Integer.parseInt(Current_Standard_MID_Type.substring(7)) + 1;
                            String ChainLevel = "Chain_L" + number;
                            System.out.println(ChainLevel);
//                            New table Row to maintain immutability
                            TableRow newRow = new TableRow();
                            newRow.putAll(row);
                            newRow.set("Current_Standard_MID_Type", ChainLevel);
                            context.output(newRow);
                            return;
                        }
                    }
                }
            }
            context.output(row);
        }
    }
}



//===============================================================================================
//// Naga's approach
////        Convert TableRow to MyClass
//        PCollection<MyData> filteredRows = rows.apply(
//                "TableRows to MyData",
//                MapElements.into(TypeDescriptor.of(MyData.class)).via(MyData::fromTableRow)
//        );
//
//        for (int i = 0; i < 5; i++) {
//            PCollection<MyData> nullTypeRows = filteredRows.apply("Filter Null Standard_MID_type",
//                    Filter.by(row -> row.getCurrent_Standard_MID_Type() == null));
//
//            PCollection<MyData> updatedRows = nullTypeRows.apply("UpdateRows", ParDo.of(new DoFn<MyData, MyData>()
//                    {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) {
//                        MyData row = c.element();
//                        String internalMid = row.getInternal_MID();
//                        for (MyData otherRow : filteredRows) {
//                            String parentInternalMid = (String) otherRow.get("Parent_Internal_MID");
//                            String currentType = (String) otherRow.get("Current_Standard_MID_Type");
//                            if (currentType != null && currentType.contains("Chain_L") && internalMid.equals(parentInternalMid)) {
//                                // If match found, update currentType to "Chain_L" + (num + 1)
//                                String updatedType = "ABC" ;//getNextChainLType(currentType);
//                                row.set("current_standard_mid_type", updatedType);
//                                // Emit the updated row
//                                c.output(row);
//                                break;
//                            }
//                        }
//                        // Step 4: Update filteredRows with the updated rows
//                        filteredRows = PCollectionList.of(filteredRows).and(updatedRows).apply("Flatten", Flatten.pCollections());
//        }

//============================================================================================












//        // Read data from the source BigQuery table as it is. (WORKING FINE)
//        PCollection data = pipeline
//                .apply("ReadFromBigQuery", BigQueryIO.readTableRows().from(sourceTable).withoutValidation());


//        // Read data from BigQuery using SQL query - Try 1
//        PCollection<TableRow> data = pipeline
//                .apply(BigQueryIO.read().fromQuery("SELECT * from burner-mihparma.cardnet_dataset.table1 where salary>55000").usingStandardSql());

// ==================================================================================================
//        //STEP - 1: Simple Labelling Read data from BigQuery using SQL query
//        PCollection<TableRow> data = pipeline.apply
//                ("Read data from big query and apply simple labels",BigQueryIO.readTableRows()
//                        .fromQuery("SELECT *,"
//                                + "CASE"
//                                + " WHEN Account_Type = 'INTL' THEN 'Internal' "
//                                + " WHEN Account_Type = 'AGNY' THEN 'Agency' "
//                                + " WHEN Account_Type = 'SING' THEN 'Outlet' "
//                                + " WHEN Account_Type = 'OUT' THEN 'Outlet' "
//                                + " WHEN Internal_MID = _Merchant_Internal_MID THEN 'Merchant' "
//                                + " WHEN Internal_MID IN (SELECT Parent_Internal_MID FROM burner-mihparma.cardnet_c05.cardnet_account_attributes WHERE Account_Type in ('OUT','SING')) THEN 'Chain_L1' "
//                                + " ELSE NULL "
//                                + " END AS Standard_MID_Type "
//                                + " FROM burner-mihparma.cardnet_c05.cardnet_account_attributes")
//                        .usingStandardSql());
//        System.out.println("*******");
//        System.out.println(data);
////              Write the transformed data to the sink BigQuery table
//                data.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
//                        .to(sinkTable)
//                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
//                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
//        System.out.println("*******1242");
//        // Run the pipeline

//=======================================================================
//STEP - 2: Recursive logic for chains and updation.
//        step:i - take out rows where current_standard_mid_type is still null
//        step:ii = if internal_mid is any other row's parent internal mid whose current_standard_MID_Type
//        contains "Chain_L" keyword, then that row's current_standard_MID_type = Chain_L(num+1)
//        Else null
//        Loop this process for 5 times.

// Filter rows where Current_Standard_MID_type is null
//        PCollection<TableRow> nullTypeRows = data
//                .apply("Filter Null Standard_MID_type",
//                        Filter.by(row -> row.get("Current_Standard_MID_Type") == null));
////
////        // Filter rows where Current_Standard_MID_type is not null and contains - Chain_L
//        PCollection<TableRow> filteredRows = data
//                .apply("FilterByStandardMIDType", Filter.by((TableRow row) -> {
//                    String standardMidType = (String) row.get("Current_Standard_MID_Type");
//                    return standardMidType != null && standardMidType.contains("Chain_L");
//                }));
