package org.example;

import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import com.google.api.services.bigquery.model.TableRow;
//import org.example.curation.MyData;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.transforms.DoFn;


public class Curation_c3 {
    public static void main(String[] args) {
        // Create and initialize pipeline options
        String[] arguments = {"--runner=DirectRunner", "--tempLocation=gs://cardnet_2", "--project=burner-mihparma"};
        PipelineOptions options = PipelineOptionsFactory.fromArgs(arguments).withValidation().as(PipelineOptions.class);

        options.setTempLocation("gs://cardnet_1");
        Pipeline pipeline = Pipeline.create(options);
        // Define the BigQuery source and sink tables
        String sourceTable = "burner-mihparma.Curation_c3.Cardnet_Outlet_Fees";
        String sinkTable = "burner-mihparma:Curation_c3.Output_1";

        String Final_Query = "SELECT * FROM " + sourceTable;
        //STEP 1: Read source from BQ
        PCollection<TableRow> rows = pipeline.apply
                ("Read data from big query and apply simple labels", BigQueryIO.readTableRows()
                        .fromQuery(Final_Query)
                        .usingStandardSql());
        System.out.println("*******");

        //STEP 2: Apply ParDo to process each element (This is my new output table)
        PCollection<TableRow> output = rows.apply("ProcessRows", ParDo.of(new ProcessRowFn()));

// ============================================================================================
        // Now Checking for Duplicates in staging table to handle update and append operations
        // Read Stage layer (Old table) table from BQ:
        PCollection<TableRow> stage_table = pipeline.apply
                ("Read data from big query and apply simple labels", BigQueryIO.readTableRows()
                        .fromQuery("SELECT * FROM burner-mihparma.Curation_c3.Output_1")
                        .usingStandardSql());

        PCollectionView<List<TableRow>> stage_view = stage_table.apply(View.asList());

        final TupleTag<TableRow> existingMIDS = new TupleTag<>(){};
        final TupleTag<TableRow> newMIDS = new TupleTag<>(){};

        PCollectionTuple results = output.apply("Compare PCollections", ParDo.of(new DoFn<TableRow, TableRow>() {

            @ProcessElement
            public void processElement(ProcessContext c) {
                List<TableRow> stage_rows = c.sideInput(stage_view);
                TableRow tableRow = checkIfAlreadyPresent(stage_rows, c.element());
                if(tableRow != null){
                    System.out.println("Matched ####");
                    c.output(existingMIDS,tableRow);
                }
                else {
                    System.out.println("##### Not matched ");
                    c.output(c.element());
                }
            }
            public TableRow checkIfAlreadyPresent(List<TableRow> accounts, TableRow row){
                if(accounts.stream().anyMatch(o -> (o.get("Internal_MID").equals(row.get("Internal_MID")) )))
                {
                    TableRow record = accounts.stream().filter(o -> (o.get("Internal_MID").equals(row.get("Internal_MID")))).findFirst().get();
                    TableRow tableRow = new TableRow();

                    tableRow.set("Internal_MID", record.get("Internal_MID"));
                    tableRow.set("Period", record.get("Period"));
                    tableRow.set("Fee_Id", record.get("Fee_Id"));
                    tableRow.set("Fee_Value", record.get("Fee_Value"));
                    tableRow.set("Meta_File_Name", record.get("Meta_File_Name"));
                    tableRow.set("Meta_Insert_Timestamp", record.get("Meta_Insert_Timestamp"));
                    tableRow.set("Meta_Validation_Errors", record.get("Meta_Validation_Errors"));
                    tableRow.set("Fee_Category_L1", record.get("Fee_Category_L1"));
                    tableRow.set("Fee_Category_L2", record.get("Fee_Category_L2"));
                    tableRow.set("Fee_Category_L3", record.get("Fee_Category_L3"));

                    return tableRow;
                }
                return null;
            }


        }).withSideInputs(stage_view).withOutputTags(newMIDS,TupleTagList.of(existingMIDS)));

        PCollection<TableRow> new_mids = results.get(newMIDS);
        new_mids.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                .to(sinkTable)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


        PCollection<TableRow> existing_mids = results.get(existingMIDS);
        existing_mids.apply("Update BQ", ParDo.of(new UpdateBQ()));

// ==========================================================================================
        //STEP 3 : Write output table:
        //Write the transformed data to the sink BigQuery table
//        output.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
//                .to(sinkTable)
//                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
//                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
//        System.out.println("1232****");
        // Run the pipeline
        pipeline.run();
    }

    // Define a DoFn to process each TableRow and set the output based on col1 and col2
    public static class ProcessRowFn extends DoFn<TableRow, TableRow> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
            String Fee_Id = (String) row.get("Fee_Id");
            System.out.println(Fee_Id);
            TableRow newRow = new TableRow();
            newRow.putAll(row);
            if (Fee_Id != null){
                if (Fee_Id.equals("8")){
                    System.out.println("Inside 8");
                    newRow.set("Fee_Category_L1", "Repeatable");
                    newRow.set("Fee_Category_L2", "Chargebacks");
                    newRow.set("Fee_Category_L3", "Fees_Chargebacks");
                } else if (Fee_Id.equals("369")) {
                    System.out.println("Inside 369");
                    newRow.set("Fee_Category_L1", "Non-Repeatable");
                    newRow.set("Fee_Category_L2", "Chargebacks");
                    newRow.set("Fee_Category_L3", "Fees_ChargebacksSuspense");
                }
                else if (Fee_Id.equals("28046") || Fee_Id.equals("28525") || Fee_Id.equals("29542")  || Fee_Id.equals("23379") || Fee_Id.equals("27917") || Fee_Id.equals("28279") ) {
                    System.out.println("Inside 28046 and others");
                    newRow.set("Fee_Category_L1", "Repeatable");
                    newRow.set("Fee_Category_L2", "Chargebacks");
                    newRow.set("Fee_Category_L3", "Fees_MerchChargeback");
                }
                else {
                    System.out.println("Inside no match found");
                    newRow.set("Fee_Category_L1", null);
                    newRow.set("Fee_Category_L2", null);
                    newRow.set("Fee_Category_L3", null);
                }
            }
            else{
                System.out.println("Inside Null");
                newRow.set("Fee_Category_L1", null);
                newRow.set("Fee_Category_L2", null);
                newRow.set("Fee_Category_L3", null);
            }
            out.output(newRow);
        }
    }

    public static class UpdateBQ extends DoFn<TableRow, Void> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {


            BigQuery bigQuery = BigQueryOptions.newBuilder().setProjectId("burner-mihparma").build().getService();

            String update_query = ("UPDATE Curation_c3.Output_1 SET " +
                    "Period=\"{Period}\", " +
                    "Fee_Id={Fee_Id}, " +
                    "Fee_Value={Fee_Value}, " +
                    "Meta_File_Name=\"{Meta_File_Name}\", " +
                    "Meta_Insert_Timestamp=\"{Meta_Insert_Timestamp}\", " +
                    "Meta_Validation_Errors=\"{Meta_Validation_Errors}\", " +
                    "Fee_Category_L1=\"{Fee_Category_L1}\", " +
                    "Fee_Category_L2=\"{Fee_Category_L2}\", " +
                    "Fee_Category_L3=\"{Fee_Category_L3}\", " +
                    "Internal_MID=\"{Internal_MID}\" " +
                    "WHERE Internal_MID=\"{Internal_MID}\"")
                    .replace("{Period}",c.element().get("Period") != null ? c.element().get("Period").toString() : "NULL")
                    .replace("{Fee_Id}", c.element().get("Fee_Id") != null ? c.element().get("Fee_Id").toString() : "NULL")
                    .replace("{Fee_Value}",c.element().get("Fee_Value") != null ? c.element().get("Fee_Value").toString() : "NULL")
                    .replace("{Meta_File_Name}", c.element().get("Meta_File_Name") != null ? c.element().get("Meta_File_Name").toString() : "NULL")
                    .replace("{Meta_Insert_Timestamp}",c.element().get("Meta_Insert_Timestamp") != null ? c.element().get("Meta_Insert_Timestamp").toString() : "NULL")
                    .replace("{Meta_Validation_Errors}", c.element().get("Meta_Validation_Errors") != null ? c.element().get("Meta_Validation_Errors").toString() : "NULL")
                    .replace("{Fee_Category_L1}", c.element().get("Fee_Category_L1") != null ? c.element().get("Fee_Category_L1").toString() : "NULL")
                    .replace("{Fee_Category_L2}", c.element().get("Fee_Category_L2") != null ? c.element().get("Fee_Category_L2").toString() : "NULL")
                    .replace("{Fee_Category_L3}", c.element().get("Fee_Category_L3") != null ? c.element().get("Fee_Category_L3").toString() : "NULL")
                    .replace("{Internal_MID}", c.element().get("Internal_MID") != null ? c.element().get("Internal_MID").toString() : "NULL");

            System.out.println(update_query);
            update_query = update_query.replace("\"NULL\"","NULL");
            System.out.println("============================");
            System.out.println(update_query);
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(update_query).build();

            Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).build());
            queryJob = queryJob.waitFor();

            if (queryJob.getStatus().getError() != null) {
                throw new Exception(queryJob.getStatus().getError().toString());

            }
        }
    }
}

