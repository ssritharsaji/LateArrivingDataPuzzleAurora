PIPELINE BLUEPRINT - Aurora Outfitters incremental load (Microsoft Fabric)

1. TOOL CHOICE
A Data Pipeline coordinates the run; a PySpark Notebook (Notebook activity) applies the rules. The notebook is needed because
the logic is row-level: latest row per key, a watermark per table, SCD Type 2 (close and insert), Type 1 overwrite on all
versions, and inferred members updated in place. Copy job (plain incremental copy) and Dataflow Gen2 (awkward for window
logic and re-runnable code) do not fit. Files land in Lakehouse Files/aurora/; outputs are Delta tables dim_customer,
fact_orders and etl_watermark (table_name, watermark, last_wave).

2. TRIGGERING
Storage-event trigger on file creation for wave*_customers.csv AND wave*_orders.csv. A Get Metadata step checks that BOTH
files for the wave exist. If only one has arrived (for example orders first), the run ends cleanly as "waiting": no load,
no watermark change. The later file fires the trigger again and the load proceeds. Customers always load before orders
inside the notebook.
Fallback: a daily schedule reads last_wave from etl_watermark and processes waves in order, one at a time (sequential
ForEach). It processes the earliest unprocessed wave that has both files, and stops at the first wave that is missing a
file, so waves are never skipped or run out of order.

3. FAILURE RECOVERY (design used by the submitted code: clean-start replay)
Delta has no transaction across tables, so dim_customer, fact_orders and etl_watermark are written one after another.
The watermark row is written LAST, so it marks a wave as done only after both data tables are written. If the dimension
write succeeds and the fact write fails, the watermark is unchanged and the wave counts as not done. The retry replays
waves 1..N from scratch and overwrites all three tables, so the result is the same as one clean run. Between the failure
and the retry, readers could see a dimension that is ahead of the fact table; to avoid that, write staging tables and
swap them in, or restore from the Delta versions recorded before the run (RESTORE TABLE). A single-wave resume that
applies only the new wave on top of the stored tables is a possible later optimisation; it would need these recorded
table versions to undo a partial write first.
Pipeline: Notebook activity retry 2 times, 60 s apart; an On fail path sends a Teams or email alert with the run id and wave.

4. PARAMETERS AND EXPRESSIONS
Pipeline parameters: WaveNumber (int), FolderPath (default Files/aurora).
Trigger file name: @pipeline()?.TriggerEvent?.FileName (Fabric storage-event trigger, e.g. "wave2_orders.csv").
  WaveNumber:     @int(substring(split(pipeline()?.TriggerEvent?.FileName,'_')[0], 4,
                       sub(length(split(pipeline()?.TriggerEvent?.FileName,'_')[0]), 4)))
  CustomersFile:  @concat('wave', string(pipeline().parameters.WaveNumber), '_customers.csv')
  OrdersFile:     @concat('wave', string(pipeline().parameters.WaveNumber), '_orders.csv')
  Full path:      @concat(pipeline().parameters.FolderPath, '/', variables('CustomersFile'))
Two watermarks, selected by source table with two Lookup activities (each with a query such as
SELECT watermark FROM etl_watermark WHERE table_name = 'customers', and the same for 'orders'):
  @activity('LookupCustomersWM').output.firstRow.watermark
  @activity('LookupOrdersWM').output.firstRow.watermark
Both, plus the wave number and file names, go to the Notebook activity as base parameters. The notebook keeps rows with
modified_at greater than that table's watermark, and the new watermark is the max modified_at of everything read, including
rejected rows. It is written to etl_watermark last.
