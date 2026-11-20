
# LateArrivingDataPuzzleAurora
PySpark SCD Type 2 pipeline with watermarks and late-arriving customers 

 It loads three waves of customer and order files into a Type 2 customer dimension and an orders fact table, handling incremental loads, duplicates, missing values, and late-arriving customers.

## What it does

| Rule | How it is handled |
|---|---|
| Incremental load | One watermark per source table: the highest `modified_at` read, including rejected rows. From wave 2 on, rows not newer than the watermark are ignored. |
| Duplicates | Exact duplicate rows are dropped, then only the latest `modified_at` is kept per key within a wave. |
| Missing values | Blank city or province becomes `Unknown`; blank loyalty tier becomes `Bronze`. Defaults are applied before comparing to the dimension. |
| SCD Type 2 | `city`, `province`, `loyalty_tier`. A change closes the current row (`effective_to` = incoming `modified_at`) and inserts a new current row. |
| SCD Type 1 | `first_name`, `last_name`, `email`. An incoming row overwrites these on every version of the customer (including with a blank); never creates a new version. |
| Unknown member | `customer_id = -1`. Orders with a blank customer go here. |
| Late-arriving customers | An order for an unseen customer creates an inferred member. When the real record arrives, that row is updated in place with no new version. |
| Rejected orders | Orders with a blank amount are counted and not loaded. |
| Order updates | `fact_orders` holds one row per `order_id`; a newer row overwrites. |
| Point-in-time keys | An order joins the customer version where `effective_from <= order_date 00:00:00 < effective_to`. The code asserts every order matches exactly one version, counted per `order_id` so a zero-match and a two-match order cannot cancel out. Q14 is counted through this join (orders on the unknown member). |

Waves are processed in order, and customers load before orders within each wave.

## Files

- `aurora_pipeline.py` is the full pipeline and prints the 16 contest answers.
- `blueprint.md` is the Fabric pipeline blueprint (tool choice, triggering, recovery, parameters).
- `verify_pandas.py` is an independent row-by-row check. It gives the same answers by a different method.

The six wave CSV files are not included. They are the contest starter files.

## Run locally

Requires Python 3.10+ and Java 17 or newer.

```bash
pip install pyspark pandas
# put wave1_customers.csv ... wave3_orders.csv in a folder, e.g. ./data
python aurora_pipeline.py ./data
```

## Run in Google Colab

```python
!apt-get update -qq
!apt-get install -y openjdk-17-jdk-headless -qq > /dev/null
!pip install pyspark pandas -q
```

Upload the script and the six CSVs, then:

```python
!python aurora_pipeline.py /content
```

## Run in Microsoft Fabric

1. Create a Lakehouse and upload the CSVs to `Files/aurora/`.
2. Open a notebook attached to the Lakehouse and paste `aurora_pipeline.py`.
3. Set `USE_DELTA = True` and `DATA_DIR = "Files/aurora"`.
4. Replace the `if __name__ == "__main__":` block with a plain `run()` call.

This writes the Delta tables `dim_customer`, `fact_orders` and `etl_watermark`. The Delta path has not been run in Fabric; the answers come from local and Colab runs.

## Re-runnable from a clean start

Every run rebuilds the dimension, fact table and watermarks from wave 1, so no state carries over between runs. In Fabric the three Delta tables are written one after another, with the watermark last, so a failed run is simply replayed (see `blueprint.md`). The notebook forces UTC so timestamps do not shift between Python and Spark.

## Output

The script prints the 16 contest answers at the end of a run. They are not listed here.

## Design notes

- Raw files are read as strings so blank cells are never coerced to values before the rules run.
- Each wave is applied set-based: inferred members, new customers, Type 1 overwrites and Type 2 closes plus inserts are computed as separate steps and unioned.
- A rejected order does not create an inferred customer, but its `modified_at` still advances the watermark.
