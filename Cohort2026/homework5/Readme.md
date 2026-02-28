## Question 1. Bruin Pipeline Structure
In a Bruin project, what are the required files/directories?

- bruin.yml and assets/
- .bruin.yml and pipeline.yml (assets can be anywhere)
- ✅ ** .bruin.yml and pipeline/ with pipeline.yml and assets/ --> answer **
- pipeline.yml and assets/ only

## Question 2. Materialization Strategies
You're building a pipeline that processes NYC taxi data organized by month based on pickup_datetime. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

- append - always add new rows
- replace - truncate and rebuild entirely
- ✅ ** time_interval - incremental based on a time column **
- view - create a virtual table only

## Question 3. Pipeline Variables

You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?
- bruin run --taxi-types yellow
- bruin run --var taxi_types=yellow
- ✅ bruin run --var 'taxi_types=["yellow"]'
- bruin run --set taxi_types=["yellow"]

Answer
```
bruin run --var 'taxi_types=["yellow"]'
```

Explanation

The variable taxi_types is defined as an array.
Therefore, when overriding it from the CLI, the value must be passed as a JSON-formatted array.

## Question 4. Running with Dependencies

You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets.

- bruin run ingestion.trips --all
- ✅ bruin run ingestion/trips.py --downstream
- bruin run pipeline/trips.py --recursive
- bruin run --select ingestion.trips+

### Answer
`bruin run ingestion/trips.py --downstream`

### Explanation
Bruin executes assets using file paths rather than selectors.

The `--downstream` flag runs the selected asset together with all downstream dependent assets.

## Question 5. Quality Checks

You want to ensure the `pickup_datetime` column in your trips table never has NULL values.

- name: unique
- ✅ name: not_null
- name: positive
- name: accepted_values, value: [not_null]

### Answer
`name: not_null`

### Explanation
The `not_null` quality check ensures that a column does not contain NULL values.  
`accepted_values` is used to validate allowed data values, not NULL constraints.

## Question 6. Lineage and Dependencies

After building your pipeline, you want to visualize the dependency graph between assets.

- bruin graph
- bruin dependencies
- ✅ bruin lineage
- bruin show

### Answer
`bruin lineage`

### Explanation
The `bruin lineage` command visualizes the dependency graph between assets, showing upstream and downstream relationships within the pipeline.

## Question 7. First-Time Run

You're running a Bruin pipeline for the first time on a new DuckDB database.  
What flag should you use to ensure tables are created from scratch?

- --create
- --init
- ✅ --full-refresh
- --truncate

### Answer
`--full-refresh`

### Explanation
The `--full-refresh` flag forces Bruin to rebuild tables from scratch, ignoring incremental state. This is required when running a pipeline for the first time on a new database.