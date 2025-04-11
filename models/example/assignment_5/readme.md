# Assignment 5: Analysis and Takeaways

## Performance Comparison

### First Run vs Second Run Metrics
- First run of fct_assignment5_totals: [execution time] seconds, [data processed] MiB
- Second run of fct_assignment5_totals: [execution time] seconds, [data processed] MiB
- Data processing reduction: [percentage]%

- First run of fct_assignment5_lookup: [execution time] seconds, [data processed] MiB  
- Second run of fct_assignment5_lookup: [execution time] seconds, [data processed] MiB
- Data processing reduction: [percentage]%

### Efficiency Analysis
The incremental models significantly reduced the amount of data processed in the second run because:
- For insert_overwrite: Only the partitions that needed updating were processed
- For merge strategy: Only new or modified records were processed

## Data Accuracy Analysis

### Why total_views Changed in the Lookup Model
The total_views in the lookup model changed between runs because:
[Your analysis after observing the actual results]

### Potential Pitfalls with Aggregations in Incremental Models
1. Double-counting: When using merge strategy with aggregations, you may count the same data multiple times if the incremental logic is not properly implemented.
2. Incomplete data: If the incremental filter excludes relevant data, aggregations can be inaccurate.
3. [Additional pitfalls you observe]

### Production Environment Solutions
1. Use insert_overwrite with partitioning for aggregate tables when possible
2. Implement proper tests to verify aggregation accuracy
3. Consider using dbt snapshots for historical tracking
4. [Your additional recommendations]

## Strategy Selection

### When to Use Insert_Overwrite vs. Merge
- **Insert_Overwrite**: 
  - Best for time-series data that is naturally partitioned
  - When entire partitions need to be refreshed
  - When data is rarely updated after initial load

- **Merge**:
  - Best for slowly changing dimensions
  - When only a small subset of records need updating
  - When you need to track historical changes

### Performance vs. Accuracy Trade-offs
[Your analysis after observing the results]

### BigQuery Cost Optimization Recommendations
1. Partition large tables to minimize data scanned
2. Use clustering for frequently filtered columns
3. Implement appropriate incremental filters
4. [Your additional recommendations]