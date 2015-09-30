package parquet.column.statistics;

import parquet.column.ColumnDescriptor;
import parquet.column.statistics.bloomfilter.BloomFilterOpts;

import java.util.HashMap;
import java.util.Map;

/**
 * File level statistics options
 */
public class StatisticsOpts {

  Map<ColumnDescriptor, ColumnStatisticsOpts> statisticsOptsMap = new HashMap<>();

  // Used for test
  public StatisticsOpts() {

  }

  public StatisticsOpts(BloomFilterOpts bloomFilterOpts) {
    if (bloomFilterOpts != null) {
      Map<ColumnDescriptor, BloomFilterOpts.BloomFilterEntry> bloomFilterEntryMap =
          bloomFilterOpts.getFilterEntryList();
      for (ColumnDescriptor c : bloomFilterEntryMap.keySet()) {
        statisticsOptsMap.put(c, new ColumnStatisticsOpts(bloomFilterEntryMap.get(c)));
      }
    }
  }

  public ColumnStatisticsOpts getStatistics(ColumnDescriptor colDescriptor) {
    return statisticsOptsMap.get(colDescriptor);
  }
}
