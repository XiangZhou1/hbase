/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is used for the tracking and enforcement of columns and numbers
 * of versions during the course of a Get or Scan operation, when explicit
 * column qualifiers have been asked for in the query.
 *
 * With a little magic (see {@link ScanQueryMatcher}), we can use this matcher
 * for both scans and gets.  The main difference is 'next' and 'done' collapse
 * for the scan case (since we see all columns in order), and we only reset
 * between rows.
 *
 * <p>
 * This class is utilized by {@link ScanQueryMatcher} mainly through two methods:
 * <ul><li>{@link #checkColumn} is called when a Put satisfies all other
 * conditions of the query.
 * <ul><li>{@link #getNextRowOrNextColumn} is called whenever ScanQueryMatcher
 * believes that the current column should be skipped (by timestamp, filter etc.)
 * <p>
 * These two methods returns a
 * {@link org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode}
 * to define what action should be taken.
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded
 */
@InterfaceAudience.Private
public class ExplicitColumnTracker implements ColumnTracker {

  private final int maxVersions;
  private final int minVersions;

 /**
  * Contains the list of columns that the ExplicitColumnTracker is tracking.
  * Each ColumnCount instance also tracks how many versions of the requested
  * column have been returned.
  */
  private final ColumnCount[] columns;
  private int index;
  private ColumnCount column;
  /** Keeps track of the latest timestamp included for current column.
   * Used to eliminate duplicates. */
  private long latestTSOfCurrentColumn;
  private long oldestStamp;

  /**
   * Default constructor.
   * @param columns columns specified user in query
   * @param minVersions minimum number of versions to keep
   * @param maxVersions maximum versions to return per column
   * @param oldestUnexpiredTS the oldest timestamp we are interested in,
   *  based on TTL
   */
  public ExplicitColumnTracker(NavigableSet<byte[]> columns, int minVersions,
      int maxVersions, long oldestUnexpiredTS) {
    this.maxVersions = maxVersions;
    this.minVersions = minVersions;
    this.oldestStamp = oldestUnexpiredTS;
    this.columns = new ColumnCount[columns.size()];
    int i=0;
    for(byte [] column : columns) {
      this.columns[i++] = new ColumnCount(column);
    }
    reset();
  }

    /**
   * Done when there are no more columns to match against.
   */
  public boolean done() {
    return this.index >= columns.length;
  }

  public ColumnCount getColumnHint() {
    return this.column;
  }

  /**
   * {@inheritDoc}
   *
   * 当 ScanQueryMatcher 拿到一个 KeyValue (单元格) 时，它首先会调用 checkColumn 来判断这个 KeyValue 的列是否是当前需要寻找的列。
   * checkColumn 的内部逻辑是一个 do-while(true) 循环，非常精巧：
   * ● 检查清单是否完成: 如果 index 已经超出了 columns 数组的范围 (done())，说明清单上所有的商品都已找完。直接返回 SEEK_NEXT_ROW，告诉 ScanQueryMatcher：“这一行没东西可找了，直接跳到下一行吧。”
   * ● 比较当前商品: 将传入的 KeyValue 的列限定符与 this.column（当前正在寻找的目标列）进行字节比较。
   * ● 比较结果有三种情况:
   *   ○ 等于 0 (匹配成功):
   *     ■ 太棒了！找到了清单上正在寻找的商品。
   *     ■ 返回 MatchCode.INCLUDE。这个返回值只是一个临时许可，意思是“列是对的，但数量（版本）够不够，我还没看”。ScanQueryMatcher 收到后，会紧接着调用 checkVersions 方法来做最终判断。
   *   ○ 大于 0 (当前商品“太小了”):
   *     ■ 这意味着 KeyValue 的列在字典序上小于我们正在寻找的 this.column。例如，我们正在找 "name"，但传进来的是 "age"。
   *     ■ 这说明我们需要跳过当前这个 KeyValue，继续寻找 "name"。
   *     ■ 返回 MatchCode.SEEK_NEXT_COL。这个 MatchCode 是一个强烈的指令，告诉上层扫描器（StoreScanner）：
   *              “你不用再一个一个地给我 KeyValue 了，请直接用 HFile 的索引**跳跃（seek）**到我们目标列 this.column 的位置。” 这是一个关键的性能优化。
   *   ○ 小于 0 (当前商品“太大了”):
   *     ■ 这意味着 KeyValue 的列在字典序上大于我们正在寻找的 this.column。例如，我们正在找 "age"，但传进来的是 "name"。
   *     ■ 因为 KeyValue 是有序的，这说明我们永远也找不到 "age" 了（已经错过了）。
   *     ■ 采购员的动作: 在清单上划掉 "age" 这一项，将指针 index 加 1，更新 this.column 为清单上的下一个商品（比如 "phone"）。
   *     ■ 然后，continue 循环，用这个新的目标列 "phone" 与刚刚传进来的 "name" 再次进行比较。这个递归式的比较会一直进行，直到找到匹配的列，或者发现当前 KeyValue 比所有待找的列都小。
   */
  @Override
  public ScanQueryMatcher.MatchCode checkColumn(byte [] bytes, int offset,
      int length, byte type) {
    // delete markers should never be passed to an
    // *Explicit*ColumnTracker
    assert !KeyValue.isDelete(type);
    do {
      // No more columns left, we are done with this query
      if(done()) {
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
      }

      // No more columns to match against, done with storefile
      if(this.column == null) {
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
      }

      // Compare specific column to current column
      int ret = Bytes.compareTo(column.getBuffer(), column.getOffset(),
          column.getLength(), bytes, offset, length);

      // Column Matches. Return include code. The caller would call checkVersions
      // to limit the number of versions.
      if(ret == 0) {
        return ScanQueryMatcher.MatchCode.INCLUDE;
      }

      resetTS();

      if (ret > 0) {
        // The current KV is smaller than the column the ExplicitColumnTracker
        // is interested in, so seek to that column of interest.
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_COL;
      }

      // The current KV is bigger than the column the ExplicitColumnTracker
      // is interested in. That means there is no more data for the column
      // of interest. Advance the ExplicitColumnTracker state to next
      // column of interest, and check again.
      if (ret <= -1) {
        ++this.index;
        if (done()) {
          // No more to match, do not include, done with this row.
          return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
        }
        // This is the recursive case.
        this.column = this.columns[this.index];
      }
    } while(true);
  }

    /**
     * ● 去重: 首先检查传入的 KeyValue 的时间戳是否与上一个被接受的同列 KeyValue 的时间戳相同。如果是，说明是重复数据，直接返回 SKIP。
     * ● 计数与版本检查:
     *   ○ 将当前列的 count 加 1。
     *   ○ 检查 count 是否已经达到了 maxVersions 的上限。
     *   ○ 如果达到了上限：
     *     ■ 这意味着这个商品（列）我们已经拿够了。
     *     ■ 在清单上划掉这一项（index 加 1），并更新 this.column 指向下一个目标列。
     *     ■ 返回 MatchCode.INCLUDE_AND_SEEK_NEXT_COL。这个复合指令的意思是：“收下当前这个 KeyValue，但这个商品我已经拿够了，请直接跳到我清单上的下一个商品。”
     *     ■ 如果划掉后，清单上所有商品都已找完 (done())，则返回 INCLUDE_AND_SEEK_NEXT_ROW，意思是：“收下当前这个 KeyValue，并且我的任务都完成了，请直接跳到下一行。”
     * ● 未达到上限:
     *   ○ 如果版本数还没满，就记录下当前 KeyValue 的时间戳（用于下次去重），并返回 MatchCode.INCLUDE。
     * @throws IOException
     */
  @Override
  public ScanQueryMatcher.MatchCode checkVersions(byte[] bytes, int offset, int length,
      long timestamp, byte type, boolean ignoreCount) throws IOException {
    assert !KeyValue.isDelete(type);
    if (ignoreCount) return ScanQueryMatcher.MatchCode.INCLUDE;
    // Check if it is a duplicate timestamp
    if (sameAsPreviousTS(timestamp)) {
      // If duplicate, skip this Key
      return ScanQueryMatcher.MatchCode.SKIP;
    }
    int count = this.column.increment();
    if (count >= maxVersions || (count >= minVersions && isExpired(timestamp))) {
      // Done with versions for this column
      ++this.index;
      resetTS();
      if (done()) {
        // We have served all the requested columns.
        this.column = null;
        return ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW;
      }
      // We are done with current column; advance to next column
      // of interest.
      this.column = this.columns[this.index];
      return ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL;
    }
    setTS(timestamp);
    return ScanQueryMatcher.MatchCode.INCLUDE;
  }

  // Called between every row.
  public void reset() {
    this.index = 0;
    this.column = this.columns[this.index];
    for(ColumnCount col : this.columns) {
      col.setCount(0);
    }
    resetTS();
  }

  private void resetTS() {
    latestTSOfCurrentColumn = HConstants.LATEST_TIMESTAMP;
  }

  private void setTS(long timestamp) {
    latestTSOfCurrentColumn = timestamp;
  }

  private boolean sameAsPreviousTS(long timestamp) {
    return timestamp == latestTSOfCurrentColumn;
  }

  private boolean isExpired(long timestamp) {
    return timestamp < oldestStamp;
  }

  /**
   * This method is used to inform the column tracker that we are done with
   * this column. We may get this information from external filters or
   * timestamp range and we then need to indicate this information to
   * tracker. It is required only in case of ExplicitColumnTracker.
   * @param bytes
   * @param offset
   * @param length
   */
  public void doneWithColumn(byte [] bytes, int offset, int length) {
    while (this.column != null) {
      int compare = Bytes.compareTo(column.getBuffer(), column.getOffset(),
          column.getLength(), bytes, offset, length);
      resetTS();
      if (compare <= 0) {
        ++this.index;
        if (done()) {
          // Will not hit any more columns in this storefile
          this.column = null;
        } else {
          this.column = this.columns[this.index];
        }
        if (compare <= -1)
          continue;
      }
      return;
    }
  }

  public MatchCode getNextRowOrNextColumn(byte[] bytes, int offset,
      int qualLength) {
    doneWithColumn(bytes, offset,qualLength);

    if (getColumnHint() == null) {
      return MatchCode.SEEK_NEXT_ROW;
    } else {
      return MatchCode.SEEK_NEXT_COL;
    }
  }

  public boolean isDone(long timestamp) {
    return minVersions <= 0 && isExpired(timestamp);
  }
}
