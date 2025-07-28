/**
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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Keeps track of the columns for a scan if they are not explicitly specified
 * ScanWildcardColumnTracker 是 ColumnTracker 接口的另一个实现，专门用于处理**“通配符”式的列查询。
 *      当你的扫描（Scan）请求没有指定具体的列，而是要获取整个列族**的所有数据时，ScanQueryMatcher 内部就会启用 ScanWildcardColumnTracker。
 *
 * 你可以把它想象成一个**“巡视员”**，他没有一个明确的“购物清单”，他的任务是巡视整个仓库（列族），并对看到的每一件商品（列）进行检查和计数，
 *     确保每种商品拿到的数量（版本）不超过规定，同时也要处理好过期和重复的问题。
 *
 *
 *
 * 与 ExplicitColumnTracker 的“目标导向”不同，ScanWildcardColumnTracker 是**“被动响应”和“状态记忆”**的。
 * ● 记录当前列 (columnBuffer, columnOffset, columnLength):
 *   ○ 它不像 ExplicitColumnTracker 那样有一个预设的列列表。相反，它只记住当前正在处理的列是什么。当遇到一个新列时，它会更新这些变量来记住这个新列。
 * ● 当前列的版本计数器 (currentCount):
 *   ○ 这个计数器只与当前正在处理的列相关。当切换到新列时，这个计数器会被重置为 0。
 * ● 版本控制 (maxVersions, minVersions):
 *   ○ maxVersions: 巡视员对任何一种商品最多能拿的数量。
 *   ○ minVersions: 巡视员对任何一种商品最少需要保留的数量（即使它已过期）。
 * ● 去重机制 (latestTSOfCurrentColumn, latestTypeOfCurrentColumn):
 *   ○ 为了防止因底层存储的某些原因导致返回重复的 KeyValue，它会记住上一个被接受的、属于当前列的 KeyValue 的时间戳和类型。
 */
@InterfaceAudience.Private
public class ScanWildcardColumnTracker implements ColumnTracker {
  private byte [] columnBuffer = null;
  private int columnOffset = 0;
  private int columnLength = 0;
  private int currentCount = 0;
  private int maxVersions;
  private int minVersions;
  /* Keeps track of the latest timestamp and type included for current column.
   * Used to eliminate duplicates. */
  private long latestTSOfCurrentColumn;
  private byte latestTypeOfCurrentColumn;

  private long oldestStamp;

  /**
   * Return maxVersions of every row.
   * @param minVersion Minimum number of versions to keep
   * @param maxVersion Maximum number of versions to return
   * @param oldestUnexpiredTS oldest timestamp that has not expired according
   *          to the TTL.
   */
  public ScanWildcardColumnTracker(int minVersion, int maxVersion,
      long oldestUnexpiredTS) {
    this.maxVersions = maxVersion;
    this.minVersions = minVersion;
    this.oldestStamp = oldestUnexpiredTS;
  }

  /**
   * {@inheritDoc}
   * This receives puts *and* deletes.
   */
  @Override
  public MatchCode checkColumn(byte[] bytes, int offset, int length, byte type)
      throws IOException {
    return MatchCode.INCLUDE;
  }

  /**
   * {@inheritDoc}
   * This receives puts *and* deletes. Deletes do not count as a version, but rather
   * take the version of the previous put (so eventually all but the last can be reclaimed).
   *   ○ 去重: 首先检查传入的 KeyValue 的时间戳和类型是否与 latestTSOfCurrentColumn 和 latestTypeOfCurrentColumn 完全相同。如果是，说明是重复数据，直接返回 SKIP。
   *   ○ 版本计数: 如果不是删除标记 (Delete Marker)，就将 currentCount 加 1。删除标记不计入版本数。
   *   ○ 版本上限检查: 检查 currentCount 是否已经超过了 maxVersions。
   *     ■ 如果超过了，说明这个列的版本我们已经拿够了，对于这个版本以及后续更旧的版本，我们都不再需要。
   *     ■ 返回 MatchCode.SEEK_NEXT_COL，这是一个强烈的指令，告诉上层扫描器：“这个列我完事了，请直接跳到下一个不同的列。”
   *   ○ TTL 与最小版本检查: 如果版本数未超限，会进一步检查：
   *     ■ currentCount 是否小于等于 minVersions？
   *     ■ 或者，这个 KeyValue 的时间戳是否比 oldestStamp (根据 TTL 计算出的最老有效时间戳) 更晚？
   *     ■ 只要满足上述任一条件，就意味着这个版本应该被保留。
   *     ■ 此时，更新 latestTS... 状态，并返回 MatchCode.INCLUDE。
   *   ○ 过期淘汰: 如果版本数超过了 minVersions，并且时间戳也已经过期，那么这个版本将被淘汰。返回 MatchCode.SEEK_NEXT_COL，跳过这个过期的版本以及所有比它更旧的版本。
   */
  @Override
  public ScanQueryMatcher.MatchCode checkVersions(byte[] bytes, int offset, int length,
      long timestamp, byte type, boolean ignoreCount) throws IOException {

    if (columnBuffer == null) {
      // first iteration.
      resetBuffer(bytes, offset, length);
      if (ignoreCount) return ScanQueryMatcher.MatchCode.INCLUDE;
      // do not count a delete marker as another version
      return checkVersion(type, timestamp);
    }
    int cmp = Bytes.compareTo(bytes, offset, length,
        columnBuffer, columnOffset, columnLength);
    if (cmp == 0) {
      if (ignoreCount) return ScanQueryMatcher.MatchCode.INCLUDE;

      //If column matches, check if it is a duplicate timestamp
      if (sameAsPreviousTSAndType(timestamp, type)) {
        return ScanQueryMatcher.MatchCode.SKIP;
      }
      return checkVersion(type, timestamp);
    }

    resetTSAndType();

    // new col > old col
    if (cmp > 0) {
      // switched columns, lets do something.x
      resetBuffer(bytes, offset, length);
      if (ignoreCount) return ScanQueryMatcher.MatchCode.INCLUDE;
      return checkVersion(type, timestamp);
    }

    // new col < oldcol
    // WARNING: This means that very likely an edit for some other family
    // was incorrectly stored into the store for this one. Throw an exception,
    // because this might lead to data corruption.
    throw new IOException(
        "ScanWildcardColumnTracker.checkColumn ran into a column actually " +
        "smaller than the previous column: " +
        Bytes.toStringBinary(bytes, offset, length));
  }

  private void resetBuffer(byte[] bytes, int offset, int length) {
    columnBuffer = bytes;
    columnOffset = offset;
    columnLength = length;
    currentCount = 0;
  }

  /**
   * Check whether this version should be retained.
   * There are 4 variables considered:
   * If this version is past max versions -> skip it
   * If this kv has expired or was deleted, check min versions
   * to decide whther to skip it or not.
   *
   * Increase the version counter unless this is a delete
   */
  private MatchCode checkVersion(byte type, long timestamp) {
    if (!KeyValue.isDelete(type)) {
      currentCount++;
    }
    if (currentCount > maxVersions) {
      return ScanQueryMatcher.MatchCode.SEEK_NEXT_COL; // skip to next col
    }
    // keep the KV if required by minversions or it is not expired, yet
    if (currentCount <= minVersions || !isExpired(timestamp)) {
      setTSAndType(timestamp, type);
      return ScanQueryMatcher.MatchCode.INCLUDE;
    } else {
      return MatchCode.SEEK_NEXT_COL;
    }

  }

  @Override
  public void reset() {
    columnBuffer = null;
    resetTSAndType();
  }

  private void resetTSAndType() {
    latestTSOfCurrentColumn = HConstants.LATEST_TIMESTAMP;
    latestTypeOfCurrentColumn = 0;
  }

  private void setTSAndType(long timestamp, byte type) {
    latestTSOfCurrentColumn = timestamp;
    latestTypeOfCurrentColumn = type;
  }

  private boolean sameAsPreviousTSAndType(long timestamp, byte type) {
    return timestamp == latestTSOfCurrentColumn && type == latestTypeOfCurrentColumn;
  }

  private boolean isExpired(long timestamp) {
    return timestamp < oldestStamp;
  }

  /**
   * Used by matcher and scan/get to get a hint of the next column
   * to seek to after checkColumn() returns SKIP.  Returns the next interesting
   * column we want, or NULL there is none (wildcard scanner).
   *
   * @return The column count.
   */
  public ColumnCount getColumnHint() {
    return null;
  }

  /**
   * We can never know a-priori if we are done, so always return false.
   * @return false
   */
  @Override
  public boolean done() {
    return false;
  }

  public MatchCode getNextRowOrNextColumn(byte[] bytes, int offset,
      int qualLength) {
    return MatchCode.SEEK_NEXT_COL;
  }

  public boolean isDone(long timestamp) {
    return minVersions <= 0 && isExpired(timestamp);
  }
}
