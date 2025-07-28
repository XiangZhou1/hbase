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
import java.util.NavigableSet;

import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.DeleteTracker.DeleteResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.base.Preconditions;

/**
 * A query matcher that is specifically designed for the scan case.
 * ScanQueryMatcher 的核心作用是：对于给定的一个 KeyValue，根据 Scan 请求的所有条件，判断应该如何处理这个 KeyValue。
 * ● 决策中心: 它是 StoreScanner 内部的决策核心。StoreScanner 从 KeyValueHeap 中取出每一个 KeyValue 后，都会立即将其交给 ScanQueryMatcher 的 match() 方法来“审判”。
 * ● 条件封装器: 它在构造时，会从 Scan 对象和 ScanInfo（列族 schema）中提取并封装所有相关的查询条件，包括：
 *   ○ 行键范围 (startRow, stopRow)
 *   ○ 时间范围 (TimeRange)
 *   ○ 版本数 (maxVersions)
 *   ○ 过滤器 (Filter)
 *   ○ 列选择（通过 ColumnTracker 实现）
 *   ○ 删除标记的处理逻辑（DeleteTracker）
 * ● 状态机: ScanQueryMatcher 是有状态的。它会记住当前正在处理的行 (row)，以及在该行内已经匹配到的列和版本。这使得它能够做出诸如“这个版本太多了，跳过”、“这一列已经满足条件，请 seek 到下一列”等复杂的决策。
 * ● 指令发出者: 它的 match() 方法的返回值是一个 MatchCode 枚举。这个 MatchCode 就是它给 StoreScanner 的明确指令，告诉 StoreScanner 下一步是应该包含当前 KeyValue、跳过它，还是高效地 seek 到下一个感兴趣的位置。
 */
@InterfaceAudience.Private
public class ScanQueryMatcher {
  // Optimization so we can skip lots of compares when we decide to skip
  // to the next row.
  private boolean stickyNextRow;
  private final byte[] stopRow;

  private final TimeRange tr;

  private final Filter filter;

  /** Keeps track of deletes
   * ● DeleteTracker:
   *   ○ 职责: 负责跟踪和应用删除标记。
   *   ○ 工作方式: 当 ScanQueryMatcher 遇到一个删除标记（如 DeleteColumn, DeleteFamily）时，会将其添加到 DeleteTracker 中。之后，对于遇到的每一个 Put 类型的 KeyValue，都会先询问 DeleteTracker：“这个 KeyValue 是否已经被删除了？” DeleteTracker 会根据其内部记录的删除标记的时间戳来做出判断。
   *   ○ 实现: ScanDeleteTracker 是其默认实现。
   * */
  private final DeleteTracker deletes;

  /*
   * The following three booleans define how we deal with deletes.
   * There are three different aspects:
   * 1. Whether to keep delete markers. This is used in compactions.
   *    Minor compactions always keep delete markers.
   * 2. Whether to keep deleted rows. This is also used in compactions,
   *    if the store is set to keep deleted rows. This implies keeping
   *    the delete markers as well.
   *    In this case deleted rows are subject to the normal max version
   *    and TTL/min version rules just like "normal" rows.
   * 3. Whether a scan can do time travel queries even before deleted
   *    marker to reach deleted rows.
   */
  /** whether to retain delete markers */
  private boolean retainDeletesInOutput;

  /** whether to return deleted rows */
  private final KeepDeletedCells keepDeletedCells;
  /** whether time range queries can see rows "behind" a delete */
  private final boolean seePastDeleteMarkers;


  /** Keeps track of columns and versions
   *   ○ 职责: 负责处理所有与列和版本相关的匹配逻辑。
   *   ○ 工作方式: 它会跟踪 Scan 请求中需要哪些列。对于每个 KeyValue，它会检查其列是否在请求范围内。更重要的是，它会为每个匹配的列维护一个版本计数器，根据 maxVersions 的设置，判断当前 KeyValue 的版本是否应该被包含。
   *   ○ 两种实现:
   *     ■ ScanWildcardColumnTracker: 用于处理扫描整个列族（scan.addFamily()）的情况。
   *     ■ ExplicitColumnTracker: 用于处理只扫描指定列（scan.addColumn()）的情况，性能更高。
   * */
  private final ColumnTracker columns;

  /** Key to seek to in memstore and StoreFiles */
  private final KeyValue startKey;

  /** Row comparator for the region this query is for */
  private final KeyValue.KVComparator rowComparator;

  /* row is not private for tests */
  /** Row the query is on */
  byte [] row;
  int rowOffset;
  short rowLength;
  
  /**
   * Oldest put in any of the involved store files
   * Used to decide whether it is ok to delete
   * family delete marker of this store keeps
   * deleted KVs.
   */
  private final long earliestPutTs;
  private final long ttl;

  /** The oldest timestamp we are interested in, based on TTL */
  private final long oldestUnexpiredTS;
  private final long now;

  /** readPoint over which the KVs are unconditionally included */
  protected long maxReadPointToTrackVersions;

  private byte[] dropDeletesFromRow = null, dropDeletesToRow = null;

  /**
   * This variable shows whether there is an null column in the query. There
   * always exists a null column in the wildcard column query.
   * There maybe exists a null column in the explicit column query based on the
   * first column.
   * */
  private boolean hasNullColumn = true;
  
  private RegionCoprocessorHost regionCoprocessorHost= null;

  // By default, when hbase.hstore.time.to.purge.deletes is 0ms, a delete
  // marker is always removed during a major compaction. If set to non-zero
  // value then major compaction will try to keep a delete marker around for
  // the given number of milliseconds. We want to keep the delete markers
  // around a bit longer because old puts might appear out-of-order. For
  // example, during log replication between two clusters.
  //
  // If the delete marker has lived longer than its column-family's TTL then
  // the delete marker will be removed even if time.to.purge.deletes has not
  // passed. This is because all the Puts that this delete marker can influence
  // would have also expired. (Removing of delete markers on col family TTL will
  // not happen if min-versions is set to non-zero)
  //
  // But, if time.to.purge.deletes has not expired then a delete
  // marker will not be removed just because there are no Puts that it is
  // currently influencing. This is because Puts, that this delete can
  // influence.  may appear out of order.
  private final long timeToPurgeDeletes;
  
  private final boolean isUserScan;

  private final boolean isReversed;

  /**
   * Construct a QueryMatcher for a scan
   * @param scan
   * @param scanInfo The store's immutable scan info
   * @param columns
   * @param scanType Type of the scan
   * @param earliestPutTs Earliest put seen in any of the store files.
   * @param oldestUnexpiredTS the oldest timestamp we are interested in,
   *  based on TTL
   * @param regionCoprocessorHost 
   * @throws IOException 
   */
  public ScanQueryMatcher(Scan scan, ScanInfo scanInfo, NavigableSet<byte[]> columns,
      ScanType scanType, long readPointToUse, long earliestPutTs, long oldestUnexpiredTS,
      long now, RegionCoprocessorHost regionCoprocessorHost) throws IOException {
    this.tr = scan.getTimeRange();
    this.rowComparator = scanInfo.getComparator();
    this.regionCoprocessorHost = regionCoprocessorHost;
    this.deletes =  instantiateDeleteTracker();
    this.stopRow = scan.getStopRow();
    this.startKey = KeyValue.createFirstDeleteFamilyOnRow(scan.getStartRow(),
        scanInfo.getFamily());
    this.filter = scan.getFilter();
    this.earliestPutTs = earliestPutTs;
    this.oldestUnexpiredTS = oldestUnexpiredTS;
    this.now = now;

    this.maxReadPointToTrackVersions = readPointToUse;
    this.timeToPurgeDeletes = scanInfo.getTimeToPurgeDeletes();
    this.ttl = oldestUnexpiredTS;

    /* how to deal with deletes */
    this.isUserScan = scanType == ScanType.USER_SCAN;
    // keep deleted cells: if compaction or raw scan
    this.keepDeletedCells = scan.isRaw() ? KeepDeletedCells.TRUE :
      isUserScan ? KeepDeletedCells.FALSE : scanInfo.getKeepDeletedCells();
    // retain deletes: if minor compaction or raw scanisDone
    this.retainDeletesInOutput = scanType == ScanType.COMPACT_RETAIN_DELETES || scan.isRaw();
    // seePastDeleteMarker: user initiated scans
    this.seePastDeleteMarkers =
        scanInfo.getKeepDeletedCells() != KeepDeletedCells.FALSE && isUserScan;

    int maxVersions =
        scan.isRaw() ? scan.getMaxVersions() : Math.min(scan.getMaxVersions(),
          scanInfo.getMaxVersions());

    // Single branch to deal with two types of reads (columns vs all in family)
    if (columns == null || columns.size() == 0) {
      // there is always a null column in the wildcard column query.
      hasNullColumn = true;

      // use a specialized scan for wildcard column tracker.
      this.columns = new ScanWildcardColumnTracker(
          scanInfo.getMinVersions(), maxVersions, oldestUnexpiredTS);
    } else {
      // whether there is null column in the explicit column query
      hasNullColumn = (columns.first().length == 0);

      // We can share the ExplicitColumnTracker, diff is we reset
      // between rows, not between storefiles.
      this.columns = new ExplicitColumnTracker(columns, scanInfo.getMinVersions(), maxVersions,
          oldestUnexpiredTS);
    }
    this.isReversed = scan.isReversed();
  }

  private DeleteTracker instantiateDeleteTracker() throws IOException {
    DeleteTracker tracker = new ScanDeleteTracker();
    if (regionCoprocessorHost != null) {
      tracker = regionCoprocessorHost.postInstantiateDeleteTracker(tracker);
    }
    return tracker;
  }

  /**
   * Construct a QueryMatcher for a scan that drop deletes from a limited range of rows.
   * @param scan
   * @param scanInfo The store's immutable scan info
   * @param columns
   * @param earliestPutTs Earliest put seen in any of the store files.
   * @param oldestUnexpiredTS the oldest timestamp we are interested in, based on TTL
   * @param now the current server time
   * @param dropDeletesFromRow The inclusive left bound of the range; can be EMPTY_START_ROW.
   * @param dropDeletesToRow The exclusive right bound of the range; can be EMPTY_END_ROW.
   * @param regionCoprocessorHost 
   * @throws IOException 
   */
  public ScanQueryMatcher(Scan scan, ScanInfo scanInfo, NavigableSet<byte[]> columns,
      long readPointToUse, long earliestPutTs, long oldestUnexpiredTS, long now, byte[] dropDeletesFromRow,
      byte[] dropDeletesToRow, RegionCoprocessorHost regionCoprocessorHost) throws IOException {
    this(scan, scanInfo, columns, ScanType.COMPACT_RETAIN_DELETES, readPointToUse, earliestPutTs,
        oldestUnexpiredTS, now, regionCoprocessorHost);
    Preconditions.checkArgument((dropDeletesFromRow != null) && (dropDeletesToRow != null));
    this.dropDeletesFromRow = dropDeletesFromRow;
    this.dropDeletesToRow = dropDeletesToRow;
  }

  /*
   * Constructor for tests
   */
  ScanQueryMatcher(Scan scan, ScanInfo scanInfo,
      NavigableSet<byte[]> columns, long oldestUnexpiredTS, long now) throws IOException {
    this(scan, scanInfo, columns, ScanType.USER_SCAN,
          Long.MAX_VALUE, /* max Readpoint to track versions */
        HConstants.LATEST_TIMESTAMP, oldestUnexpiredTS, now, null);
  }

  /**
   *
   * @return  whether there is an null column in the query
   */
  public boolean hasNullColumnInQuery() {
    return hasNullColumn;
  }

  /**
   * Determines if the caller should do one of several things:
   * - seek/skip to the next row (MatchCode.SEEK_NEXT_ROW)
   * - seek/skip to the next column (MatchCode.SEEK_NEXT_COL)
   * - include the current KeyValue (MatchCode.INCLUDE)
   * - ignore the current KeyValue (MatchCode.SKIP)
   * - got to the next row (MatchCode.DONE)
   *
   * @param kv KeyValue to check
   * @return The match code instance.
   * @throws IOException in case there is an internal consistency problem
   *      caused by a data corruption.
   */
  public MatchCode match(KeyValue kv) throws IOException {
    if (filter != null && filter.filterAllRemaining()) {
      return MatchCode.DONE_SCAN;
    }

    byte [] bytes = kv.getBuffer();
    int offset = kv.getOffset();

    int keyLength = Bytes.toInt(bytes, offset, Bytes.SIZEOF_INT);
    offset += KeyValue.ROW_OFFSET;

    int initialOffset = offset;

    short rowLength = Bytes.toShort(bytes, offset, Bytes.SIZEOF_SHORT);
    offset += Bytes.SIZEOF_SHORT;

    /**
     * ● 行范围检查 (Row Range Check):
     *   ○ 首先，比较当前 kv 的行键与 matcher 内部记录的当前行 (this.row)。
     *   ○ 如果 kv 的行键小于当前行（在反向扫描中是大于），说明扫描器可能需要重新 seek，返回 SEEK_NEXT_ROW。
     *   ○ 如果 kv 的行键大于当前行（在反向扫描中是小于），说明当前行已经处理完毕，返回 DONE，StoreScanner 会开始处理新行。
     *   ○ 如果行键匹配，则继续。
     */
    int ret = this.rowComparator.compareRows(row, this.rowOffset, this.rowLength,
        bytes, offset, rowLength);
    if (!this.isReversed) {
      if (ret <= -1) {
        return MatchCode.DONE;
      } else if (ret >= 1) {
        // could optimize this, if necessary?
        // Could also be called SEEK_TO_CURRENT_ROW, but this
        // should be rare/never happens.
        return MatchCode.SEEK_NEXT_ROW;
      }
    } else {
      if (ret <= -1) {
        return MatchCode.SEEK_NEXT_ROW;
      } else if (ret >= 1) {
        return MatchCode.DONE;
      }
    }

    // optimize case.
    // --- 3. 行级优化检查 ---
    // stickyNextRow 是一个状态标志。一旦被设为 true，意味着 matcher 已经判定当前行的剩余部分都不需要了。
    // 比如 Filter 返回了 NEXT_ROW，或者所有请求的列的版本都已找够。
    // 这个检查可以避免对同一行内后续的 KV 进行不必要的复杂判断。
    if (this.stickyNextRow)
        return MatchCode.SEEK_NEXT_ROW;

    // 如果 ColumnTracker 报告说所有需要的列都已经找到了足够的版本，
    // 那么当前行的后续 KV 也不再需要。设置 stickyNextRow 并返回 SEEK_NEXT_ROW。
    if (this.columns.done()) {
      stickyNextRow = true;
      return MatchCode.SEEK_NEXT_ROW;
    }

    //Passing rowLength
    // --- 4. 单元格级别解析与优化 ---
    // 继续解析 KV 的剩余部分。
    offset += rowLength;

    //Skipping family
    byte familyLength = bytes [offset];
    offset += familyLength + 1;

    int qualLength = keyLength -
      (offset - initialOffset) - KeyValue.TIMESTAMP_TYPE_SIZE;

    long timestamp = Bytes.toLong(bytes, initialOffset + keyLength - KeyValue.TIMESTAMP_TYPE_SIZE);
    // check for early out based on timestamp alone
    // 基于时间戳的优化：如果当前 KV 的时间戳比 ColumnTracker 记录的“最旧需要的时间戳”还要旧，
    // 那么它和它之后的（时间戳更旧的）KV 都不可能满足版本要求，可以直接 seek 到下一列或下一行。
    if (columns.isDone(timestamp)) {
      return columns.getNextRowOrNextColumn(kv.getQualifierArray(), kv.getQualifierOffset(),
        kv.getQualifierLength());
    }
    // check if the cell is expired by cell TTL
    // 基于 TTL 的检查：如果该单元格因为 TTL 过期了，直接跳过。
    if (HStore.isCellTTLExpired(kv, this.oldestUnexpiredTS, this.now)) {
      return MatchCode.SKIP;
    }    

    /*
     * The delete logic is pretty complicated now.
     * This is corroborated by the following:
     * 1. The store might be instructed to keep deleted rows around.
     * 2. A scan can optionally see past a delete marker now.
     * 3. If deleted rows are kept, we have to find out when we can
     *    remove the delete markers.
     * 4. Family delete markers are always first (regardless of their TS)
     * 5. Delete markers should not be counted as version
     * 6. Delete markers affect puts of the *same* TS
     * 7. Delete marker need to be version counted together with puts
     *    they affect
     */

    // --- 5. 删除标记处理 ---
    // 这是 match 方法中最复杂的部分，处理逻辑依赖于多种配置。
    byte type = bytes[initialOffset + keyLength - 1];
    if (kv.isDelete()) {
      if (keepDeletedCells == KeepDeletedCells.FALSE
          || (keepDeletedCells == KeepDeletedCells.TTL && timestamp < ttl)) {
        // first ignore delete markers if the scanner can do so, and the
        // range does not include the marker
        //
        // during flushes and compactions also ignore delete markers newer
        // than the readpoint of any open scanner, this prevents deleted
        // rows that could still be seen by a scanner from being collected
        boolean includeDeleteMarker = seePastDeleteMarkers ?
            tr.withinTimeRange(timestamp) :
            tr.withinOrAfterTimeRange(timestamp);
        if (includeDeleteMarker
            && kv.getMvccVersion() <= maxReadPointToTrackVersions) {
          this.deletes.add(kv);
        }
        // Can't early out now, because DelFam come before any other keys
      }

      if ((!isUserScan)
          && timeToPurgeDeletes > 0
          && (EnvironmentEdgeManager.currentTimeMillis() - timestamp) <= timeToPurgeDeletes) {
        return MatchCode.INCLUDE;
      } else if (retainDeletesInOutput || kv.getMvccVersion() > maxReadPointToTrackVersions) {
        // always include or it is not time yet to check whether it is OK
        // to purge deltes or not
        if (!isUserScan) {
          // if this is not a user scan (compaction), we can filter this deletemarker right here
          // otherwise (i.e. a "raw" scan) we fall through to normal version and timerange checking
          return MatchCode.INCLUDE;
        }
      } else if (keepDeletedCells == KeepDeletedCells.TRUE
          || (keepDeletedCells == KeepDeletedCells.TTL && timestamp >= ttl)) {
        if (timestamp < earliestPutTs) {
          // keeping delete rows, but there are no puts older than
          // this delete in the store files.
          return columns.getNextRowOrNextColumn(bytes, offset, qualLength);
        }
        // else: fall through and do version counting on the
        // delete markers
      } else {
        return MatchCode.SKIP;
      }
      // note the following next else if...
      // delete marker are not subject to other delete markers
    } else if (!this.deletes.isEmpty()) {
      DeleteResult deleteResult = deletes.isDeleted(kv);
      switch (deleteResult) {
        case FAMILY_DELETED:
        case COLUMN_DELETED:
          return columns.getNextRowOrNextColumn(bytes, offset, qualLength);
        case VERSION_DELETED:
        case FAMILY_VERSION_DELETED:
          return MatchCode.SKIP;
        case NOT_DELETED:
          break;
        default:
          throw new RuntimeException("UNEXPECTED");
        }
    }

    // NOTE: Cryptic stuff!
    // if the timestamp is HConstants.OLDEST_TIMESTAMP, then this is a fake cell made to prime a
    // Scanner; See KeyValueUTil#createLastOnRow. This Cell should never end up returning out of
    // here a matchcode of INCLUDE else we will return to the client a fake Cell. If we call
    // TimeRange, it will return 0 because it doesn't deal in OLDEST_TIMESTAMP and we will fall
    // into the later code where we could return a matchcode of INCLUDE. See HBASE-16074 "ITBLL
    // fails, reports lost big or tiny families" for a horror story. Check here for
    // OLDEST_TIMESTAMP. TimeRange#compare is about more generic timestamps, between 0L and
    // Long.MAX_LONG. It doesn't do OLDEST_TIMESTAMP weird handling.

    // --- 6. 时间范围检查 ---
    // HConstants.OLDEST_TIMESTAMP 是一个特殊的标记值，用于构造 seek key (如 LastOnRow)，
    // 这种伪造的 Cell 绝不能返回给客户端。这里做一个特殊检查，将其视为不在任何时间范围内。
    int timestampComparison = timestamp == HConstants.OLDEST_TIMESTAMP? -1: tr.compare(timestamp);
    if (timestampComparison >= 1) {
      return MatchCode.SKIP;
    } else if (timestampComparison <= -1) {
      return columns.getNextRowOrNextColumn(bytes, offset, qualLength);
    }

    // STEP 1: Check if the column is part of the requested columns
    // --- 7. 核心匹配流程 (四步法) ---
    // 步骤 1: 检查列是否匹配 (Check if the column is part of the requested columns)
    // `columns` (ColumnTracker) 会判断当前 KV 的列是否是 Scan 请求的列之一。
    MatchCode colChecker = columns.checkColumn(bytes, offset, qualLength, type);
    if (colChecker == MatchCode.INCLUDE) {
      ReturnCode filterResponse = ReturnCode.SKIP;
      // STEP 2: Yes, the column is part of the requested columns. Check if filter is present
      if (filter != null) {
        // STEP 3: Filter the key value and return if it filters out
        filterResponse = filter.filterKeyValue(kv);
        switch (filterResponse) {
        case SKIP:
          return MatchCode.SKIP;
        case NEXT_COL:
          return columns.getNextRowOrNextColumn(bytes, offset, qualLength);
        case NEXT_ROW:
          stickyNextRow = true;
          return MatchCode.SEEK_NEXT_ROW;
        case SEEK_NEXT_USING_HINT:
          return MatchCode.SEEK_NEXT_USING_HINT;
        default:
          //It means it is either include or include and seek next
          break;
        }
      }
      /*
       * STEP 4: Reaching this step means the column is part of the requested columns and either
       * the filter is null or the filter has returned INCLUDE or INCLUDE_AND_NEXT_COL response.
       * Now check the number of versions needed. This method call returns SKIP, INCLUDE,
       * INCLUDE_AND_SEEK_NEXT_ROW, INCLUDE_AND_SEEK_NEXT_COL.
       *
       * FilterResponse            ColumnChecker               Desired behavior
       * INCLUDE                   SKIP                        row has already been included, SKIP.
       * INCLUDE                   INCLUDE                     INCLUDE
       * INCLUDE                   INCLUDE_AND_SEEK_NEXT_COL   INCLUDE_AND_SEEK_NEXT_COL
       * INCLUDE                   INCLUDE_AND_SEEK_NEXT_ROW   INCLUDE_AND_SEEK_NEXT_ROW
       * INCLUDE_AND_SEEK_NEXT_COL SKIP                        row has already been included, SKIP.
       * INCLUDE_AND_SEEK_NEXT_COL INCLUDE                     INCLUDE_AND_SEEK_NEXT_COL
       * INCLUDE_AND_SEEK_NEXT_COL INCLUDE_AND_SEEK_NEXT_COL   INCLUDE_AND_SEEK_NEXT_COL
       * INCLUDE_AND_SEEK_NEXT_COL INCLUDE_AND_SEEK_NEXT_ROW   INCLUDE_AND_SEEK_NEXT_ROW
       *
       * In all the above scenarios, we return the column checker return value except for
       * FilterResponse (INCLUDE_AND_SEEK_NEXT_COL) and ColumnChecker(INCLUDE)
       */
      colChecker =
          columns.checkVersions(bytes, offset, qualLength, timestamp, type,
            kv.getMvccVersion() > maxReadPointToTrackVersions);
      //Optimize with stickyNextRow
      stickyNextRow = colChecker == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW ? true : stickyNextRow;
      return (filterResponse == ReturnCode.INCLUDE_AND_NEXT_COL &&
          colChecker == MatchCode.INCLUDE) ? MatchCode.INCLUDE_AND_SEEK_NEXT_COL
          : colChecker;
    }
    stickyNextRow = (colChecker == MatchCode.SEEK_NEXT_ROW) ? true
        : stickyNextRow;
    return colChecker;
  }

  /** Handle partial-drop-deletes. As we match keys in order, when we have a range from which
   * we can drop deletes, we can set retainDeletesInOutput to false for the duration of this
   * range only, and maintain consistency. */
  private void checkPartialDropDeleteRange(byte [] row, int offset, short length) {
    // If partial-drop-deletes are used, initially, dropDeletesFromRow and dropDeletesToRow
    // are both set, and the matcher is set to retain deletes. We assume ordered keys. When
    // dropDeletesFromRow is leq current kv, we start dropping deletes and reset
    // dropDeletesFromRow; thus the 2nd "if" starts to apply.
    if ((dropDeletesFromRow != null)
        && ((dropDeletesFromRow == HConstants.EMPTY_START_ROW)
          || (Bytes.compareTo(row, offset, length,
              dropDeletesFromRow, 0, dropDeletesFromRow.length) >= 0))) {
      retainDeletesInOutput = false;
      dropDeletesFromRow = null;
    }
    // If dropDeletesFromRow is null and dropDeletesToRow is set, we are inside the partial-
    // drop-deletes range. When dropDeletesToRow is leq current kv, we stop dropping deletes,
    // and reset dropDeletesToRow so that we don't do any more compares.
    if ((dropDeletesFromRow == null)
        && (dropDeletesToRow != null) && (dropDeletesToRow != HConstants.EMPTY_END_ROW)
        && (Bytes.compareTo(row, offset, length,
            dropDeletesToRow, 0, dropDeletesToRow.length) >= 0)) {
      retainDeletesInOutput = true;
      dropDeletesToRow = null;
    }
  }

  public boolean moreRowsMayExistAfter(KeyValue kv) {
    if (this.isReversed) {
      if (rowComparator.compareRows(kv.getRowArray(), kv.getRowOffset(),
          kv.getRowLength(), stopRow, 0, stopRow.length) <= 0) {
        return false;
      } else {
        return true;
      }
    }
    if (!Bytes.equals(stopRow , HConstants.EMPTY_END_ROW) &&
        rowComparator.compareRows(kv.getRowArray(),kv.getRowOffset(),
            kv.getRowLength(), stopRow, 0, stopRow.length) >= 0) {
      // KV >= STOPROW
      // then NO there is nothing left.
      return false;
    } else {
      return true;
    }
  }

  /**
   * Set current row
   * @param row
   */
  public void setRow(byte [] row, int offset, short length) {
    checkPartialDropDeleteRange(row, offset, length);
    this.row = row;
    this.rowOffset = offset;
    this.rowLength = length;
    reset();
  }

  public void reset() {
    this.deletes.reset();
    this.columns.reset();

    stickyNextRow = false;
  }

  /**
   *
   * @return the start key
   */
  public KeyValue getStartKey() {
    return this.startKey;
  }

  /**
   *
   * @return the Filter
   */
  Filter getFilter() {
    return this.filter;
  }

  public Cell getNextKeyHint(Cell kv) throws IOException {
    if (filter == null) {
      return null;
    } else {
      return filter.getNextCellHint(kv);
    }
  }

  public KeyValue getKeyForNextColumn(KeyValue kv) {
    ColumnCount nextColumn = columns.getColumnHint();
    if (nextColumn == null) {
      return KeyValue.createLastOnRow(
          kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
          kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
          kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
    } else {
      return KeyValue.createFirstOnRow(
          kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
          kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
          nextColumn.getBuffer(), nextColumn.getOffset(), nextColumn.getLength());
    }
  }

  public KeyValue getKeyForNextRow(KeyValue kv) {
    return KeyValue.createLastOnRow(
      kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
      null, 0, 0,
      null, 0, 0);
  }

  /**
   * @param nextIndexed the key of the next entry in the block index (if any)
   * @param kv The Cell we're using to calculate the seek key
   * @return result of the compare between the indexed key and the key portion of the passed cell
   */
  public int compareKeyForNextRow(byte[] nextIndexed, Cell kv) {
    return rowComparator.compareKey(nextIndexed, 0, nextIndexed.length,
      kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
      null, 0, 0, null, 0, 0,
      HConstants.OLDEST_TIMESTAMP, Type.Minimum.getCode());
  }

  /**
   * @param nextIndexed the key of the next entry in the block index (if any)
   * @param kv The Cell we're using to calculate the seek key
   * @return result of the compare between the indexed key and the key portion of the passed cell
   */
  public int compareKeyForNextColumn(byte[] nextIndexed, Cell kv) {
    ColumnCount nextColumn = columns.getColumnHint();
    if (nextColumn == null) {
      return rowComparator.compareKey(nextIndexed, 0, nextIndexed.length,
        kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
        kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
        kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
        HConstants.OLDEST_TIMESTAMP, Type.Minimum.getCode());
    } else {
      return rowComparator.compareKey(nextIndexed, 0, nextIndexed.length,
        kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
        kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
        nextColumn.getBuffer(), nextColumn.getOffset(), nextColumn.getLength(),
        HConstants.LATEST_TIMESTAMP, Type.Maximum.getCode());
    }
  }

  //Used only for testing purposes
  static MatchCode checkColumn(ColumnTracker columnTracker, byte[] bytes, int offset,
      int length, long ttl, byte type, boolean ignoreCount) throws IOException {
    MatchCode matchCode = columnTracker.checkColumn(bytes, offset, length, type);
    if (matchCode == MatchCode.INCLUDE) {
      return columnTracker.checkVersions(bytes, offset, length, ttl, type, ignoreCount);
    }
    return matchCode;
  }

  /**
   * {@link #match} return codes.  These instruct the scanner moving through
   * memstores and StoreFiles what to do with the current KeyValue.
   * <p>
   * Additionally, this contains "early-out" language to tell the scanner to
   * move on to the next File (memstore or Storefile), or to return immediately.
   */
  public static enum MatchCode {
    /**
     * Include KeyValue in the returned result
     */
    INCLUDE,

    /**
     * Do not include KeyValue in the returned result
     */
    SKIP,

    /**
     * Do not include, jump to next StoreFile or memstore (in time order)
     */
    NEXT,

    /**
     * Do not include, return current result
     */
    DONE,

    /**
     * These codes are used by the ScanQueryMatcher
     */

    /**
     * Done with the row, seek there.
     */
    SEEK_NEXT_ROW,
    /**
     * Done with column, seek to next.
     */
    SEEK_NEXT_COL,

    /**
     * Done with scan, thanks to the row filter.
     */
    DONE_SCAN,

    /*
     * Seek to next key which is given as hint.
     */
    SEEK_NEXT_USING_HINT,

    /**
     * Include KeyValue and done with column, seek to next.
     */
    INCLUDE_AND_SEEK_NEXT_COL,

    /**
     * Include KeyValue and done with row, seek to next.
     */
    INCLUDE_AND_SEEK_NEXT_ROW,
  }
}
