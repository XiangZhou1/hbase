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

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is responsible for the tracking and enforcement of Deletes
 * during the course of a Scan operation.
 *
 * It only has to enforce Delete and DeleteColumn, since the
 * DeleteFamily is handled at a higher level.
 *
 * <p>
 * This class is utilized through three methods:
 * <ul><li>{@link #add} when encountering a Delete or DeleteColumn
 * <li>{@link #isDeleted} when checking if a Put KeyValue has been deleted
 * <li>{@link #update} when reaching the end of a StoreFile or row for scans
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded
 *
 * ScanDeleteTracker 的核心作用是实现一个高效的状态机，用于判断一个 Put 类型的 KeyValue 是否已经被之前遇到的删除标记所“遮蔽”或“删除”。
 * ● ScanQueryMatcher 的组件: 它是 ScanQueryMatcher 内部的一个核心组件。ScanQueryMatcher 在处理每个 KeyValue 时，都会与 ScanDeleteTracker 进行交互。
 * ● 行级作用域: ScanDeleteTracker 的生命周期和作用域严格限制在单行的扫描过程中。当 StoreScanner 开始扫描一个新行时，
 *       会调用 ScanQueryMatcher.setRow()，后者接着会调用 deleteTracker.reset()，清空所有状态，为新一行的删除跟踪做准备。
 * ● 非线程安全: 如 Javadoc 中所述，这个类是非线程安全的，因为它被设计为在单个扫描线程中使用。
 * ● 处理的删除类型: 它主要处理以下三种删除标记：
 *   ○ DeleteFamily: 删除整个列族在某个时间戳之前的所有版本。
 *   ○ DeleteFamilyVersion: 删除整个列族在某个特定时间戳的版本。
 *   ○ DeleteColumn: 删除某个特定列在某个时间戳之前的所有版本。
 *   ○ Delete: 删除某个特定列在某个特定时间戳的版本。
 */
@InterfaceAudience.Private
public class ScanDeleteTracker implements DeleteTracker {

  protected boolean hasFamilyStamp = false;
  protected long familyStamp = 0L;
  protected SortedSet<Long> familyVersionStamps = new TreeSet<Long>();
  protected byte [] deleteBuffer = null;
  protected int deleteOffset = 0;
  protected int deleteLength = 0;

  // 记录了当前列级删除标记的类型（DeleteColumn 还是 Delete）和时间戳。
  protected byte deleteType = 0;
  protected long deleteTimestamp = 0L;

  /**
   * Constructor for ScanDeleteTracker
   */
  public ScanDeleteTracker() {
    super();
  }

  /**
   * Add the specified KeyValue to the list of deletes to check against for
   * this row operation.
   * <p>
   * This is called when a Delete is encountered.
   * @param cell - the delete cell
   * ● 执行流程:
   *   ○ 优先级判断: if (!hasFamilyStamp || timestamp > familyStamp)。首先检查新遇到的删除标记的时间戳是否比已记录的 familyStamp 更新。因为一个更新的 DeleteFamily 会覆盖一个旧的，所以只保留最新的即可。
   *   ○ 处理 DeleteFamily: 如果是 DeleteFamily 类型，就更新 hasFamilyStamp 和 familyStamp。
   *   ○ 处理 DeleteFamilyVersion: 如果是 DeleteFamilyVersion 类型，就将其时间戳加入 familyVersionStamps 集合。
   *   ○ 处理 DeleteColumn / Delete:
   *     ■ 平级比较: if (deleteBuffer != null && type < deleteType)。如果当前已经记录了一个列级删除标记，并且新遇到的标记类型更“宽泛”（例如，已有一个 Delete，又来一个 DeleteColumn，Delete 的类型值更大），则忽略新标记。
   *     ■ 更新状态: 否则，用新标记的列、类型和时间戳更新 deleteBuffer, deleteType, deleteTimestamp。
   */
  @Override
  public void add(Cell cell) {
    long timestamp = cell.getTimestamp();
    int qualifierOffset = cell.getQualifierOffset();
    int qualifierLength = cell.getQualifierLength();
    byte type = cell.getTypeByte();
    if (!hasFamilyStamp || timestamp > familyStamp) {
      if (type == KeyValue.Type.DeleteFamily.getCode()) {
        hasFamilyStamp = true;
        familyStamp = timestamp;
        return;
      } else if (type == KeyValue.Type.DeleteFamilyVersion.getCode()) {
        familyVersionStamps.add(timestamp);
        return;
      }

      if (deleteBuffer != null && type < deleteType) {
        // same column, so ignore less specific delete
        if (Bytes.equals(deleteBuffer, deleteOffset, deleteLength,
            cell.getQualifierArray(), qualifierOffset, qualifierLength)){
          return;
        }
      }
      // new column, or more general delete type
      deleteBuffer = cell.getQualifierArray();
      deleteOffset = qualifierOffset;
      deleteLength = qualifierLength;
      deleteType = type;
      deleteTimestamp = timestamp;
    }
    // missing else is never called.
  }

  /**
   * Check if the specified KeyValue buffer has been deleted by a previously
   * seen delete.
   *
   * @param cell - current cell to check if deleted by a previously seen delete
   * @return deleteResult
   *
   *
   * 当 ScanQueryMatcher 遇到一个 Put 类型的 Cell 时，会调用此方法来“审判”它是否应该被过滤掉。
   * ● 执行流程 (按优先级顺序):
   *   ○ 检查 DeleteFamily: if (hasFamilyStamp && timestamp <= familyStamp)。
   *     ■ 检查当前 Put 的时间戳是否小于或等于已记录的 familyStamp。
   *     ■ 如果是，说明它被 DeleteFamily 删除了，立即返回 DeleteResult.FAMILY_DELETED。ScanQueryMatcher 收到这个结果后，会直接 seek 到下一行，因为这一行的所有数据都被删了。
   *   ○ 检查 DeleteFamilyVersion: if (familyVersionStamps.contains(Long.valueOf(timestamp)))。
   *     ■ 检查当前 Put 的时间戳是否存在于 familyVersionStamps 集合中。
   *     ■ 如果是，说明它被 DeleteFamilyVersion 删除了，返回 DeleteResult.FAMILY_VERSION_DELETED。ScanQueryMatcher 会 skip 这个版本。
   *   ○ 检查 DeleteColumn / Delete: if (deleteBuffer != null)。
   *     ■ 比较列: Bytes.compareTo(...)。比较当前 Put 的列限定符和 deleteBuffer 中记录的列限定符。
   *     ■ 如果列相同 (ret == 0):
   *       ● 如果 deleteType 是 DeleteColumn，那么所有时间戳小于等于 deleteTimestamp 的 Put 都被删除。返回 DeleteResult.COLUMN_DELETED。ScanQueryMatcher 会 seek 到下一列。
   *       ● 如果 deleteType 是 Delete，只有时间戳完全相同的 Put 被删除。返回 DeleteResult.VERSION_DELETED。ScanQueryMatcher 会 skip 这个版本。
   *       ● 状态重置: 如果 Put 的时间戳比 Delete 的时间戳更早，说明这个 Delete 标记已经处理完毕（因为后续的 Put 时间戳只会更小），此时会将 deleteBuffer 设为 null，为下一个不同的列做准备。
   *     ■ 如果 Put 的列在 deleteBuffer 之后 (ret > 0): 这是不可能发生的，因为数据是有序的。如果发生，说明有内部逻辑错误，抛出 IllegalStateException。
   *     ■ 如果 Put 的列在 deleteBuffer 之前 (ret < 0): 说明 deleteBuffer 中记录的那个列已经扫描完毕，现在进入了一个新的列。将 deleteBuffer 设为 null。
   *   ○ 未被删除: 如果通过了以上所有检查，返回 DeleteResult.NOT_DELETED。
   */
  @Override
  public DeleteResult isDeleted(Cell cell) {
    long timestamp = cell.getTimestamp();
    int qualifierOffset = cell.getQualifierOffset();
    int qualifierLength = cell.getQualifierLength();
    if (hasFamilyStamp && timestamp <= familyStamp) {
      return DeleteResult.FAMILY_DELETED;
    }

    if (familyVersionStamps.contains(Long.valueOf(timestamp))) {
        return DeleteResult.FAMILY_VERSION_DELETED;
    }

    if (deleteBuffer != null) {
      int ret = Bytes.compareTo(deleteBuffer, deleteOffset, deleteLength,
          cell.getQualifierArray(), qualifierOffset, qualifierLength);

      if (ret == 0) {
        if (deleteType == KeyValue.Type.DeleteColumn.getCode()) {
          return DeleteResult.COLUMN_DELETED;
        }
        // Delete (aka DeleteVersion)
        // If the timestamp is the same, keep this one
        if (timestamp == deleteTimestamp) {
          return DeleteResult.VERSION_DELETED;
        }
        // use assert or not?
        assert timestamp < deleteTimestamp;

        // different timestamp, let's clear the buffer.
        deleteBuffer = null;
      } else if(ret < 0){
        // Next column case.
        deleteBuffer = null;
      } else {
        throw new IllegalStateException("isDelete failed: deleteBuffer="
            + Bytes.toStringBinary(deleteBuffer, deleteOffset, deleteLength)
            + ", qualifier="
            + Bytes.toStringBinary(cell.getQualifierArray(), qualifierOffset, qualifierLength)
            + ", timestamp=" + timestamp + ", comparison result: " + ret);
      }
    }

    return DeleteResult.NOT_DELETED;
  }

  @Override
  public boolean isEmpty() {
    return deleteBuffer == null && !hasFamilyStamp &&
           familyVersionStamps.isEmpty();
  }

  @Override
  // called between every row.
  public void reset() {
    hasFamilyStamp = false;
    familyStamp = 0L;
    familyVersionStamps.clear();
    deleteBuffer = null;
  }

  @Override
  // should not be called at all even (!)
  public void update() {
    this.reset();
  }
}
