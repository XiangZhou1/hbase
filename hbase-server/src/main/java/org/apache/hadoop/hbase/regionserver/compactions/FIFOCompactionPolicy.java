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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 *
 * FIFO compaction policy selects only files which have all cells expired.
 * The column family MUST have non-default TTL. One of the use cases for this
 * policy is when we need to store raw data which will be post-processed later
 * and discarded completely after quite short period of time. Raw time-series vs.
 * time-based roll up aggregates and compacted time-series. We collect raw time-series
 * and store them into CF with FIFO compaction policy, periodically we run task
 * which creates roll up aggregates and compacts time-series, the original raw data
 * can be discarded after that.
 *
 * FIFOCompactionPolicy 将一个 Store（列族）视为一个有时效性的数据队列。HFile 被不断地从队尾加入（通过 Flush），
 * 当一个 HFile 已经足够“老”，以至于其内部所有的 Cell 都已经因为 TTL (Time-To-Live) 而过期时，这个 HFile 就会被从队头整个丢弃。它几乎不进行数据合并，而是直接删除整个过期的 HFile。
 * 与默认策略的根本区别：
 * ● 默认策略（如 Exploring）：目标是合并数据以优化读取性能和回收空间。它会读取多个 HFile 的内容，进行归并排序，然后写入新的 HFile。
 * ● FIFO 策略：目标是丢弃数据。它不关心文件内部的数据，只关心整个文件是否过期。它避免了昂贵的读写合并操作，通过直接删除文件来回收空间，效率极高。
 *
 *
 * FIFOCompactionPolicy 是一个为特定场景设计的、高度优化的“丢弃”策略。它通过完全放弃数据合并，只关注 HFile 的整体过期，从而实现了极高效的空间回收。它与默认的 Compaction 策略在目标和行为上都有着根本性的差异。其关键逻辑在于：
 * ● 依赖 TTL: 必须为列族设置 TTL。
 * ● 检查最大时间戳: 通过比较 文件的最大时间戳 + TTL 与当前时间，来判断整个文件是否过期。
 * ● 不进行合并: 它选择出来的“合并”请求，实际上是让 Compaction 框架去执行一个输入为 N 个文件、输出为 0 个文件的操作，从而达到删除输入文件的目的。
 * ● 特殊情况处理: 它能正确识别并处理 Region Split 后必须进行的 Major Compaction，临时退化为父类的行为来保证数据一致性。
 */
@InterfaceAudience.Private
public class FIFOCompactionPolicy extends ExploringCompactionPolicy {

  private static final Log LOG = LogFactory.getLog(FIFOCompactionPolicy.class);


  public FIFOCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  @Override
  public CompactionRequest selectCompaction(Collection<StoreFile> candidateFiles,
      List<StoreFile> filesCompacting, boolean isUserCompaction, boolean mayUseOffPeak,
      boolean forceMajor) throws IOException {
    // 1. Major Compaction 对 FIFO 策略无意义，直接忽略
    if(forceMajor){
      LOG.warn("Major compaction is not supported for FIFO compaction policy. Ignore the flag.");
    }

    // 2. 如果检测到 Region 刚分裂过 (存在引用文件)
    boolean isAfterSplit = StoreUtils.hasReferences(candidateFiles);
    if(isAfterSplit){
      LOG.info("Split detected, delegate selection to the parent policy.");
      return super.selectCompaction(candidateFiles, filesCompacting, isUserCompaction,
        mayUseOffPeak, forceMajor);
    }

    // Nothing to compact
    // 3. 正常情况下，选择所有已过期的文件进行 "compaction" (实为丢弃)
    Collection<StoreFile> toCompact = getExpiredStores(candidateFiles, filesCompacting);
    CompactionRequest result = new CompactionRequest(toCompact);
    return result;
  }

  @Override
  public boolean shouldPerformMajorCompaction(Collection<StoreFile> filesToCompact) throws IOException {
    boolean isAfterSplit = StoreUtils.hasReferences(filesToCompact);
    if(isAfterSplit){
      LOG.info("Split detected, delegate to the parent policy.");
      return super.shouldPerformMajorCompaction(filesToCompact);
    }
    return false;
  }

  @Override
  public boolean needsCompaction(Collection<StoreFile> storeFiles,
      List<StoreFile> filesCompacting) {
    boolean isAfterSplit = StoreUtils.hasReferences(storeFiles);
    if(isAfterSplit){
      LOG.info("Split detected, delegate to the parent policy.");
      return super.needsCompaction(storeFiles, filesCompacting);
    }
    return hasExpiredStores(storeFiles);
  }

  private  boolean hasExpiredStores(Collection<StoreFile> files) {
    long currentTime = EnvironmentEdgeManager.currentTimeMillis();
    for(StoreFile sf: files){
      // Check MIN_VERSIONS is in HStore removeUnneededFiles
      Long maxTs = sf.getReader().getMaxTimestamp();
      long maxTtl = storeConfigInfo.getStoreFileTtl();
      if(maxTs == null
          || maxTtl == Long.MAX_VALUE
          || (currentTime - maxTtl < maxTs)){
        continue;
      } else{
        return true;
      }
    }
    return false;
  }

  private  Collection<StoreFile> getExpiredStores(Collection<StoreFile> files,
    Collection<StoreFile> filesCompacting) {
    long currentTime = EnvironmentEdgeManager.currentTimeMillis();
    Collection<StoreFile> expiredStores = new ArrayList<StoreFile>();
    for(StoreFile sf: files){
      // Check MIN_VERSIONS is in HStore removeUnneededFiles
      Long maxTs = sf.getReader().getMaxTimestamp();
      long maxTtl = storeConfigInfo.getStoreFileTtl();
      if(maxTs == null
          || maxTtl == Long.MAX_VALUE
          || (currentTime - maxTtl < maxTs)){
        continue;
      } else if(filesCompacting == null || filesCompacting.contains(sf) == false){
        expiredStores.add(sf);
      }
    }
    return expiredStores;
  }
}
