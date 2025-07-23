/**
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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreUtils;

/**
 * The default algorithm for selecting files for compaction. Combines the compaction configuration
 * and the provisional file selection that it's given to produce the list of suitable candidates for
 * compaction.
 */
@InterfaceAudience.Private
public class RatioBasedCompactionPolicy extends SortedCompactionPolicy {
  private static final Log LOG = LogFactory.getLog(RatioBasedCompactionPolicy.class);

  public RatioBasedCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  /*
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  @Override
  public boolean shouldPerformMajorCompaction(final Collection<StoreFile> filesToCompact)
      throws IOException {
    boolean result = false;
    long mcTime = getNextMajorCompactTime(filesToCompact);
    if (filesToCompact == null || filesToCompact.isEmpty() || mcTime == 0) {
      return result;
    }
    // TODO: Use better method for determining stamp of last major (HBASE-2990)
    /**
     * ● 检查最旧文件的时间戳:
     *   ○ 获取当前 Store 中所有 HFile 里最旧的那个文件的时间戳（lowTimestamp）。
     */
    long lowTimestamp = StoreUtils.getLowestTimestamp(filesToCompact);
    long now = System.currentTimeMillis();
    /**
     * ● 时间比较:
     *   ○ 用当前时间 now 减去 lowTimestamp，得到最旧文件的“年龄”。
     *   ○ 如果这个“年龄”大于配置的 Major Compaction 周期 (mcTime)，则基本确定需要进行 Major Compaction。
     */
    if (lowTimestamp > 0l && lowTimestamp < (now - mcTime)) {
      // Major compaction time has elapsed.
      long cfTtl = this.storeConfigInfo.getStoreFileTtl();
      if (filesToCompact.size() == 1) {
        // Single file
        StoreFile sf = filesToCompact.iterator().next();
        Long minTimestamp = sf.getMinimumTimestamp();
        long oldest = (minTimestamp == null) ? Long.MIN_VALUE : now - minTimestamp.longValue();
        /**
         * ● 特殊情况处理:
         *   ○ TTL (Time-To-Live): 如果 Store 设置了 TTL，并且最旧文件的年龄已经超过了 TTL，那么为了清理过期数据，也必须触发 Major Compaction。
         *   ○ 数据本地性 (Block Locality): 如果 Store 中只有一个 HFile，并且它已经经过了 Major Compaction，正常情况下不需要再次合并。
         *   但如果这个文件的 HDFS 数据块在本地节点上的比例很低（低于 hbase.hstore.min.locality.to.force.major.compact），
         *   为了提升数据本地性、优化读性能，也会强制触发一次 Major Compaction。
         */
        if (sf.isMajorCompaction() && (cfTtl == Long.MAX_VALUE || oldest < cfTtl)) {
          float blockLocalityIndex =
              sf.getHDFSBlockDistribution().getBlockLocalityIndex(
                HRegionServer.getHostname(comConf.conf));
          if (blockLocalityIndex < comConf.getMinLocalityToForceCompact()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Major compaction triggered on only store " + this
                  + "; to make hdfs blocks local, current blockLocalityIndex is "
                  + blockLocalityIndex + " (min " + comConf.getMinLocalityToForceCompact() + ")");
            }
            result = true;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Skipping major compaction of " + this
                  + " because one (major) compacted file only, oldestTime " + oldest
                  + "ms is < ttl=" + cfTtl + " and blockLocalityIndex is " + blockLocalityIndex
                  + " (min " + comConf.getMinLocalityToForceCompact() + ")");
            }
          }
        } else if (cfTtl != HConstants.FOREVER && oldest > cfTtl) {
          LOG.debug("Major compaction triggered on store " + this
              + ", because keyvalues outdated; time since last major compaction "
              + (now - lowTimestamp) + "ms");
          result = true;
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Major compaction triggered on store " + this
              + "; time since last major compaction " + (now - lowTimestamp) + "ms");
        }
        result = true;
      }
    }
    return result;
  }

  @Override
  protected CompactionRequest getCompactionRequest(ArrayList<StoreFile> candidateSelection,
      boolean tryingMajor, boolean isUserCompaction, boolean mayUseOffPeak, boolean mayBeStuck)
      throws IOException {
    /**
     * ● 判断是否是 Major Compaction (!tryingMajor):
     *   ○ 如果不是 Major Compaction（即是 Minor Compaction），则执行以下步骤。如果是 Major Compaction，则默认选择所有文件，直接跳到步骤 2。
     *   ○ filterBulk(...): 根据配置，排除掉 Bulk Load 进来的文件。
     *   ○ applyCompactionPolicy(...): 这是 Ratio 策略的核心算法所在，下面会详细讲解。它会根据 Ratio 算法筛选出一组合适的文件。
     *   ○ checkMinFilesCriteria(...): 检查筛选出的文件数量是否达到了最小合并数（hbase.hstore.compaction.min.files），如果不够，则取消本次 Compaction。
     */
    if (!tryingMajor) {
      candidateSelection = filterBulk(candidateSelection);
      candidateSelection = applyCompactionPolicy(candidateSelection, mayUseOffPeak, mayBeStuck);
      candidateSelection =
          checkMinFilesCriteria(candidateSelection, comConf.getMinFilesToCompact());
    }
    // removeExcessFiles(...): 确保最终选择的文件数不超过最大合并数（hbase.hstore.compaction.max.files）。
    removeExcessFiles(candidateSelection, isUserCompaction, tryingMajor);
    // 创建并返回 CompactionRequest: 将最终确定的文件列表包装成一个 CompactionRequest 对象返回。
    CompactionRequest result = new CompactionRequest(candidateSelection);
    result.setOffPeak(!candidateSelection.isEmpty() && !tryingMajor && mayUseOffPeak);
    return result;
  }

  /**
   * @param candidates pre-filtrate
   * @return filtered subset -- Default minor compaction selection algorithm: choose
   *         CompactSelection from candidates -- First exclude bulk-load files if indicated in
   *         configuration. Start at the oldest file and stop when you find the first file that
   *         meets compaction criteria: (1) a recently-flushed, small file (i.e. <= minCompactSize)
   *         OR (2) within the compactRatio of sum(newer_files) Given normal skew, any newer files
   *         will also meet this criteria
   *         <p/>
   *         Additional Note: If fileSizes.size() >> maxFilesToCompact, we will recurse on
   *         compact(). Consider the oldest files first to avoid a situation where we always compact
   *         [end-threshold,end). Then, the last file becomes an aggregate of the previous
   *         compactions. normal skew: older ----> newer (increasing seqID) _ | | _ | | | | _ --|-|-
   *         |-|- |-|---_-------_------- minCompactSize | | | | | | | | _ | | | | | | | | | | | | |
   *         | | | | | | | | | | | | |
   *
   *
   * 从最旧的文件开始，逐个检查每个文件 (F) 是否满足合并条件。一个文件 F 满足条件，当且仅当它的大小小于或等于所有比它更新的文件的总大小乘以一个比例 (ratio)。
   * size(F) <= sum(size(all_newer_files)) * ratio
   * 这个算法的直观理解是：一个文件只有在它相对于后面（更新的）那些文件的总和来说“足够小”的时候，才值得被合并。
   * 如果一个文件自己已经很大了，而后面的文件都很小，那么合并的开销会很大，但收益（减少的文件数）却很小，性价比低。
   */
  protected ArrayList<StoreFile> applyCompactionPolicy(ArrayList<StoreFile> candidates,
      boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    if (candidates.isEmpty()) {
      return candidates;
    }

    // we're doing a minor compaction, let's see what files are applicable
    int start = 0;
    double ratio = comConf.getCompactionRatio();
    if (mayUseOffPeak) {
      ratio = comConf.getCompactionRatioOffPeak();
      LOG.info("Running an off-peak compaction, selection ratio = " + ratio);
    }

    // get store file sizes for incremental compacting selection.
    // 预计算文件大小和后缀和
    /**
     * ● 预计算:
     *   ○ 获取所有候选 HFile 的大小，存入 fileSizes 数组。
     *   ○ 为了快速计算 sum(size(all_newer_files))，它会预先计算一个后缀和数组 sumSize。sumSize[i] 存储了从 fileSizes[i] 到 fileSizes[i + maxFilesToCompact - 1] 的文件大小之和（有一个窗口限制）。
     */
    final int countOfFiles = candidates.size();
    long[] fileSizes = new long[countOfFiles];
    long[] sumSize = new long[countOfFiles];


    for (int i = countOfFiles - 1; i >= 0; --i) {
      StoreFile file = candidates.get(i);
      fileSizes[i] = file.getReader().length();
      // calculate the sum of fileSizes[i,i+maxFilesToCompact-1) for algo
      int tooFar = i + comConf.getMaxFilesToCompact() - 1;
      sumSize[i] =
          fileSizes[i] + ((i + 1 < countOfFiles) ? sumSize[i + 1] : 0)
              - ((tooFar < countOfFiles) ? fileSizes[tooFar] : 0);
    }

    // 从最旧的文件 (start=0) 开始检查
    while (countOfFiles - start >= comConf.getMinFilesToCompact()
            // // 核心条件：当前文件大小 > (所有更新文件大小之和 * ratio)
        && fileSizes[start] > Math.max(comConf.getMinCompactSize(),
          (long) (sumSize[start + 1] * ratio))) {
      // 如果条件为真，说明当前文件太大，不合并。指针后移，检查下一个。
      ++start;
    }
    if (start < countOfFiles) {
      LOG.info("Default compaction algorithm has selected " + (countOfFiles - start)
          + " files from " + countOfFiles + " candidates");
    } else if (mayBeStuck) {
      // We may be stuck. Compact the latest files if we can.
      int filesToLeave = candidates.size() - comConf.getMinFilesToCompact();
      if (filesToLeave >= 0) {
        start = filesToLeave;
      }
    }
    candidates.subList(0, start).clear();
    return candidates;
  }

  /**
   * Overwrite min threshold for compaction
   * @param minThreshold
   */
  public void setMinThreshold(int minThreshold) {
    comConf.setMinFilesToCompact(minThreshold);
  }
}
