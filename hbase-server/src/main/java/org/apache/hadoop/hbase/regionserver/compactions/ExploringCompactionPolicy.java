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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;

/**
 * Class to pick which files if any to compact together.
 *
 * This class will search all possibilities for different and if it gets stuck it will choose
 * the smallest set of files to compact.
 * ExploringCompactionPolicy 则不同，它会探索（遍历）所有可能的、连续的 HFile 组合，并从中找出一个“最佳”的合并方案。
 */
@InterfaceAudience.Private
public class ExploringCompactionPolicy extends RatioBasedCompactionPolicy {
  private static final Log LOG = LogFactory.getLog(ExploringCompactionPolicy.class);

  /**
   * Constructor for ExploringCompactionPolicy.
   * @param conf The configuration object
   * @param storeConfigInfo An object to provide info about the store.
   */
  public ExploringCompactionPolicy(final Configuration conf,
                                   final StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  @Override
  protected final ArrayList<StoreFile> applyCompactionPolicy(final ArrayList<StoreFile> candidates,
    final boolean mayUseOffPeak, final boolean mightBeStuck) throws IOException {
    return new ArrayList<StoreFile>(applyCompactionPolicy(candidates, mightBeStuck,
        mayUseOffPeak, comConf.getMinFilesToCompact(), comConf.getMaxFilesToCompact()));
  }

  public List<StoreFile> applyCompactionPolicy(final List<StoreFile> candidates,
       boolean mightBeStuck, boolean mayUseOffPeak, int minFiles, int maxFiles) {

    final double currentRatio = mayUseOffPeak
        ? comConf.getCompactionRatioOffPeak() : comConf.getCompactionRatio();

    // Start off choosing nothing.
    List<StoreFile> bestSelection = new ArrayList<StoreFile>(0);
    List<StoreFile> smallest = mightBeStuck ? new ArrayList<StoreFile>(0) : null;
    long bestSize = 0;
    long smallestSize = Long.MAX_VALUE;

    int opts = 0, optsInRatio = 0, bestStart = -1; // for debug logging
    // Consider every starting place.
    /**
     * ● 双重循环遍历:
     *   ○ 外层循环 (for start ...): 遍历所有 HFile，将每一个文件作为潜在合并区间的起点。
     *   ○ 内层循环 (for currentEnd ...): 从 start 开始，不断向后扩展，将每一个文件作为潜在合并区间的终点。
     *   ○ 通过这两个循环，potentialMatchFiles = candidates.subList(start, currentEnd + 1) 就生成了一个所有可能的、连续的 HFile 组合。
     */
    for (int start = 0; start < candidates.size(); start++) {
      /**
       * ● 评估每个潜在组合 (potentialMatchFiles):
       *   ○ 合法性检查:
       *     ■ 组合中的文件数必须在 minFiles 和 maxFiles 之间。
       *     ■ 组合的总大小不能超过 maxCompactSize。
       *   ○ 存储最小组合: 如果 Store 可能卡住，并且当前组合的总大小比已知的 smallestSize 还小，则更新 smallest 和 smallestSize。
       *   ○ Ratio 检查 (filesInRatio): 这是一个关键的筛选条件。一个组合只有在其内部所有文件都满足 Ratio 约束时，才被认为是“健康的”。filesInRatio 会检查组合中的每一个文件，确保它的FileSize <= (组合总大小 - 该文件大小) * ratio。这个检查防止了将一个“巨无霸”文件和一堆“小不点”文件合并在一起的低效情况。
       *   ○ 比较与选择 (isBetterSelection): 如果一个组合通过了上述所有检查，就调用 isBetterSelection 将它与当前已知的 bestSelection 进行比较，以决定是否更新最佳选择。
       */
      // Consider every different sub list permutation in between start and end with min files.
      for (int currentEnd = start + minFiles - 1;
          currentEnd < candidates.size(); currentEnd++) {
        List<StoreFile> potentialMatchFiles = candidates.subList(start, currentEnd + 1);

        // Sanity checks
        if (potentialMatchFiles.size() < minFiles) {
          continue;
        }
        if (potentialMatchFiles.size() > maxFiles) {
          continue;
        }

        // Compute the total size of files that will
        // have to be read if this set of files is compacted.
        long size = getTotalStoreSize(potentialMatchFiles);

        // Store the smallest set of files.  This stored set of files will be used
        // if it looks like the algorithm is stuck.
        if (mightBeStuck && size < smallestSize) {
          smallest = potentialMatchFiles;
          smallestSize = size;
        }

        if (size > comConf.getMaxCompactSize()) {
          continue;
        }

        ++opts;
        if (size >= comConf.getMinCompactSize()
            && !filesInRatio(potentialMatchFiles, currentRatio)) {
          continue;
        }

        ++optsInRatio;
        if (isBetterSelection(bestSelection, bestSize, potentialMatchFiles, size, mightBeStuck)) {
          bestSelection = potentialMatchFiles;
          bestSize = size;
          bestStart = start;
        }
      }
    }
    if (bestSelection.size() == 0 && mightBeStuck) {
      LOG.debug("Exploring compaction algorithm has selected " + smallest.size()
          + " files of size "+ smallestSize + " because the store might be stuck");
      return new ArrayList<StoreFile>(smallest);
    }
    LOG.debug("Exploring compaction algorithm has selected " + bestSelection.size()
        + " files of size " + bestSize + " starting at candidate #" + bestStart +
        " after considering " + opts + " permutations with " + optsInRatio + " in ratio");
    return new ArrayList<StoreFile>(bestSelection);
  }

  private boolean isBetterSelection(List<StoreFile> bestSelection,
      long bestSize, List<StoreFile> selection, long size, boolean mightBeStuck) {
    if (mightBeStuck && bestSize > 0 && size > 0) {
      // Keep the selection that removes most files for least size. That penaltizes adding
      // large files to compaction, but not small files, so we don't become totally inefficient
      // (might want to tweak that in future). Also, given the current order of looking at
      // permutations, prefer earlier files and smaller selection if the difference is small.
      final double REPLACE_IF_BETTER_BY = 1.05;
      double thresholdQuality = ((double)bestSelection.size() / bestSize) * REPLACE_IF_BETTER_BY;
      return thresholdQuality < ((double)selection.size() / size);
    }
    // Keep if this gets rid of more files.  Or the same number of files for less io.
    return selection.size() > bestSelection.size()
      || (selection.size() == bestSelection.size() && size < bestSize);
  }

  /**
   * Find the total size of a list of store files.
   * @param potentialMatchFiles StoreFile list.
   * @return Sum of StoreFile.getReader().length();
   */
  private long getTotalStoreSize(final List<StoreFile> potentialMatchFiles) {
    long size = 0;

    for (StoreFile s:potentialMatchFiles) {
      size += s.getReader().length();
    }
    return size;
  }

  /**
   * Check that all files satisfy the constraint
   *      FileSize(i) <= ( Sum(0,N,FileSize(_)) - FileSize(i) ) * Ratio.
   *
   * @param files List of store files to consider as a compaction candidate.
   * @param currentRatio The ratio to use.
   * @return a boolean if these files satisfy the ratio constraints.
   */
  private boolean filesInRatio(final List<StoreFile> files, final double currentRatio) {
    if (files.size() < 2) {
      return true;
    }

    long totalFileSize = getTotalStoreSize(files);

    for (StoreFile file : files) {
      long singleFileSize = file.getReader().length();
      long sumAllOtherFileSizes = totalFileSize - singleFileSize;

      if (singleFileSize > sumAllOtherFileSizes * currentRatio) {
        return false;
      }
    }
    return true;
  }
}
