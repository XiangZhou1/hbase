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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.util.Random;

/**
 * A {@link RegionSplitPolicy} implementation which splits a region
 * as soon as any of its store files exceeds a maximum configurable
 * size.
 * <p>
 * This is the default split policy. From 0.94.0 on the default split policy has
 * changed to {@link IncreasingToUpperBoundRegionSplitPolicy}
 * </p>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ConstantSizeRegionSplitPolicy extends RegionSplitPolicy {
  private static final Random RANDOM = new Random();

  private long desiredMaxFileSize;

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    Configuration conf = getConf();
    /**
     * ● 加载表级配置:
     *   ○ 首先，它会尝试从 HTableDescriptor (表的元数据定义) 中获取 max.file.size 这个属性。
     *          这意味着你可以在创建表的时候，为特定的表设置一个自定义的最大文件大小。
     *   ○ 例如：create 'my_table', 'cf', {MAX_FILESIZE => '134217728'} (设置为 128MB)。
     */
    HTableDescriptor desc = region.getTableDesc();
    if (desc != null) {
      this.desiredMaxFileSize = desc.getMaxFileSize();
    }
    /**
     * ● 加载全局配置:
     *   ○ 如果表级别没有设置 max.file.size（或者设置为0或负数），它会回退到加载 HBase 的全局配置文件 (hbase-site.xml) 中的 hbase.hregion.max.filesize 配置。
     *   ○ 如果全局配置也没有，它会使用一个默认值 HConstants.DEFAULT_MAX_FILE_SIZE (在早期版本通常是 256MB，后来逐渐增大到 10GB)。
     */
    if (this.desiredMaxFileSize <= 0) {
      this.desiredMaxFileSize = conf.getLong(HConstants.HREGION_MAX_FILESIZE,
        HConstants.DEFAULT_MAX_FILE_SIZE);
    }
    /**
     * ● 添加“抖动” (Jitter):
     *   ○ 这是一个非常重要的防止分裂风暴 (Split Storm) 的机制。
     *   ○ 它会检查配置 hbase.hregion.max.filesize.jitter (一个 0.0 到 1.0 之间的小数，比如 0.25)。
     *   ○ 如果配置了这个 jitter，它会在 desiredMaxFileSize 的基础上增加一个随机的、正负均可的扰动。
     *   ○ 为什么需要 Jitter？ 想象一下，一个集群中有大量 Region，它们的数据增长速度相似。
     *           如果没有 jitter，它们可能会在几乎同一时间达到分裂阈值，导致集群在短时间内同时发起大量的分裂操作。
     *           这会给 Master、ZooKeeper 和 HDFS 带来巨大压力，造成性能抖动，这就是“分裂风暴”。
     *   ○ 通过引入一个小的随机抖动，每个 Region 的实际分裂阈值会略有不同，从而将分裂操作在时间上错开 (stagger)，使得整个过程更加平滑。
     */
    float jitter = conf.getFloat("hbase.hregion.max.filesize.jitter", Float.NaN);
    if (!Float.isNaN(jitter)) {
      this.desiredMaxFileSize += (long)(desiredMaxFileSize * (RANDOM.nextFloat() - 0.5D) * jitter);
    }
  }

  @Override
  protected boolean shouldSplit() {
    /**
     * ● 检查强制分裂:
     *   ○ boolean force = region.shouldForceSplit();
     *   ○ 首先检查 Region 是否被标记为“强制分裂”。
     *      管理员可以通过 HBase Shell 或 API 手动触发一个 Region 的分裂，这时这个标记就会被设置。
     *      如果需要强制分裂，那么无论大小如何，都应该分裂。
     */
    boolean force = region.shouldForceSplit();
    boolean foundABigStore = false;

    /**
     * ● 遍历所有 Store:
     *   ○ HBase 的一个 Region 由一个或多个 Store 组成，每个 Store 对应一个列族 (Column Family)。
     *   ○ 它会遍历这个 Region 内部所有的 Store。
     */
    for (Store store : region.getStores().values()) {
      // If any of the stores are unable to split (eg they contain reference files)
      // then don't split
      /**
       *   ○ 它会询问每个 Store 是否“可以分裂”。一个 Store 可能因为包含了引用文件 (reference files) 而暂时不能分裂。
       *       引用文件是上一次分裂操作的产物，代表这个 Store 的数据文件实际上是其父 Region 的一部分。
       *       在这些引用文件通过 Compaction（合并）被清理掉之前，再次分裂这个 Store 是不安全的。
       *   ○ 只要有一个 Store 不能分裂，整个 Region 就不能分裂，方法立即返回 false。
       */
      if ((!store.canSplit())) {
        return false;
      }

      /**
       *   ○ 这里 store.getSize() 不是获取这个 Store（列族）下所有 HFile 的总大小，而是获取最大的那个 HFile 的大小。
       *   ○ 它会检查这个最大的 HFile 是否超过了之前配置的 desiredMaxFileSize（已经包含了 jitter）。
       *   ○ 只要找到一个 Store 中有足够大的 HFile，就把 foundABigStore 标记为 true。
       */
      // Mark if any store is big enough
      if (store.getSize() > desiredMaxFileSize) {
        foundABigStore = true;
      }
    }
    /**
     *   ○ 最后，只要满足以下两个条件之一，就返回 true（表示“应该分裂”）：
     *     ■ 找到了一个足够大的 StoreFile (foundABigStore 为 true)。
     *     ■ 或者，收到了强制分裂的指令 (force 为 true)。
     */
    return foundABigStore || force;
  }

  long getDesiredMaxFileSize() {
    return desiredMaxFileSize;
  }
}
