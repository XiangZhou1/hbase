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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Preconditions;

/**
 * A split policy determines when a region should be split.
 * @see IncreasingToUpperBoundRegionSplitPolicy Default split policy since
 *      0.94.0
 * @see ConstantSizeRegionSplitPolicy Default split policy before 0.94.0
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class RegionSplitPolicy extends Configured {
  private static final Class<? extends RegionSplitPolicy>
    DEFAULT_SPLIT_POLICY_CLASS = IncreasingToUpperBoundRegionSplitPolicy.class;

  /**
   * The region configured for this split policy.
   */
  protected HRegion region;

  /**
   * Upon construction, this method will be called with the region
   * to be governed. It will be called once and only once.
   */
  protected void configureForRegion(HRegion region) {
    Preconditions.checkState(
        this.region == null,
        "Policy already configured for region {}",
        this.region);

    this.region = region;
  }

  /**
   * @return true if the specified region should be split.
   */
  protected abstract boolean shouldSplit();

  /**
   * @return the key at which the region should be split, or null
   * if it cannot be split. This will only be called if shouldSplit
   * previously returned true.
   */
  protected byte[] getSplitPoint() {
    /**
     *     显式分裂点优先: 首先检查 Region 是否有一个手动设置的分裂点（region.getExplicitSplitPoint()）。
     *     这通常是管理员通过 shell 命令 split 'tableName', 'splitKey' 强制设置的。
     *     如果存在，直接返回这个最高优先级的键。
     */
    byte[] explicitSplitPoint = this.region.getExplicitSplitPoint();
    if (explicitSplitPoint != null) {
      return explicitSplitPoint;
    }
    Map<byte[], Store> stores = region.getStores();

    byte[] splitPointFromLargestStore = null;
    long largestStoreSize = 0;
    for (Store s : stores.values()) {
      /**
       *     ■ 最大 Store 的分裂点: 如果没有显式分裂点，策略会遍历 Region 内所有的 Store (即 Column Family)。
       *     ■ 它会找到其中尺寸最大 (size) 的那个 Store。
       *     ■ 然后，它会调用这个最大 Store 的 getSplitPoint() 方法，该方法通常会返回这个 Store 中所有 HFile 的中心键 (mid-key)。
       *     ■ 最终，最大 Store 的中心键被作为整个 Region 的分裂点。
       */
      byte[] splitPoint = s.getSplitPoint();
      long storeSize = s.getSize();
      if (splitPoint != null && largestStoreSize < storeSize) {
        splitPointFromLargestStore = splitPoint;
        largestStoreSize = storeSize;
      }
    }

    return splitPointFromLargestStore;
  }

  /**
   * Create the RegionSplitPolicy configured for the given table.
   * @param region
   * @param conf
   * @return a RegionSplitPolicy
   * @throws IOException
   */
  public static RegionSplitPolicy create(HRegion region,
      Configuration conf) throws IOException {
    Class<? extends RegionSplitPolicy> clazz = getSplitPolicyClass(
        region.getTableDesc(), conf);
    RegionSplitPolicy policy = ReflectionUtils.newInstance(clazz, conf);
    policy.configureForRegion(region);
    return policy;
  }

  public static Class<? extends RegionSplitPolicy> getSplitPolicyClass(
      HTableDescriptor htd, Configuration conf) throws IOException {
    String className = htd.getRegionSplitPolicyClassName();
    if (className == null) {
      className = conf.get(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
          DEFAULT_SPLIT_POLICY_CLASS.getName());
    }

    try {
      Class<? extends RegionSplitPolicy> clazz =
        Class.forName(className).asSubclass(RegionSplitPolicy.class);
      return clazz;
    } catch (Exception  e) {
      throw new IOException(
          "Unable to load configured region split policy '" +
          className + "' for table '" + htd.getTableName() + "'",
          e);
    }
  }

  /**
   * In {@link HRegionFileSystem#splitStoreFile(org.apache.hadoop.hbase.HRegionInfo, String,
   * StoreFile, byte[], boolean, RegionSplitPolicy)} we are not creating the split reference
   * if split row not lies in the StoreFile range. But in some use cases we may need to create
   * the split reference even when the split row not lies in the range. This method can be used
   * to decide, whether to skip the the StoreFile range check or not.
   * @return whether to skip the StoreFile range check or not
   * @deprecated Use {@link #skipStoreFileRangeCheck(String)}} instead
   */
  @Deprecated
  protected boolean skipStoreFileRangeCheck() {
    return false;
  }

  /**
   * See {@link #skipStoreFileRangeCheck()} javadoc.
   * @param familyName
   * @return whether to skip the StoreFile range check or not
   */
  protected boolean skipStoreFileRangeCheck(String familyName) {
    return skipStoreFileRangeCheck();
  }
}
