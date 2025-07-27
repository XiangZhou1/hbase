/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Handles processing region merges. Put in a queue, owned by HRegionServer.
 */
@InterfaceAudience.Private
class RegionMergeRequest implements Runnable {
  static final Log LOG = LogFactory.getLog(RegionMergeRequest.class);
  private final HRegion region_a;
  private final HRegion region_b;
  private final HRegionServer server;
  private final boolean forcible;
  private TableLock tableLock;
  private final long masterSystemTime;
  private final User user;

  RegionMergeRequest(HRegion a, HRegion b, HRegionServer hrs, boolean forcible,
    long masterSystemTime, User user) {
    Preconditions.checkNotNull(hrs);
    this.region_a = a;
    this.region_b = b;
    this.server = hrs;
    this.forcible = forcible;
    this.masterSystemTime = masterSystemTime;
    this.user = user;
  }

  @Override
  public String toString() {
    return "MergeRequest,regions:" + region_a + ", " + region_b + ", forcible="
        + forcible;
  }

  @Override
  public void run() {
    if (this.server.isStopping() || this.server.isStopped()) {
      LOG.debug("Skipping merge because server is stopping="
          + this.server.isStopping() + " or stopped=" + this.server.isStopped());
      return;
    }
    try {
      final long startTime = EnvironmentEdgeManager.currentTimeMillis();
      RegionMergeTransaction mt = new RegionMergeTransaction(region_a,
          region_b, forcible, masterSystemTime);

      //acquire a shared read lock on the table, so that table schema modifications
      //do not happen concurrently
      tableLock = server.getTableLockManager().readLock(region_a.getTableDesc().getTableName()
          , "MERGE_REGIONS:" + region_a.getRegionNameAsString() + ", " + region_b.getRegionNameAsString());
      try {
        tableLock.acquire();
      } catch (IOException ex) {
        tableLock = null;
        throw ex;
      }

      // If prepare does not return true, for some reason -- logged inside in
      // the prepare call -- we are not ready to merge just now. Just return.
      /**
       * ● 执行预准备 (mt.prepare(...)):
       *   ○ 这是合并事务的第一步，进行各种先决条件检查，例如：
       *     ■ 检查两个 Region 是否都存在且在线。
       *     ■ 检查两个 Region 是否相邻 (region_a 的 endKey 是否等于 region_b 的 startKey)。
       *     ■ 检查两个 Region 是否属于同一张表。
       *     ■ 检查 RegionServer 是否正常运行。
       *   ○ 如果 prepare() 返回 false，说明当前不满足合并条件，RegionMergeRequest 会直接返回，放弃本次合并。
       */
      if (!mt.prepare(this.server)) return;
      try {
        /**
         *     ■ 在 HDFS 上为新的、合并后的 Region 创建一个临时目录。
         *     ■ 关闭要被合并的两个源 Region (region_a 和 region_b)。这个过程会将它们各自的 MemStore 刷写（flush）到磁盘，生成新的 HFile。
         *     ■ 将这两个源 Region 从 RegionServer 的在线服务列表中移除。
         *     ■ 将两个源 Region 的所有 HFile 移动 (move) 到新的合并后 Region 的目录下。这是一个纯粹的 HDFS 文件系统元数据操作，速度非常快，不涉及数据拷贝。
         *         ■ 在 hbase:meta 表中执行一个原子性的“CAS”（Compare-And-Swap）操作：
         *       ● 将 region_a 和 region_b 的记录标记为“已合并”。
         *       ● 同时，插入一条新的记录，代表合并后生成的新 Region。
         *       ● 这同样是整个合并过程的**“不归点 (Point of No Return)”**。一旦 hbase:meta 表更新成功，合并就被认为是完成了。
         *     ■ 将新的、合并后的 Region 上线，并向 Master 报告合并成功。
         *
         */
        mt.execute(this.server, this.server, this.user);
      } catch (Exception e) {
        if (this.server.isStopping() || this.server.isStopped()) {
          LOG.info(
              "Skip rollback/cleanup of failed merge of " + region_a + " and "
                  + region_b + " because server is"
                  + (this.server.isStopping() ? " stopping" : " stopped"), e);
          return;
        }
        if (e instanceof DroppedSnapshotException) {
          server.abort("Replay of WAL required. Forcing server shutdown", e);
          return;
        }
        try {
          LOG.warn("Running rollback/cleanup of failed merge of "
                  + region_a +" and "+ region_b + "; " + e.getMessage(), e);
          if (mt.rollback(this.server, this.server)) {
            LOG.info("Successful rollback of failed merge of "
                + region_a +" and "+ region_b);
          } else {
            this.server.abort("Abort; we got an error after point-of-no-return"
                + "when merging " + region_a + " and " + region_b);
          }
        } catch (RuntimeException ee) {
          String msg = "Failed rollback of failed merge of "
              + region_a +" and "+ region_b + " -- aborting server";
          // If failed rollback, kill this server to avoid having a hole in
          // table.
          LOG.info(msg, ee);
          this.server.abort(msg);
        }
        return;
      }
      LOG.info("Regions merged, hbase:meta updated, and report to master. region_a="
          + region_a + ", region_b=" + region_b + ",merged region="
          + mt.getMergedRegionInfo().getRegionNameAsString()
          + ". Region merge took "
          + StringUtils.formatTimeDiff(EnvironmentEdgeManager.currentTimeMillis(), startTime));
    } catch (IOException ex) {
      LOG.error("Merge failed " + this,
          RemoteExceptionHandler.checkIOException(ex));
      server.checkFileSystem();
    } finally {
      releaseTableLock();
    }
  }

  protected void releaseTableLock() {
    if (this.tableLock != null) {
      try {
        this.tableLock.release();
      } catch (IOException ex) {
        LOG.error("Could not release the table lock (something is really wrong). "
           + "Aborting this server to avoid holding the lock forever.");
        this.server.abort("Abort; we got an error when releasing the table lock "
                         + "on " + region_a.getRegionNameAsString());
      }
    }
  }
}
