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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Handles processing region splits. Put in a queue, owned by HRegionServer.
 */
@InterfaceAudience.Private
class SplitRequest implements Runnable {
  static final Log LOG = LogFactory.getLog(SplitRequest.class);
  private final HRegion parent;
  private final byte[] midKey;
  private final HRegionServer server;
  private final User user;
  private TableLock tableLock;

  SplitRequest(HRegion region, byte[] midKey, HRegionServer hrs, User user) {
    Preconditions.checkNotNull(hrs);
    this.parent = region;
    this.midKey = midKey;
    this.server = hrs;
    this.user = user;
  }

  @Override
  public String toString() {
    return "regionName=" + parent + ", midKey=" + Bytes.toStringBinary(midKey);
  }

  private void doSplitting(User user) {
    boolean success = false;
    server.getMetrics().incrSplitRequest();
    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    SplitTransaction st = new SplitTransaction(parent, midKey);
    try {
      //acquire a shared read lock on the table, so that table schema modifications
      //do not happen concurrently
      tableLock = server.getTableLockManager().readLock(parent.getTableDesc().getTableName()
          , "SPLIT_REGION:" + parent.getRegionNameAsString());
      try {
        tableLock.acquire();
      } catch (IOException ex) {
        tableLock = null;
        throw ex;
      }

      // If prepare does not return true, for some reason -- logged inside in
      // the prepare call -- we are not ready to split just now. Just return.
      /**
       * ● 执行预准备 (st.prepare()):
       *   ○ 这是分裂事务的第一步，进行各种检查，确认是否可以进行分裂。例如：
       *     ■ 检查父 Region 是否在线、是否正在关闭。
       *     ■ 检查 midKey 是否合法（不能是 Region 的起始或结束 key）。
       *     ■ 检查 RegionServer 是否正常运行。
       *   ○ 如果 prepare() 返回 false，说明当前不适合分裂，SplitRequest 会直接返回，放弃本次分裂。
       */
      if (!st.prepare()) return;
      try {
        /**
         * ● 核心执行 (st.execute(...)):
         *   ○ 如果 prepare() 成功，就调用 execute() 方法，这是整个分裂过程的核心。SplitTransaction 在这一步会执行一系列复杂的操作，大致包括：
         *     ■ 在 HDFS 上为两个新的子 Region（Daughter A 和 Daughter B）创建目录。
         *     ■ 关闭父 Region 的写入，并将其刷写（flush）到磁盘，确保所有内存中的数据都已持久化。
         *     ■ 在父 Region 的 HDFS 目录下创建引用文件 (reference files)，这些文件指向父 Region 的 HFile。
         *               这样，新的子 Region 就不需要立即复制所有数据，而是可以共享父 Region 的数据文件，这使得分裂操作非常快。
         *     ■ 在 hbase:meta 表中执行一个原子性的“CAS”（Compare-And-Swap）操作：
         *       ● 将父 Region 的记录标记为“已分裂”。
         *       ● 同时，插入两条记录，代表两个新的子 Region。
         *       ● 这是整个分裂过程的**“不归点 (Point of No Return)”**。一旦 hbase:meta 表更新成功，分裂就被认为是完成了，
         *             之后即使发生故障，也不能简单地回滚，而是需要通过其他恢复机制（如 HBase Master 的修复工具）来处理。
         *     ■ 将两个新的子 Region 上线，并向 Master 报告分裂成功。
         * ● 标记成功: 如果 execute() 没有抛出异常，success 标记被设为 true。
         */
        st.execute(this.server, this.server, user);
        success = true;
      } catch (Exception e) {
        if (this.server.isStopping() || this.server.isStopped()) {
          LOG.info(
              "Skip rollback/cleanup of failed split of "
                  + parent.getRegionNameAsString() + " because server is"
                  + (this.server.isStopping() ? " stopping" : " stopped"), e);
          return;
        }
        if (e instanceof DroppedSnapshotException) {
          server.abort("Replay of WAL required. Forcing server shutdown", e);
          return;
        }
        try {
          LOG.info("Running rollback/cleanup of failed split of " +
            parent.getRegionNameAsString() + "; " + e.getMessage(), e);
          /**
           * doSplitting() 的 catch 块体现了 HBase 的健壮性设计。
           * ● 捕获异常: 如果 st.execute() 抛出异常，说明分裂失败。
           * ● 检查服务器状态: 首先检查 RegionServer 是否正在停止。如果是，就不再尝试回滚，因为整个服务器都要关闭了。
           * ● 特殊异常处理: DroppedSnapshotException 是一个致命错误，通常意味着 WAL（预写日志）重放失败，此时会直接中止（abort）整个 RegionServer。
           * ● 执行回滚 (st.rollback(...)):
           *   ○ 对于大多数普通失败，会尝试执行 rollback()。
           *   ○ rollback() 会尝试清理分裂过程中产生的“垃圾”，比如删除在 HDFS 上为子 Region 创建的空目录等。
           *   ○ 回滚成功: 日志记录成功信息。
           *   ○ 回滚失败: 如果在“不归点”之后发生故障，回滚本身也可能失败。这时情况非常严重，意味着可能在 HDFS 上留下了不一致的状态
           *              （比如 hbase:meta 表说分裂了，但 HDFS 上的文件结构不完整）。为了防止数据出现“空洞”，最安全的做法是中止（abort）整个 RegionServer。
           *               HBase Master 会检测到这个 RegionServer 宕机，并将其上的所有 Region 重新分配到其他健康的服务器上，进行数据恢复。
           */
          if (st.rollback(this.server, this.server)) {
            LOG.info("Successful rollback of failed split of " +
              parent.getRegionNameAsString());
          } else {
            this.server.abort("Abort; we got an error after point-of-no-return");
          }
        } catch (RuntimeException ee) {
          String msg = "Failed rollback of failed split of " +
            parent.getRegionNameAsString() + " -- aborting server";
          // If failed rollback, kill this server to avoid having a hole in table.
          LOG.info(msg, ee);
          this.server.abort(msg + " -- Cause: " + ee.getMessage());
        }
        return;
      }
    } catch (IOException ex) {
      LOG.error("Split failed " + this, RemoteExceptionHandler.checkIOException(ex));
      server.checkFileSystem();
    } finally {
      if (this.parent.getCoprocessorHost() != null) {
        try {
          this.parent.getCoprocessorHost().postCompleteSplit();
        } catch (IOException io) {
          LOG.error("Split failed " + this,
              RemoteExceptionHandler.checkIOException(io));
        }
      }
      if (parent.shouldForceSplit()) {
        parent.clearSplit();
      }
      releaseTableLock();
      long endTime = EnvironmentEdgeManager.currentTimeMillis();
      // Update regionserver metrics with the split transaction total running time
      server.getMetrics().updateSplitTime(endTime - startTime);
      if (success) {
        server.getMetrics().incrSplitSuccess();
        // Log success
        LOG.info("Region split, hbase:meta updated, and report to master. Parent="
            + parent.getRegionNameAsString() + ", new regions: "
            + st.getFirstDaughter().getRegionNameAsString() + ", "
            + st.getSecondDaughter().getRegionNameAsString() + ". Split took "
            + StringUtils.formatTimeDiff(EnvironmentEdgeManager.currentTimeMillis(), startTime));
      }
      // Always log the split transaction journal
      LOG.info("Split transaction journal:\n\t" + Strings.join("\n\t", st.getJournal()));
    }
  }

  @Override
  public void run() {
    if (this.server.isStopping() || this.server.isStopped()) {
      LOG.debug("Skipping split because server is stopping=" +
        this.server.isStopping() + " or stopped=" + this.server.isStopped());
      return;
    }
    doSplitting(user);
  }

  protected void releaseTableLock() {
    if (this.tableLock != null) {
      try {
        this.tableLock.release();
      } catch (IOException ex) {
        LOG.error("Could not release the table lock (something is really wrong). "
           + "Aborting this server to avoid holding the lock forever.");
        this.server.abort("Abort; we got an error when releasing the table lock "
                         + "on " + parent.getRegionNameAsString());
      }
    }
  }
}
