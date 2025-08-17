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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Compact region on request and then run split if appropriate
 */
@InterfaceAudience.Private
public class CompactSplitThread implements CompactionRequestor {
  static final Log LOG = LogFactory.getLog(CompactSplitThread.class);

  public static final String REGION_SERVER_REGION_SPLIT_LIMIT =
      "hbase.regionserver.regionSplitLimit";
  public static final int DEFAULT_REGION_SERVER_REGION_SPLIT_LIMIT= 1000;
  
  private final HRegionServer server;
  private final Configuration conf;

  // 用于执行“大型” Compaction 任务的线程池。
  private final ThreadPoolExecutor largeCompactions;
  // 用于执行“小型” Compaction 任务的线程池。
  private final ThreadPoolExecutor smallCompactions;
  // 用于执行 Region 分裂任务的线程池。
  private final ThreadPoolExecutor splits;
  // 用于执行 Region 合并任务的线程池。
  private final ThreadPoolExecutor mergePool;

  /**
   * 吞吐量控制器: 它持有一个 CompactionThroughputController 的实例，用于根据当前集群的负载情况，
   * 动态地限制 Compaction 操作所能使用的 I/O 吞吐量，防止后台任务对线上读写请求产生过大的影响。
   */
  private final CompactionThroughputController compactionThroughputController;

  /**
   * Splitting should not take place if the total number of regions exceed this.
   * This is not a hard limit to the number of regions but it is a guideline to
   * stop splitting after number of online regions is greater than this.
   */
  private int regionSplitLimit;

  /** @param server */
  CompactSplitThread(HRegionServer server) {
    super();
    this.server = server;
    this.conf = server.getConfiguration();
    this.regionSplitLimit = conf.getInt(REGION_SERVER_REGION_SPLIT_LIMIT,
        DEFAULT_REGION_SERVER_REGION_SPLIT_LIMIT);

    int largeThreads = Math.max(1, conf.getInt(
        "hbase.regionserver.thread.compaction.large", 1));
    int smallThreads = conf.getInt(
        "hbase.regionserver.thread.compaction.small", 1);

    int splitThreads = conf.getInt("hbase.regionserver.thread.split", 1);

    // if we have throttle threads, make sure the user also specified size
    Preconditions.checkArgument(largeThreads > 0 && smallThreads > 0);

    final String n = Thread.currentThread().getName();

    this.largeCompactions = new ThreadPoolExecutor(largeThreads, largeThreads,
        60, TimeUnit.SECONDS, new PriorityBlockingQueue<Runnable>(),
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(n + "-largeCompactions-" + System.currentTimeMillis());
            return t;
          }
      });
    this.largeCompactions.setRejectedExecutionHandler(new Rejection());
    this.smallCompactions = new ThreadPoolExecutor(smallThreads, smallThreads,
        60, TimeUnit.SECONDS, new PriorityBlockingQueue<Runnable>(),
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(n + "-smallCompactions-" + System.currentTimeMillis());
            return t;
          }
      });
    this.smallCompactions
        .setRejectedExecutionHandler(new Rejection());
    this.splits = (ThreadPoolExecutor)
        Executors.newFixedThreadPool(splitThreads,
            new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(n + "-splits-" + System.currentTimeMillis());
            return t;
          }
      });
    int mergeThreads = conf.getInt("hbase.regionserver.thread.merge", 1);
    this.mergePool = (ThreadPoolExecutor) Executors.newFixedThreadPool(
        mergeThreads, new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(n + "-merges-" + System.currentTimeMillis());
            return t;
          }
        });

    // compaction throughput controller
    this.compactionThroughputController =
        CompactionThroughputControllerFactory.create(server, conf);
  }

  @Override
  public String toString() {
    return "compaction_queue=("
        + largeCompactions.getQueue().size() + ":"
        + smallCompactions.getQueue().size() + ")"
        + ", split_queue=" + splits.getQueue().size()
        + ", merge_queue=" + mergePool.getQueue().size();
  }
  
  public String dumpQueue() {
    StringBuffer queueLists = new StringBuffer();
    queueLists.append("Compaction/Split Queue dump:\n");
    queueLists.append("  LargeCompation Queue:\n");
    BlockingQueue<Runnable> lq = largeCompactions.getQueue();
    Iterator<Runnable> it = lq.iterator();
    while (it.hasNext()) {
      queueLists.append("    " + it.next().toString());
      queueLists.append("\n");
    }

    if (smallCompactions != null) {
      queueLists.append("\n");
      queueLists.append("  SmallCompation Queue:\n");
      lq = smallCompactions.getQueue();
      it = lq.iterator();
      while (it.hasNext()) {
        queueLists.append("    " + it.next().toString());
        queueLists.append("\n");
      }
    }

    queueLists.append("\n");
    queueLists.append("  Split Queue:\n");
    lq = splits.getQueue();
    it = lq.iterator();
    while (it.hasNext()) {
      queueLists.append("    " + it.next().toString());
      queueLists.append("\n");
    }

    queueLists.append("\n");
    queueLists.append("  Region Merge Queue:\n");
    lq = mergePool.getQueue();
    it = lq.iterator();
    while (it.hasNext()) {
      queueLists.append("    " + it.next().toString());
      queueLists.append("\n");
    }

    return queueLists.toString();
  }

  public synchronized void requestRegionsMerge(final HRegion a,
      final HRegion b, final boolean forcible, long masterSystemTime, User user) {
    try {
      mergePool.execute(new RegionMergeRequest(a, b, this.server, forcible, masterSystemTime,user));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Region merge requested for " + a + "," + b + ", forcible="
            + forcible + ".  " + this);
      }
    } catch (RejectedExecutionException ree) {
      LOG.warn("Could not execute merge for " + a + "," + b + ", forcible="
          + forcible, ree);
    }
  }

  /**
   *   ○ 分裂条件检查: shouldSplitRegion()。检查当前 RegionServer 的总 Region 数量是否已经超过了 hbase.regionserver.regionSplitLimit。这是一个保护性配置，防止因无限分裂导致 Master 负载过高。
   *   ○ 优先级检查: 检查 Region 的 compactPriority。如果一个 Region 正处于高优先级的 Compaction 压力下（例如 HFile 数量过多），通常会优先进行 Compaction 而不是 Split。
   *   ○ 获取分裂点: r.checkSplit()。委托 HRegion 根据其 RegionSplitPolicy 计算最佳的分裂点（midKey）。
   *   ○ 提交任务: 如果找到了有效的分裂点，就创建一个 SplitRequest 对象，并将其提交到 splits 线程池中。
   * @param r
   * @return
   */
  public synchronized boolean requestSplit(final HRegion r) {
    // don't split regions that are blocking
    if (shouldSplitRegion() && r.getCompactPriority() >= Store.PRIORITY_USER) {
      byte[] midKey = r.checkSplit();
      if (midKey != null) {
        requestSplit(r, midKey);
        return true;
      }
    }
    return false;
  }

  public synchronized void requestSplit(final HRegion r, byte[] midKey) {
    requestSplit(r, midKey, null);
  }

  /*
   * The User parameter allows the split thread to assume the correct user identity
   */
  public synchronized void requestSplit(final HRegion r, byte[] midKey, User user) {
    if (midKey == null) {
      LOG.debug("Region " + r.getRegionNameAsString() +
        " not splittable because midkey=null");
      if (r.shouldForceSplit()) {
        r.clearSplit();
      }
      return;
    }
    try {
      this.splits.execute(new SplitRequest(r, midKey, this.server, user));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Split requested for " + r + ".  " + this);
      }
    } catch (RejectedExecutionException ree) {
      LOG.info("Could not execute split for " + r, ree);
    }
  }

  @Override
  public synchronized List<CompactionRequest> requestCompaction(final HRegion r, final String why)
      throws IOException {
    return requestCompaction(r, why, null);
  }

  @Override
  public synchronized List<CompactionRequest> requestCompaction(final HRegion r, final String why,
      List<Pair<CompactionRequest, Store>> requests) throws IOException {
    return requestCompaction(r, why, Store.NO_PRIORITY, requests, null);
  }

  @Override
  public synchronized CompactionRequest requestCompaction(final HRegion r, final Store s,
      final String why, CompactionRequest request) throws IOException {
    return requestCompaction(r, s, why, Store.NO_PRIORITY, request, null);
  }

  @Override
  public synchronized List<CompactionRequest> requestCompaction(final HRegion r, final String why,
      int p, List<Pair<CompactionRequest, Store>> requests, User user) throws IOException {
    return requestCompactionInternal(r, why, p, requests, true, user);
  }

  private List<CompactionRequest> requestCompactionInternal(final HRegion r, final String why,
      int p, List<Pair<CompactionRequest, Store>> requests, boolean selectNow, User user)
          throws IOException {
    // not a special compaction request, so make our own list
    List<CompactionRequest> ret = null;
    if (requests == null) {
      ret = selectNow ? new ArrayList<CompactionRequest>(r.getStores().size()) : null;
      for (Store s : r.getStores().values()) {
        CompactionRequest cr = requestCompactionInternal(r, s, why, p, null, selectNow, user);
        if (selectNow) ret.add(cr);
      }
    } else {
      Preconditions.checkArgument(selectNow); // only system requests have selectNow == false
      ret = new ArrayList<CompactionRequest>(requests.size());
      for (Pair<CompactionRequest, Store> pair : requests) {
        ret.add(requestCompaction(r, pair.getSecond(), why, p, pair.getFirst(), user));
      }
    }
    return ret;
  }

  public CompactionRequest requestCompaction(final HRegion r, final Store s,
      final String why, int priority, CompactionRequest request, User user) throws IOException {
    return requestCompactionInternal(r, s, why, priority, request, true, user);
  }

  public synchronized void requestSystemCompaction(
      final HRegion r, final String why) throws IOException {
    requestCompactionInternal(r, why, Store.NO_PRIORITY, null, false, null);
  }

  public void requestSystemCompaction(
      final HRegion r, final Store s, final String why) throws IOException {
    requestCompactionInternal(r, s, why, Store.NO_PRIORITY, null, false, null);
  }

  /**
   * @param r HRegion store belongs to
   * @param s Store to request compaction on
   * @param why Why compaction requested -- used in debug messages
   * @param priority override the default priority (NO_PRIORITY == decide)
   * @param request custom compaction request. Can be <tt>null</tt> in which case a simple
   *          compaction will be used.
   *
   *   ○ 检查：确认 RegionServer 是否正在运行，以及表的 Compaction 功能是否被禁用。
   *   ○ 文件选择 (selectNow)：
   *     ■ 对于系统自动触发的 Compaction，selectNow 为 false。请求只是一个“意图”，会被放入队列，具体的 HFile 选择会推迟到 CompactionRunner 真正执行时才进行。
   *     ■ 对于用户手动触发的 Compaction，selectNow 为 true。会立即调用 selectCompaction 来确定要合并哪些文件。
   *   ○ 选择线程池: 根据预计的 Compaction 大小，选择是放入 smallCompactions 还是 largeCompactions 线程池。这是一个简单的负载均衡策略。
   *   ○ 提交任务: 创建一个 CompactionRunner（一个 Runnable 任务）并提交到选定的线程池中异步执行。
   */
  private synchronized CompactionRequest requestCompactionInternal(final HRegion r, final Store s,
      final String why, int priority, CompactionRequest request, boolean selectNow, User user)
          throws IOException {
    if (this.server.isStopped()
        || (r.getTableDesc() != null && !r.getTableDesc().isCompactionEnabled())) {
      return null;
    }

    /**
     *   ○ 文件选择 (selectNow)：
     *     ■ 对于系统自动触发的 Compaction，selectNow 为 false。请求只是一个“意图”，会被放入队列，具体的 HFile 选择会推迟到 CompactionRunner 真正执行时才进行。
     *     ■ 对于用户手动触发的 Compaction，selectNow 为 true。会立即调用 selectCompaction 来确定要合并哪些文件。
     */
    CompactionContext compaction = null;
    if (selectNow) {
      compaction = selectCompaction(r, s, priority, request, user);
      if (compaction == null) return null; // message logged inside
    }

    // We assume that most compactions are small. So, put system compactions into small
    // pool; we will do selection there, and move to large pool if necessary.
    // 根据 compaction 的预计大小，选择合适的线程池
    // 大的 compaction 放入 largeCompactions 池，小的放入 smallCompactions 池
    ThreadPoolExecutor pool = (selectNow && s.throttleCompaction(compaction.getRequest().getSize()))
      ? largeCompactions : smallCompactions;
    // 提交任务: 创建一个 CompactionRunner（一个 Runnable 任务）并提交到选定的线程池中异步执行
    pool.execute(new CompactionRunner(s, r, compaction, pool, user));
    if (LOG.isDebugEnabled()) {
      String type = (pool == smallCompactions) ? "Small " : "Large ";
      LOG.debug(type + "Compaction requested: " + (selectNow ? compaction.toString() : "system")
          + (why != null && !why.isEmpty() ? "; Because: " + why : "") + "; " + this);
    }
    return selectNow ? compaction.getRequest() : null;
  }

  private CompactionContext selectCompaction(final HRegion r, final Store s,
      int priority, CompactionRequest request, User user) throws IOException {
    CompactionContext compaction = s.requestCompaction(priority, request, user);
    if (compaction == null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Not compacting " + r.getRegionNameAsString() +
            " because compaction request was cancelled");
      }
      return null;
    }
    assert compaction.hasSelection();
    if (priority != Store.NO_PRIORITY) {
      compaction.getRequest().setPriority(priority);
    }
    return compaction;
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    splits.shutdown();
    mergePool.shutdown();
    largeCompactions.shutdown();
    smallCompactions.shutdown();
  }

  private void waitFor(ThreadPoolExecutor t, String name) {
    boolean done = false;
    while (!done) {
      try {
        done = t.awaitTermination(60, TimeUnit.SECONDS);
        LOG.info("Waiting for " + name + " to finish...");
        if (!done) {
          t.shutdownNow();
        }
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted waiting for " + name + " to finish...");
      }
    }
  }

  void join() {
    waitFor(splits, "Split Thread");
    waitFor(mergePool, "Merge Thread");
    waitFor(largeCompactions, "Large Compaction Thread");
    waitFor(smallCompactions, "Small Compaction Thread");
  }

  /**
   * Returns the current size of the queue containing regions that are
   * processed.
   *
   * @return The current size of the regions queue.
   */
  public int getCompactionQueueSize() {
    return largeCompactions.getQueue().size() + smallCompactions.getQueue().size();
  }

  public int getLargeCompactionQueueSize() {
    return largeCompactions.getQueue().size();
  }


  public int getSmallCompactionQueueSize() {
    return smallCompactions.getQueue().size();
  }

  public int getSplitQueueSize() {
    return splits.getQueue().size();
  }

  /**
   * 检查当前 RegionServer 的 Region 总数是否小于 regionSplitLimit。
   * 如果接近或超过上限，则直接返回 false，不进行分裂。
   * @return
   */
  private boolean shouldSplitRegion() {
    if(server.getNumberOfOnlineRegions() > 0.9*regionSplitLimit) {
      LOG.warn("Total number of regions is approaching the upper limit " + regionSplitLimit + ". "
          + "Please consider taking a look at http://hbase.apache.org/book.html#ops.regionmgt");
    }
    return (regionSplitLimit > server.getNumberOfOnlineRegions());
  }

  /**
   * @return the regionSplitLimit
   */
  public int getRegionSplitLimit() {
    return this.regionSplitLimit;
  }

  private class CompactionRunner implements Runnable, Comparable<CompactionRunner> {
    private final Store store;
    private final HRegion region;
    private CompactionContext compaction;
    private int queuedPriority;
    private ThreadPoolExecutor parent;
    private User user;

    public CompactionRunner(Store store, HRegion region,
        CompactionContext compaction, ThreadPoolExecutor parent, User user) {
      super();
      this.store = store;
      this.region = region;
      this.compaction = compaction;
      this.queuedPriority = (this.compaction == null)
          ? store.getCompactPriority() : compaction.getRequest().getPriority();
      this.parent = parent;
      this.user = user;
    }

    @Override
    public String toString() {
      return (this.compaction != null) ? ("Request = " + compaction.getRequest())
          : ("Store = " + store.toString() + ", pri = " + queuedPriority);
    }

    private void doCompaction(User user) {
      // Common case - system compaction without a file selection. Select now.
      // Case 1: 延迟文件选择 (系统自动触发)
      if (this.compaction == null) {
        int oldPriority = this.queuedPriority;
        this.queuedPriority = this.store.getCompactPriority();
        if (this.queuedPriority > oldPriority) {
          // Store priority decreased while we were in queue (due to some other compaction?),
          // requeue with new priority to avoid blocking potential higher priorities.
          this.parent.execute(this);
          return;
        }
        try {
          // 在真正执行前，才调用 selectCompaction 来选择文件
          this.compaction = selectCompaction(this.region, this.store, queuedPriority, null, user);
        } catch (IOException ex) {
          LOG.error("Compaction selection failed " + this, ex);
          server.checkFileSystem();
          return;
        }
        if (this.compaction == null) return; // nothing to do
        // Now see if we are in correct pool for the size; if not, go to the correct one.
        // We might end up waiting for a while, so cancel the selection.
        assert this.compaction.hasSelection();
        ThreadPoolExecutor pool = store.throttleCompaction(
            compaction.getRequest().getSize()) ? largeCompactions : smallCompactions;
        if (this.parent != pool) {
          this.store.cancelRequestedCompaction(this.compaction);
          this.compaction = null;
          this.parent = pool;
          this.parent.execute(this);
          return;
        }
      }
      // Finally we can compact something.
      assert this.compaction != null;

      this.compaction.getRequest().beforeExecute();
      try {
        // Note: please don't put single-compaction logic here;
        //       put it into region/store/etc. This is CST logic.
        long start = EnvironmentEdgeManager.currentTimeMillis();
        // 核心调用：将任务委托给 HRegion 的 compact 方法
        boolean completed = region.compact(compaction, store, compactionThroughputController, user);
        long now = EnvironmentEdgeManager.currentTimeMillis();
        LOG.info(((completed) ? "Completed" : "Aborted") + " compaction: " +
              this + "; duration=" + StringUtils.formatTimeDiff(now, start));
        if (completed) {
          // degenerate case: blocked regions require recursive enqueues
          if (store.getCompactPriority() <= 0) {
            requestSystemCompaction(region, store, "Recursive enqueue");
          } else {
            // see if the compaction has caused us to exceed max region size
            requestSplit(region);
          }
        }
      } catch (IOException ex) {
        IOException remoteEx = RemoteExceptionHandler.checkIOException(ex);
        LOG.error("Compaction failed " + this, remoteEx);
        if (remoteEx != ex) {
          LOG.info("Compaction failed at original callstack: " + formatStackTrace(ex));
        }
        region.reportCompactionRequestFailure();
        server.checkFileSystem();
      } catch (Exception ex) {
        LOG.error("Compaction failed " + this, ex);
        region.reportCompactionRequestFailure();
        server.checkFileSystem();
      } finally {
        LOG.debug("CompactSplitThread Status: " + CompactSplitThread.this);
      }
      this.compaction.getRequest().afterExecute();
    }

    @Override
    public void run() {
      Preconditions.checkNotNull(server);
      if (server.isStopped()
          || (region.getTableDesc() != null && !region.getTableDesc().isCompactionEnabled())) {
        return;
      }
      doCompaction(user);
    }

    private String formatStackTrace(Exception ex) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      ex.printStackTrace(pw);
      pw.flush();
      return sw.toString();
    }

    @Override
    public int compareTo(CompactionRunner o) {
      // Only compare the underlying request (if any), for queue sorting purposes.
      int compareVal = queuedPriority - o.queuedPriority; // compare priority
      if (compareVal != 0) return compareVal;
      CompactionContext tc = this.compaction, oc = o.compaction;
      // Sort pre-selected (user?) compactions before system ones with equal priority.
      return (tc == null) ? ((oc == null) ? 0 : 1)
          : ((oc == null) ? -1 : tc.getRequest().compareTo(oc.getRequest()));
    }
  }

  /**
   * Cleanup class to use when rejecting a compaction request from the queue.
   */
  private static class Rejection implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable runnable, ThreadPoolExecutor pool) {
      if (runnable instanceof CompactionRunner) {
        CompactionRunner runner = (CompactionRunner)runnable;
        LOG.debug("Compaction Rejected: " + runner);
        runner.store.cancelRequestedCompaction(runner.compaction);
      }
    }
  }

  @VisibleForTesting
  public CompactionThroughputController getCompactionThroughputController() {
    return compactionThroughputController;
  }

}
