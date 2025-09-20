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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A memstore-local allocation buffer.
 * <p>
 * The MemStoreLAB is basically a bump-the-pointer allocator that allocates
 * big (2MB) byte[] chunks from and then doles it out to threads that request
 * slices into the array.
 * <p>
 * The purpose of this class is to combat heap fragmentation in the
 * regionserver. By ensuring that all KeyValues in a given memstore refer
 * only to large chunks of contiguous memory, we ensure that large blocks
 * get freed up when the memstore is flushed.
 * <p>
 * Without the MSLAB, the byte array allocated during insertion end up
 * interleaved throughout the heap, and the old generation gets progressively
 * more fragmented until a stop-the-world compacting collection occurs.
 * <p>
 * TODO: we should probably benchmark whether word-aligning the allocations
 * would provide a performance improvement - probably would speed up the
 * Bytes.toLong/Bytes.toInt calls in KeyValue, but some of those are cached
 * anyway
 *
 * MemStoreLAB (MemStore Local Allocation Buffer) 是 HBase 写入路径上一个至关重要的内存管理组件。
 * 它的核心思想是用一个“大块分配、小块切割”的策略来代替频繁的“小块分配”，以对抗 JVM 堆内存碎片化，并优化 GC 性能。
 *
 * 背景：为什么需要 MSLAB？
 * 在没有 MSLAB 的情况下，每次客户端写入一个 KeyValue，HBase 都需要为这个 KeyValue 对象本身以及其底层的 byte[]
 * 数组向 JVM 申请内存。在高并发写入场景下，这会导致：
 * ● 大量的、小而分散的内存分配请求：这会增加 JVM 内存管理的开销。
 * ● 堆内存碎片化 (Heap Fragmentation)：这些小 byte[] 数组在 JVM 堆中是随机散布的。当一个 MemStore 刷写到磁盘后，
 * 这些 KeyValue 对象和它们的 byte[] 数组就变成了垃圾。但由于它们是穿插在其他仍然存活的对象之间的，GC 很难回收出一整块大的连续空闲内存。随着时间推移，老年代（Old Generation）会变得越来越碎片化。
 * ● Full GC 风险增加：当碎片化严重到一定程度，即使总的空闲内存足够，也可能因为找不到一块足够大的连续空间来分配一个大对象，
 * 从而触发一次耗时很长的、需要整理内存的“Stop-The-World” Full GC。这对于要求低延迟的 HBase 来说是灾难性的。
 * MemStoreLAB 就是为了解决这些问题而设计的。
 *
 */
@InterfaceAudience.Private
public class MemStoreLAB {
  static final Log LOG = LogFactory.getLog(MemStoreLAB.class);
  // curChunk: 一个 AtomicReference<Chunk>。它原子性地指向当前正在用于分配的那个 Chunk。
  // 使用 AtomicReference 是为了在多线程并发写入时，能够无锁地、安全地进行 Chunk 的切换。
  private AtomicReference<Chunk> curChunk = new AtomicReference<Chunk>();
  // A queue of chunks contained by this memstore, used with chunk pool
  private BlockingQueue<Chunk> chunkQueue = null;

  final static String CHUNK_SIZE_KEY = "hbase.hregion.memstore.mslab.chunksize";
  final static int CHUNK_SIZE_DEFAULT = 2048 * 1024;
  final int chunkSize;

  final static String MAX_ALLOC_KEY = "hbase.hregion.memstore.mslab.max.allocation";
  final static int MAX_ALLOC_DEFAULT = 256  * 1024; // allocs bigger than this don't go through allocator
  final int maxAlloc;

  private final MemStoreChunkPool chunkPool;

  // This flag is for closing this instance, its set when clearing snapshot of
  // memstore
  // close(): 当 MemStore 的 snapshot 被清理时（即刷写完成且不再需要老数据时），会调用此方法。它会将 closed 标志置为 true。
  private volatile boolean closed = false;
  // This flag is for reclaiming chunks. Its set when putting chunks back to
  // pool
  private AtomicBoolean reclaimed = new AtomicBoolean(false);
  // Current count of open scanners which reading data from this MemStoreLAB
  /**
   * incScannerCount(): 当创建一个新的 Scanner 来读取 MemStore 的数据时，会调用此方法，将 openScannerCount 计数器加一。
   * decScannerCount(): 当一个 Scanner 关闭时，会调用此方法，将计数器减一。
   *
   * 安全回收: 在 decScannerCount() 和 close() 中都有一个关键的检查：if (count == 0 && this.closed)。
   * 只有当 MSLAB 已经被标记为 closed 并且 openScannerCount 降为 0 时，才会真正调用 chunkPool.putbackChunks()
   * 将 chunkQueue 中的所有 Chunk 归还给 Pool。这确保了正在被读取的内存不会被复用，避免了数据读取错误。
   */
  private final AtomicInteger openScannerCount = new AtomicInteger();

  // Used in testing
  public MemStoreLAB() {
    this(new Configuration());
  }

  private MemStoreLAB(Configuration conf) {
    this(conf, MemStoreChunkPool.getPool(conf));
  }

  public MemStoreLAB(Configuration conf, MemStoreChunkPool pool) {
    chunkSize = conf.getInt(CHUNK_SIZE_KEY, CHUNK_SIZE_DEFAULT);
    maxAlloc = conf.getInt(MAX_ALLOC_KEY, MAX_ALLOC_DEFAULT);
    this.chunkPool = pool;
    // currently chunkQueue is only used for chunkPool
    if (this.chunkPool != null) {
      // set queue length to chunk pool max count to avoid keeping reference of
      // too many non-reclaimable chunks
      chunkQueue = new LinkedBlockingQueue<Chunk>(chunkPool.getMaxCount());
    }

    // if we don't exclude allocations >CHUNK_SIZE, we'd infiniteloop on one!
    Preconditions.checkArgument(
      maxAlloc <= chunkSize,
      MAX_ALLOC_KEY + " must be less than " + CHUNK_SIZE_KEY);
  }

  /**
   * Allocate a slice of the given length.
   *
   * If the size is larger than the maximum size specified for this
   * allocator, returns null.
   * 是 MSLAB 提供给 MemStore 使用的核心方法。
   * ● 大小检查: 检查请求的 size 是否超过 maxAlloc。如果超过，直接返回 null。
   * ● 无限循环: 进入一个 while(true) 循环，不断尝试分配，直到成功。
   * ● 获取 Chunk: 调用 getOrMakeChunk() 获取当前可用的 Chunk。
   * ● 在 Chunk 内分配: 调用 c.alloc(size)，尝试在获取到的 Chunk c 中进行指针碰撞分配。
   * ● 分配成功: 如果 alloc() 返回一个有效的偏移量（!= -1），说明分配成功。
   *   ○ 创建一个 Allocation 对象，它包含了对 Chunk 的 byte[] 数组的引用和分配到的偏移量。
   *   ○ 返回这个 Allocation 对象，循环结束。
   * ● 空间不足: 如果 alloc() 返回 -1，说明当前 Chunk 空间不足。
   *   ○ 调用 tryRetireChunk(c)，尝试将这个用完的 Chunk c 从 curChunk 引用中移除（通过 CAS 操作将其置为 null）。
   *   ○ 循环继续，下一次迭代中的 getOrMakeChunk() 就会去获取一个新的 Chunk。
   */
  public Allocation allocateBytes(int size) {
    Preconditions.checkArgument(size >= 0, "negative size");

    // Callers should satisfy large allocations directly from JVM since they
    // don't cause fragmentation as badly.
    if (size > maxAlloc) {
      return null;
    }

    while (true) {
      Chunk c = getOrMakeChunk();

      // Try to allocate from this chunk
      int allocOffset = c.alloc(size);
      if (allocOffset != -1) {
        // We succeeded - this is the common case - small alloc
        // from a big buffer
        return new Allocation(c.data, allocOffset);
      }

      // not enough space!
      // try to retire this chunk
      tryRetireChunk(c);
    }
  }

  /**
   * Close this instance since it won't be used any more, try to put the chunks
   * back to pool
   */
  void close() {
    this.closed = true;
    // We could put back the chunks to pool for reusing only when there is no
    // opening scanner which will read their data
    if (chunkPool != null && openScannerCount.get() == 0
        && reclaimed.compareAndSet(false, true)) {
      chunkPool.putbackChunks(this.chunkQueue);
    }
  }

  /**
   * Called when opening a scanner on the data of this MemStoreLAB
   */
  void incScannerCount() {
    this.openScannerCount.incrementAndGet();
  }

  /**
   * Called when closing a scanner on the data of this MemStoreLAB
   */
  void decScannerCount() {
    int count = this.openScannerCount.decrementAndGet();
    if (chunkPool != null && count == 0 && this.closed
        && reclaimed.compareAndSet(false, true)) {
      chunkPool.putbackChunks(this.chunkQueue);
    }
  }

  /**
   * Try to retire the current chunk if it is still
   * <code>c</code>. Postcondition is that curChunk.get()
   * != c
   * @param c the chunk to retire
   * @return true if we won the race to retire the chunk
   */
  private void tryRetireChunk(Chunk c) {
    curChunk.compareAndSet(c, null);
    // If the CAS succeeds, that means that we won the race
    // to retire the chunk. We could use this opportunity to
    // update metrics on external fragmentation.
    //
    // If the CAS fails, that means that someone else already
    // retired the chunk for us.
  }

  /**
   * Get the current chunk, or, if there is no current chunk,
   * allocate a new one from the JVM.
   */
  private Chunk getOrMakeChunk() {
    while (true) {
      // Try to get the chunk
      Chunk c = curChunk.get();
      if (c != null) {
        return c;
      }

      // No current chunk, so we want to allocate one. We race
      // against other allocators to CAS in an uninitialized chunk
      // (which is cheap to allocate)
      c = (chunkPool != null) ? chunkPool.getChunk() : new Chunk(chunkSize);
      if (curChunk.compareAndSet(null, c)) {
        // we won race - now we need to actually do the expensive
        // allocation step
        c.init();
        if (chunkQueue != null && !this.closed && !this.chunkQueue.offer(c)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Chunk queue is full, won't reuse this new chunk. Current queue size: "
                + chunkQueue.size());
          }
        }
        return c;
      } else if (chunkPool != null) {
        chunkPool.putbackChunk(c);
      }
      // someone else won race - that's fine, we'll try to grab theirs
      // in the next iteration of the loop.
    }
  }

  @VisibleForTesting
  Chunk getCurrentChunk() {
    return this.curChunk.get();
  }

  @VisibleForTesting
  BlockingQueue<Chunk> getChunkQueue() {
    return this.chunkQueue;
  }

  /**
   * A chunk of memory out of which allocations are sliced.
   */
  static class Chunk {
    /** Actual underlying data */
    private byte[] data;

    private static final int UNINITIALIZED = -1;
    private static final int OOM = -2;
    /**
     * Offset for the next allocation, or the sentinel value -1
     * which implies that the chunk is still uninitialized.
     * */
    private AtomicInteger nextFreeOffset = new AtomicInteger(UNINITIALIZED);

    /** Total number of allocations satisfied from this buffer */
    private AtomicInteger allocCount = new AtomicInteger();

    /** Size of chunk in bytes */
    private final int size;

    /**
     * Create an uninitialized chunk. Note that memory is not allocated yet, so
     * this is cheap.
     * @param size in bytes
     */
    Chunk(int size) {
      this.size = size;
    }

    /**
     * Actually claim the memory for this chunk. This should only be called from
     * the thread that constructed the chunk. It is thread-safe against other
     * threads calling alloc(), who will block until the allocation is complete.
     */
    public void init() {
      assert nextFreeOffset.get() == UNINITIALIZED;
      try {
        if (data == null) {
          data = new byte[size];
        }
      } catch (OutOfMemoryError e) {
        boolean failInit = nextFreeOffset.compareAndSet(UNINITIALIZED, OOM);
        assert failInit; // should be true.
        throw e;
      }
      // Mark that it's ready for use
      boolean initted = nextFreeOffset.compareAndSet(
          UNINITIALIZED, 0);
      // We should always succeed the above CAS since only one thread
      // calls init()!
      Preconditions.checkState(initted,
          "Multiple threads tried to init same chunk");
    }

    /**
     * Reset the offset to UNINITIALIZED before before reusing an old chunk
     */
    void reset() {
      if (nextFreeOffset.get() != UNINITIALIZED) {
        nextFreeOffset.set(UNINITIALIZED);
        allocCount.set(0);
      }
    }

    /**
     * Try to allocate <code>size</code> bytes from the chunk.
     * @return the offset of the successful allocation, or -1 to indicate not-enough-space
     */
    public int alloc(int size) {
      while (true) {
        int oldOffset = nextFreeOffset.get();
        if (oldOffset == UNINITIALIZED) {
          // The chunk doesn't have its data allocated yet.
          // Since we found this in curChunk, we know that whoever
          // CAS-ed it there is allocating it right now. So spin-loop
          // shouldn't spin long!
          Thread.yield();
          continue;
        }
        if (oldOffset == OOM) {
          // doh we ran out of ram. return -1 to chuck this away.
          return -1;
        }

        if (oldOffset + size > data.length) {
          return -1; // alloc doesn't fit
        }

        // Try to atomically claim this chunk
        if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size)) {
          // we got the alloc
          allocCount.incrementAndGet();
          return oldOffset;
        }
        // we raced and lost alloc, try again
      }
    }

    @Override
    public String toString() {
      return "Chunk@" + System.identityHashCode(this) +
        " allocs=" + allocCount.get() + "waste=" +
        (data.length - nextFreeOffset.get());
    }

    @VisibleForTesting
    int getNextFreeOffset() {
      return this.nextFreeOffset.get();
    }
  }

  /**
   * The result of a single allocation. Contains the chunk that the
   * allocation points into, and the offset in this array where the
   * slice begins.
   */
  public static class Allocation {
    private final byte[] data;
    private final int offset;

    private Allocation(byte[] data, int off) {
      this.data = data;
      this.offset = off;
    }

    @Override
    public String toString() {
      return "Allocation(" + "capacity=" + data.length + ", off=" + offset
          + ")";
    }

    byte[] getData() {
      return data;
    }

    int getOffset() {
      return offset;
    }
  }
}
