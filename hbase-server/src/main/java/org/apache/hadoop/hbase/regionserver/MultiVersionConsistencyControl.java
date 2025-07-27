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

import java.util.LinkedList;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Manages the read/write consistency within memstore. This provides
 * an interface for readers to determine what entries to ignore, and
 * a mechanism for writers to obtain new write numbers, then "commit"
 * the new writes for readers to read (thus forming atomic transactions).
 */
@InterfaceAudience.Private
public class MultiVersionConsistencyControl {
  private volatile long memstoreRead = 0;
  private volatile long memstoreWrite = 0;

  private final Object readWaiters = new Object();

  // This is the pending queue of writes.
  private final LinkedList<WriteEntry> writeQueue =
      new LinkedList<WriteEntry>();

  /**
   * Default constructor. Initializes the memstoreRead/Write points to 0.
   */
  public MultiVersionConsistencyControl() {
    this.memstoreRead = this.memstoreWrite = 0;
  }

  /**
   * Initializes the memstoreRead/Write points appropriately.
   * @param startPoint
   */
  public void initialize(long startPoint) {
    synchronized (writeQueue) {
      if (this.memstoreWrite != this.memstoreRead) {
        throw new RuntimeException("Already used this mvcc. Too late to initialize");
      }

      this.memstoreRead = this.memstoreWrite = startPoint;
    }
  }

  /**
   * Generate and return a {@link WriteEntry} with a new write number.
   * To complete the WriteEntry and wait for it to be visible,
   * call {@link #completeMemstoreInsert(WriteEntry)}.
   *
   * beginMemstoreInsert() - 开始写入 / 获取票据
   * ● 时机: 在 HRegion 准备向 MemStore 写入数据之前调用。
   * ● 逻辑:
   *   ○ 在 writeQueue 上加锁，保证序号分配和入队的原子性。
   *   ○ 将 memstoreWrite 指针加一 (++memstoreWrite)，得到一个新的、唯一的 nextWriteNumber。
   *   ○ 创建一个新的 WriteEntry 对象，封装这个 nextWriteNumber。
   *   ○ 将这个新的 WriteEntry 添加到 writeQueue 的队尾。
   *   ○ 返回这个 WriteEntry 对象给调用者。
   * ● 效果: 调用者拿到了一个代表本次写操作的“票据”（WriteEntry），但此时这次写入对读者来说是不可见的，因为它还没有被提交。
   */
  public WriteEntry beginMemstoreInsert() {
    synchronized (writeQueue) {
      long nextWriteNumber = ++memstoreWrite;
      WriteEntry e = new WriteEntry(nextWriteNumber);
      writeQueue.add(e);
      return e;
    }
  }

  /**
   * Complete a {@link WriteEntry} that was created by {@link #beginMemstoreInsert()}.
   *
   * At the end of this call, the global read point is at least as large as the write point
   * of the passed in WriteEntry.  Thus, the write is visible to MVCC readers.
   *   ○ advanceMemstore(e) - 推进读点
   *     ■ 在 writeQueue 上加锁。
   *     ■ 调用 e.markCompleted()，将传入的 WriteEntry 标记为已完成。
   *     ■ 核心逻辑 - 推进 memstoreRead:
   *       ● 从 writeQueue 的队头开始检查。
   *       ● 如果队头的 WriteEntry 是 completed 状态，就说明这个最老的、正在进行的写操作已经完成了。
   *       ● 将 memstoreRead 更新为这个已完成的 WriteEntry 的 writeNumber。
   *       ● 将这个已完成的 WriteEntry 从队列中移除。
   *       ● 继续检查新的队头，只要新的队头也是 completed 状态，就重复上述过程。
   *       ● 这个循环会一直进行，直到遇到一个尚未 completed 的 WriteEntry，或者队列为空。
   */
  public void completeMemstoreInsert(WriteEntry e) {
    advanceMemstore(e);
    waitForRead(e);
  }

  /**
   * Mark the {@link WriteEntry} as complete and advance the read point as
   * much as possible.
   *
   * How much is the read point advanced?
   * Let S be the set of all write numbers that are completed and where all previous write numbers
   * are also completed.  Then, the read point is advanced to the supremum of S.
   *
   * @param e
   * @return true if e is visible to MVCC readers (that is, readpoint >= e.writeNumber)
   *
   *   ○ advanceMemstore(e) - 推进读点
   *     ■ 在 writeQueue 上加锁。
   *     ■ 调用 e.markCompleted()，将传入的 WriteEntry 标记为已完成。
   *     ■ 核心逻辑 - 推进 memstoreRead:
   *       ● 从 writeQueue 的队头开始检查。
   *       ● 如果队头的 WriteEntry 是 completed 状态，就说明这个最老的、正在进行的写操作已经完成了。
   *       ● 将 memstoreRead 更新为这个已完成的 WriteEntry 的 writeNumber。
   *       ● 将这个已完成的 WriteEntry 从队列中移除。
   *       ● 继续检查新的队头，只要新的队头也是 completed 状态，就重复上述过程。
   *       ● 这个循环会一直进行，直到遇到一个尚未 completed 的 WriteEntry，或者队列为空。
   *     ■ 唤醒等待者:
   *       ● 在 readWaiters 对象上加锁。
   *       ● 更新全局的 memstoreRead 变量。
   *       ● 调用 readWaiters.notifyAll()，唤醒所有可能在 waitForRead 中等待的写线程。
   */
  boolean advanceMemstore(WriteEntry e) {
    synchronized (writeQueue) {
      e.markCompleted();

      long nextReadValue = -1;
      boolean ranOnce=false;
      while (!writeQueue.isEmpty()) {
        ranOnce=true;
        WriteEntry queueFirst = writeQueue.getFirst();

        if (nextReadValue > 0) {
          if (nextReadValue+1 != queueFirst.getWriteNumber()) {
            throw new RuntimeException("invariant in completeMemstoreInsert violated, prev: "
                + nextReadValue + " next: " + queueFirst.getWriteNumber());
          }
        }

        if (queueFirst.isCompleted()) {
          nextReadValue = queueFirst.getWriteNumber();
          writeQueue.removeFirst();
        } else {
          break;
        }
      }

      if (!ranOnce) {
        throw new RuntimeException("never was a first");
      }

      if (nextReadValue > 0) {
        synchronized (readWaiters) {
          memstoreRead = nextReadValue;
          readWaiters.notifyAll();
        }
      }
      if (memstoreRead >= e.getWriteNumber()) {
        return true;
      }
      return false;
    }
  }

  /**
   * Wait for the global readPoint to advance upto
   * the specified transaction number.
   *   ○ waitForRead(e) - 等待可见
   *     ■ 在一个 while 循环中检查 memstoreRead < e.getWriteNumber()。
   *     ■ 如果当前全局的读点还没有越过自己这次写入的 writeNumber，说明自己的写入对读者还不可见。
   *     ■ 调用 readWaiters.wait(0)，使当前写线程进入等待状态，直到被 advanceMemstore 中的 notifyAll() 唤醒。
   *     ■ 被唤醒后，重新检查循环条件。直到 memstoreRead >= e.getWriteNumber()，循环才会结束。
   */
  public void waitForRead(WriteEntry e) {
    boolean interrupted = false;
    synchronized (readWaiters) {
      while (memstoreRead < e.getWriteNumber()) {
        try {
          readWaiters.wait(0);
        } catch (InterruptedException ie) {
          // We were interrupted... finish the loop -- i.e. cleanup --and then
          // on our way out, reset the interrupt flag.
          interrupted = true;
        }
      }
    }
    if (interrupted) Thread.currentThread().interrupt();
  }

  public long memstoreReadPoint() {
    return memstoreRead;
  }


  public static class WriteEntry {
    private long writeNumber;
    private boolean completed = false;
    WriteEntry(long writeNumber) {
      this.writeNumber = writeNumber;
    }
    void markCompleted() {
      this.completed = true;
    }
    boolean isCompleted() {
      return this.completed;
    }
    long getWriteNumber() {
      return this.writeNumber;
    }
  }

  public static final long FIXED_SIZE = ClassSize.align(
      ClassSize.OBJECT +
      2 * Bytes.SIZEOF_LONG +
      2 * ClassSize.REFERENCE);

}
