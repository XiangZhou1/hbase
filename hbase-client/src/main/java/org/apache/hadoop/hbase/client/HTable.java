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
 *
 * HTable 用于与单个HBase表进行通信。实现了 HTableInterface 接口。
 * 鼓励用户通过 HConnection 和 HConnectionManager 获取实例，而不是直接构造此类的实例。
 * 有关示例，请参见 HConnectionManager 类注释。
 *
 * 对于读写操作，此类不是线程安全的。
 *
 * 在写入（Puts）的情况下，如果多个线程争用单个HTable实例，则基础写缓冲区可能会被破坏。
 *
 * 在读取的情况下，Scan使用的某些字段在所有线程之间共享。
 * HTable实现可以不保证在Get的情况下是安全的。
 *
 * 传递相同Configuration实例的HTable实例将共享到集群中服务器和zookeeper集合的连接以及区域位置的缓存。
 * 这通常是一件好事，建议为所有表重用相同的配置对象。
 * 这是因为它们都将共享相同的基础HConnection实例。有关此机制如何工作的更多信息，请参见HConnectionManager。
 *
 * HConnection将在初始构造时从传递的Configuration中读取其所需的大多数配置。
 * 此后，对于诸如hbase.client.pause，hbase.client.retries.number和hbase.client.rpc.maxattempts之类的设置，
 * 在HConnection构造后更新传递的Configuration中的值将不会被注意到。
 * 要使用更改后的值运行，请创建一个新的HTable，并传递具有新配置的新Configuration实例。
 *
 * 请注意，此类实现了Closeable接口。当不再需要HTable实例时，应将其关闭，以确保及时释放基础资源。
 * 请注意，close方法可能会引发必须处理的java.io.IOException。
 *
 * @see HBaseAdmin 用于创建，删除，列出，启用和禁用表。
 * @see HConnection
 * @see HConnectionManager
 */
package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RegionCoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.CompareType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Threads;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * <p>Used to communicate with a single HBase table.  An implementation of
 * {@link HTableInterface}.  Instances of this class can be constructed directly but it is
 * encouraged that users get instances via {@link HConnection} and {@link HConnectionManager}.
 * See {@link HConnectionManager} class comment for an example.
 *
 * <p>This class is not thread safe for reads nor write.
 *
 * <p>In case of writes (<code>Put</code>s), the underlying write buffer can
 * be corrupted if multiple threads contend over a single HTable instance.
 *
 * <p>In case of reads, some fields used by a Scan are shared among all threads.
 * The HTable implementation can either not contract to be safe in case of a Get
 *
 * <p>Instances of HTable passed the same {@link Configuration} instance will
 * share connections to servers out on the cluster and to the zookeeper ensemble
 * as well as caches of region locations.  This is usually a *good* thing and it
 * is recommended to reuse the same configuration object for all your tables.
 * This happens because they will all share the same underlying
 * {@link HConnection} instance. See {@link HConnectionManager} for more on
 * how this mechanism works.
 *
 * <p>{@link HConnection} will read most of the
 * configuration it needs from the passed {@link Configuration} on initial
 * construction.  Thereafter, for settings such as
 * <code>hbase.client.pause</code>, <code>hbase.client.retries.number</code>,
 * and <code>hbase.client.rpc.maxattempts</code> updating their values in the
 * passed {@link Configuration} subsequent to {@link HConnection} construction
 * will go unnoticed.  To run with changed values, make a new
 * {@link HTable} passing a new {@link Configuration} instance that has the
 * new configuration.
 *
 * <p>Note that this class implements the {@link Closeable} interface. When a
 * HTable instance is no longer required, it *should* be closed in order to ensure
 * that the underlying resources are promptly released. Please note that the close
 * method can throw java.io.IOException that must be handled.
 *
 * @see HBaseAdmin for create, drop, list, enable and disable of tables.
 * @see HConnection
 * @see HConnectionManager
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HTable implements HTableInterface {
  private static final Log LOG = LogFactory.getLog(HTable.class);
  /** HBase连接 */
  protected HConnection connection;
  /** 表名 */
  private final TableName tableName;
  /** 配置 */
  private volatile Configuration configuration;
  /** 表配置 */
  private TableConfiguration tableConfiguration;
  /** 异步写缓冲区 */
  protected List<Row> writeAsyncBuffer = new LinkedList<Row>();
  /** 写缓冲区大小 */
  private long writeBufferSize;
  /** 失败时是否清除缓冲区 */
  private boolean clearBufferOnFail;
  /** 是否自动刷新 */
  private boolean autoFlush;
  /** 当前写缓冲区大小 */
  protected long currentWriteBufferSize;
  /** 扫描器缓存 */
  protected int scannerCaching;
  /** 扫描器最大结果大小 */
  protected long scannerMaxResultSize;
  /** 用于Multi操作的线程池 */
  private ExecutorService pool;
  /** 是否已关闭 */
  private boolean closed;
  /** 每个阻塞方法的全局超时时间（带重试rpc） */
  private int operationTimeout;
  /** 每个rpc请求的超时时间 */
  private int rpcTimeout;
  /** 关闭时是否清理线程池 */
  private final boolean cleanupPoolOnClose;
  /** 关闭时是否清理连接 */
  private final boolean cleanupConnectionOnClose;

  /** 用于autoflush设置为false的put或multiput的异步进程 */
  protected AsyncProcess<Object> ap;
  /** RPC重试调用程序工厂 */
  private RpcRetryingCallerFactory rpcCallerFactory;
  /** RPC控制器工厂 */
  private RpcControllerFactory rpcControllerFactory;

  /**
   * 创建一个对象以访问HBase表。
   * 与使用相同 <code>conf</code> 实例创建的其他HTable实例共享zookeeper连接和其他资源。
   * 如果有可用的已填充区域缓存，则使用该缓存，该缓存由共享此 <code>conf</code> 实例的任何其他HTable实例填充。
   * 推荐使用。
   * @param conf 要使用的配置对象。
   * @param tableName 表的名称。
   * @throws IOException 如果发生远程或网络异常
   */
  public HTable(Configuration conf, final String tableName)
  throws IOException {
    this(conf, TableName.valueOf(tableName));
  }

  /**
   * 创建一个对象以访问HBase表。
   * 与使用相同 <code>conf</code> 实例创建的其他HTable实例共享zookeeper连接和其他资源。
   * 如果有可用的已填充区域缓存，则使用该缓存，该缓存由共享此 <code>conf</code> 实例的任何其他HTable实例填充。
   * 推荐使用。
   * @param conf 要使用的配置对象。
   * @param tableName 表的名称。
   * @throws IOException 如果发生远程或网络异常
   */
  public HTable(Configuration conf, final byte[] tableName)
  throws IOException {
    this(conf, TableName.valueOf(tableName));
  }



  /**
   * 创建一个对象以访问HBase表。
   * 与使用相同 <code>conf</code> 实例创建的其他HTable实例共享zookeeper连接和其他资源。
   * 如果有可用的已填充区域缓存，则使用该缓存，该缓存由共享此 <code>conf</code> 实例的任何其他HTable实例填充。
   * 推荐使用。
   * @param conf 要使用的配置对象。
   * @param tableName 表名POJO
   * @throws IOException 如果发生远程或网络异常
   */
  public HTable(Configuration conf, final TableName tableName)
  throws IOException {
    this.tableName = tableName;
    this.cleanupPoolOnClose = this.cleanupConnectionOnClose = true;
    if (conf == null) {
      this.connection = null;
      return;
    }
    this.connection = HConnectionManager.getConnection(conf);
    this.configuration = conf;

    this.pool = getDefaultExecutor(conf);
    this.finishSetup();
  }

  /**
   * 创建一个对象以访问HBase表。与使用相同 <code>connection</code> 实例创建的其他HTable实例共享zookeeper连接和其他资源。
   * 当HConnection实例由外部管理时，请使用此构造函数。
   * @param tableName 表的名称。
   * @param connection 要使用的HConnection。
   * @throws IOException 如果发生远程或网络异常
   */
  public HTable(TableName tableName, HConnection connection) throws IOException {
    this.tableName = tableName;
    this.cleanupPoolOnClose = true;
    this.cleanupConnectionOnClose = false;
    this.connection = connection;
    this.configuration = connection.getConfiguration();

    this.pool = getDefaultExecutor(this.configuration);
    this.finishSetup();
  }

  /**
   * 获取默认的线程池执行器。
   * @param conf 配置对象
   * @return 线程池执行器
   */
  public static ThreadPoolExecutor getDefaultExecutor(Configuration conf) {
    int maxThreads = conf.getInt("hbase.htable.threads.max", Integer.MAX_VALUE);
    if (maxThreads == 0) {
      maxThreads = 1; // is there a better default?
    }
    long keepAliveTime = conf.getLong("hbase.htable.threads.keepalivetime", 60);

    // Using the "direct handoff" approach, new threads will only be created
    // if it is necessary and will grow unbounded. This could be bad but in HCM
    // we only create as many Runnables as there are region servers. It means
    // it also scales when new region servers are added.
    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, maxThreads, keepAliveTime, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("htable"));
    ((ThreadPoolExecutor) pool).allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * 创建一个对象以访问HBase表。
   * 与使用相同 <code>conf</code> 实例创建的其他HTable实例共享zookeeper连接和其他资源。
   * 如果有可用的已填充区域缓存，则使用该缓存，该缓存由共享此 <code>conf</code> 实例的任何其他HTable实例填充。
   * 当ExecutorService由外部管理时，请使用此构造函数。
   * @param conf 要使用的配置对象。
   * @param tableName 表的名称。
   * @param pool 要使用的ExecutorService。
   * @throws IOException 如果发生远程或网络异常
   */
  public HTable(Configuration conf, final byte[] tableName, final ExecutorService pool)
      throws IOException {
    this(conf, TableName.valueOf(tableName), pool);
  }

  /**
   * 创建一个对象以访问HBase表。
   * 与使用相同 <code>conf</code> 实例创建的其他HTable实例共享zookeeper连接和其他资源。
   * 如果有可用的已填充区域缓存，则使用该缓存，该缓存由共享此 <code>conf</code> 实例的任何其他HTable实例填充。
   * 当ExecutorService由外部管理时，请使用此构造函数。
   * @param conf 要使用的配置对象。
   * @param tableName 表的名称。
   * @param pool 要使用的ExecutorService。
   * @throws IOException 如果发生远程或网络异常
   */
  public HTable(Configuration conf, final TableName tableName, final ExecutorService pool)
      throws IOException {
    this.connection = HConnectionManager.getConnection(conf);
    this.configuration = conf;
    this.pool = pool;
    this.tableName = tableName;
    this.cleanupPoolOnClose = false;
    this.cleanupConnectionOnClose = true;

    this.finishSetup();
  }

  /**
   * 创建一个对象以访问HBase表。
   * 与使用相同 <code>connection</code> 实例创建的其他HTable实例共享zookeeper连接和其他资源。
   * 当ExecutorService和HConnection实例由外部管理时，请使用此构造函数。
   * @param tableName 表的名称。
   * @param connection 要使用的HConnection。
   * @param pool 要使用的ExecutorService。
   * @throws IOException 如果发生远程或网络异常
   */
  public HTable(final byte[] tableName, final HConnection connection,
      final ExecutorService pool) throws IOException {
    this(TableName.valueOf(tableName), connection, pool);
  }

  /**
   * 创建一个对象以访问HBase表。
   * 与使用相同 <code>connection</code> 实例创建的其他HTable实例共享zookeeper连接和其他资源。
   * 当ExecutorService和HConnection实例由外部管理时，请使用此构造函数。
   * @param tableName 表的名称。
   * @param connection 要使用的HConnection。
   * @param pool 要使用的ExecutorService。
   * @throws IOException 如果发生远程或网络异常
   */
  public HTable(TableName tableName, final HConnection connection,
      final ExecutorService pool) throws IOException {
    this(tableName, connection, null, null, null, pool);
  }

  /**
   * 创建一个对象以访问HBase表。
   * 与使用相同 <code>connection</code> 实例创建的其他HTable实例共享zookeeper连接和其他资源。
   * 当ExecutorService和HConnection实例由外部管理时，请使用此构造函数。
   * @param tableName 表的名称。
   * @param connection 要使用的HConnection。
   * @param tableConfig 表配置
   * @param rpcCallerFactory RPC调用者工厂
   * @param rpcControllerFactory RPC控制器工厂
   * @param pool 要使用的ExecutorService。
   * @throws IOException 如果发生远程或网络异常
   */
  public HTable(TableName tableName, final HConnection connection,
      final TableConfiguration tableConfig,
      final RpcRetryingCallerFactory rpcCallerFactory,
      final RpcControllerFactory rpcControllerFactory,
      final ExecutorService pool) throws IOException {
    if (connection == null || connection.isClosed()) {
      throw new IllegalArgumentException("Connection is null or closed.");
    }
    this.tableName = tableName;
    this.connection = connection;
    this.configuration = connection.getConfiguration();
    this.tableConfiguration = tableConfig;
    this.cleanupPoolOnClose = this.cleanupConnectionOnClose = false;
    this.pool = pool;

    this.rpcCallerFactory = rpcCallerFactory;
    this.rpcControllerFactory = rpcControllerFactory;

    this.finishSetup();
  }

  /**
   * 仅供内部测试使用。
   */
  protected HTable(){
    tableName = null;
    tableConfiguration = new TableConfiguration();
    cleanupPoolOnClose = false;
    cleanupConnectionOnClose = false;
  }

  /**
   * @return 从配置中获取的最大键值大小。
   */
  public static int getMaxKeyValueSize(Configuration conf) {
    return conf.getInt("hbase.client.keyvalue.maxsize", -1);
  }

  /**
   * 根据传入的配置设置此HTable的参数
   */
  private void finishSetup() throws IOException {
    if (tableConfiguration == null) {
      tableConfiguration = new TableConfiguration(configuration);
    }
    this.operationTimeout = tableName.isSystemTable() ?
      tableConfiguration.getMetaOperationTimeout() : tableConfiguration.getOperationTimeout();
    this.writeBufferSize = tableConfiguration.getWriteBufferSize();
    this.clearBufferOnFail = true;
    this.autoFlush = true;
    this.currentWriteBufferSize = 0;
    this.scannerCaching = tableConfiguration.getScannerCaching();
    this.scannerMaxResultSize = tableConfiguration.getScannerMaxResultSize();
    this.rpcTimeout = configuration.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    if (this.rpcCallerFactory == null) {
      this.rpcCallerFactory = RpcRetryingCallerFactory.instantiate(configuration,
        this.connection.getStatisticsTracker());
    }
    if (this.rpcControllerFactory == null) {
      this.rpcControllerFactory = RpcControllerFactory.instantiate(configuration);
    }

    ap = new AsyncProcess<Object>(connection, tableName, pool, null, configuration,
      rpcCallerFactory, rpcControllerFactory);

    this.closed = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * 检查表是否启用。此方法会创建一个新的HBase配置，因此可能会因不正确的ZK客户端端口而导致单元测试失败。
   * @param tableName 要检查的表名。
   * @return 如果表在线，则为 {@code true}。
   * @throws IOException 如果发生远程或网络异常
	* @deprecated 请改用 {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not. This method creates a
   * new HBase configuration, so it might make your unit tests fail due to
   * incorrect ZK client port.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
	* @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not. This method creates a
   * new HBase configuration, so it might make your unit tests fail due to
   * incorrect ZK client port.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(TableName tableName) throws IOException {
    return isTableEnabled(HBaseConfiguration.create(), tableName);
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
	 * @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(Configuration conf, String tableName)
  throws IOException {
    return isTableEnabled(conf, TableName.valueOf(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
	 * @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(Configuration conf, byte[] tableName)
  throws IOException {
    return isTableEnabled(conf, TableName.valueOf(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link HBaseAdmin#isTableEnabled(org.apache.hadoop.hbase.TableName tableName)}
   */
  @Deprecated
  public static boolean isTableEnabled(Configuration conf,
      final TableName tableName) throws IOException {
    return HConnectionManager.execute(new HConnectable<Boolean>(conf) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.isTableEnabled(tableName);
      }
    });
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return The location of the given row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final String row)
  throws IOException {
    return connection.getRegionLocation(tableName, Bytes.toBytes(row), false);
  }

  /**
   * 查找给定行所在的区域。不重新加载缓存。
   * @param row 要查找的行。
   * @return 行的位置。
   * @throws IOException 如果发生远程或网络异常
   */
  public HRegionLocation getRegionLocation(final byte [] row)
  throws IOException {
    return connection.getRegionLocation(tableName, row, false);
  }

  /**
   * Finds the region on which the given row is being served.
   * @param row Row to find.
   * @param reload true to reload information or false to use cached information
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final byte [] row, boolean reload)
  throws IOException {
    return connection.getRegionLocation(tableName, row, reload);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte [] getTableName() {
    return this.tableName.getName();
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  /**
   * <em>内部使用</em> 供单元测试和工具进行底层操作。
   * @return HConnection实例。
   * @deprecated 此方法将从public更改为包保护级别。
   */
  // TODO(tsuna): Remove this.  Unit tests shouldn't require public helpers.
  @Deprecated
  public HConnection getConnection() {
    return this.connection;
  }

  /**
   * 获取扫描器一次将获取的行数。
   * <p>
   * 默认值来自 {@code hbase.client.scanner.caching}。
   * @deprecated 请改用 {@link Scan#setCaching(int)} 和 {@link Scan#getCaching()}
   */
  @Deprecated
  public int getScannerCaching() {
    return scannerCaching;
  }

  /**
   * 为保持0.96版本的向后兼容性而保留
   * @deprecated 从0.96版本开始。这是一个内部缓冲区，不应读取或写入。
   */
  @Deprecated
  public List<Row> getWriteBuffer() {
    return writeAsyncBuffer;
  }

  /**
   * 设置扫描器一次将获取的行数。
   * <p>
   * 这将覆盖由 {@code hbase.client.scanner.caching} 指定的值。
   * 增加此值将减少每次在扫描器上调用 {@code next()} 时所需的工作量，但会增加内存使用量（因为扫描器需要在内存中维护更多行）。
   * @param scannerCaching 扫描器一次将获取的行数。
   * @deprecated 请改用 {@link Scan#setCaching(int)}
   */
  @Deprecated
  public void setScannerCaching(int scannerCaching) {
    this.scannerCaching = scannerCaching;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    HTableDescriptor htd = HBaseAdmin.getTableDescriptor(tableName, connection, rpcCallerFactory,
      rpcControllerFactory, operationTimeout, rpcTimeout);
    if (htd != null) {
      return new UnmodifyableHTableDescriptor(htd);
    }
    return null;
  }

  /**
   * 获取当前打开表中每个区域的起始行键。
   * <p>
   * 这主要用于MapReduce集成。
   * @return 区域起始行键的数组
   * @throws IOException 如果发生远程或网络异常
   */
  public byte [][] getStartKeys() throws IOException {
    return getStartEndKeys().getFirst();
  }

  /**
   * 获取当前打开表中每个区域的结束行键。
   * <p>
   * 这主要用于MapReduce集成。
   * @return 区域结束行键的数组
   * @throws IOException 如果发生远程或网络异常
   */
  public byte[][] getEndKeys() throws IOException {
    return getStartEndKeys().getSecond();
  }

  /**
   * 获取当前打开表中每个区域的起始和结束行键。
   * <p>
   * 这主要用于MapReduce集成。
   * @return 包含区域起始和结束行键数组的Pair对象
   * @throws IOException 如果发生远程或网络异常
   */
  public Pair<byte[][],byte[][]> getStartEndKeys() throws IOException {
    NavigableMap<HRegionInfo, ServerName> regions = getRegionLocations();
    final List<byte[]> startKeyList = new ArrayList<byte[]>(regions.size());
    final List<byte[]> endKeyList = new ArrayList<byte[]>(regions.size());

    for (HRegionInfo region : regions.keySet()) {
      startKeyList.add(region.getStartKey());
      endKeyList.add(region.getEndKey());
    }

    return new Pair<byte [][], byte [][]>(
      startKeyList.toArray(new byte[startKeyList.size()][]),
      endKeyList.toArray(new byte[endKeyList.size()][]));
  }

  /**
   * 获取此表的所有区域及其地址。
   * <p>
   * 这主要用于MapReduce集成。
   * @return HRegionInfo与其服务器地址的映射
   * @throws IOException 如果发生远程或网络异常
   */
  public NavigableMap<HRegionInfo, ServerName> getRegionLocations() throws IOException {
    // TODO: Odd that this returns a Map of HRI to SN whereas getRegionLocation, singular, returns an HRegionLocation.
    return MetaScanner.allTableRegions(getConfiguration(), this.connection, getName(), false);
  }

  /**
   * 获取任意键范围对应的区域。
   * <p>
   * @param startKey 范围内的起始行，包含
   * @param endKey 范围内的结束行，不包含
   * @return 与包含指定范围的区域相对应的HRegionLocation列表
   * @throws IOException 如果发生远程或网络异常
   */
  public List<HRegionLocation> getRegionsInRange(final byte [] startKey,
    final byte [] endKey) throws IOException {
    return getRegionsInRange(startKey, endKey, false);
  }

  /**
   * 获取任意键范围对应的区域。
   * <p>
   * @param startKey 范围内的起始行，包含
   * @param endKey 范围内的结束行，不包含
   * @param reload true为重新加载信息，false为使用缓存信息
   * @return 与包含指定范围的区域相对应的HRegionLocation列表
   * @throws IOException 如果发生远程或网络异常
   */
  public List<HRegionLocation> getRegionsInRange(final byte [] startKey,
      final byte [] endKey, final boolean reload) throws IOException {
    return getKeysAndRegionsInRange(startKey, endKey, false, reload).getSecond();
  }

  /**
   * 获取任意键范围对应的起始键和区域。
   * <p>
   * @param startKey 范围内的起始行，包含
   * @param endKey 范围内的结束行
   * @param includeEndKey 如果为true，则endRow为包含，否则为不包含
   * @return 包含指定范围的起始键列表和HRegionLocation列表的Pair对象
   * @throws IOException 如果发生远程或网络异常
   */
  private Pair<List<byte[]>, List<HRegionLocation>> getKeysAndRegionsInRange(
      final byte[] startKey, final byte[] endKey, final boolean includeEndKey)
      throws IOException {
    return getKeysAndRegionsInRange(startKey, endKey, includeEndKey, false);
  }

  /**
   * 获取任意键范围对应的起始键和区域。
   * <p>
   * @param startKey 范围内的起始行，包含
   * @param endKey 范围内的结束行
   * @param includeEndKey 如果为true，则endRow为包含，否则为不包含
   * @param reload true为重新加载信息，false为使用缓存信息
   * @return 包含指定范围的起始键列表和HRegionLocation列表的Pair对象
   * @throws IOException 如果发生远程或网络异常
   */
  private Pair<List<byte[]>, List<HRegionLocation>> getKeysAndRegionsInRange(
      final byte[] startKey, final byte[] endKey, final boolean includeEndKey,
      final boolean reload) throws IOException {
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey,HConstants.EMPTY_END_ROW);
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException(
        "Invalid range: " + Bytes.toStringBinary(startKey) +
        " > " + Bytes.toStringBinary(endKey));
    }
    List<byte[]> keysInRange = new ArrayList<byte[]>();
    List<HRegionLocation> regionsInRange = new ArrayList<HRegionLocation>();
    byte[] currentKey = startKey;
    do {
      HRegionLocation regionLocation = getRegionLocation(currentKey, reload);
      keysInRange.add(currentKey);
      regionsInRange.add(regionLocation);
      currentKey = regionLocation.getRegionInfo().getEndKey();
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
        && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0
            || (includeEndKey && Bytes.compareTo(currentKey, endKey) == 0)));
    return new Pair<List<byte[]>, List<HRegionLocation>>(keysInRange,
        regionsInRange);
  }

  /**
   * {@inheritDoc}
   */
   @Override
   public Result getRowOrBefore(final byte[] row, final byte[] family)
   throws IOException {
     RegionServerCallable<Result> callable = new RegionServerCallable<Result>(this.connection,
         tableName, row) {
       public Result call() throws IOException {
            return ProtobufUtil.getRowOrBefore(getStub(), getLocation().getRegionInfo()
                .getRegionName(), row, family, rpcControllerFactory.newController());
          }
     };
     return rpcCallerFactory.<Result>newCaller(rpcTimeout).callWithRetries(callable,
         this.operationTimeout);
   }

   /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    if (scan.getBatch() > 0 && scan.isSmall()) {
      throw new IllegalArgumentException("Small scan should not be used with batching");
    }
    if (scan.getCaching() <= 0) {
      scan.setCaching(getScannerCaching());
    }

    if (scan.getMaxResultSize() <= 0) {
      scan.setMaxResultSize(scannerMaxResultSize);
    }

    if (scan.isReversed()) {
      if (scan.isSmall()) {
        return new ClientSmallReversedScanner(getConfiguration(), scan, getName(),
            this.connection);
      } else {
        return new ReversedClientScanner(getConfiguration(), scan, getName(), this.connection);
      }
    }

    if (scan.isSmall()) {
      return new ClientSmallScanner(getConfiguration(), scan, getName(), this.connection);
    } else {
      return new ClientScanner(getConfiguration(), scan, getName(), this.connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte [] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte [] family, byte [] qualifier)
  throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result get(final Get get) throws IOException {
    return get(get, get.isCheckExistenceOnly());
  }

  private Result get(Get get, final boolean checkExistenceOnly) throws IOException {
    // if we are changing settings to the get, clone it.
    if (get.isCheckExistenceOnly() != checkExistenceOnly) {
      get = ReflectionUtils.newInstance(get.getClass(), get);
      get.setCheckExistenceOnly(checkExistenceOnly);
    }

    // have to instanatiate this and set the priority here since in protobuf util we don't pass in
    // the tablename... an unfortunate side-effect of public interfaces :-/ In 0.99+ we put all the
    // logic back into HTable
    final PayloadCarryingRpcController controller = rpcControllerFactory.newController();
    controller.setPriority(tableName);
    final Get getReq = get;
    RegionServerCallable<Result> callable =
        new RegionServerCallable<Result>(this.connection, getName(), get.getRow()) {
          public Result call() throws IOException {
            return ProtobufUtil.get(getStub(), getLocation().getRegionInfo().getRegionName(),
              getReq, controller);
          }
        };
    return rpcCallerFactory.<Result>newCaller(rpcTimeout).callWithRetries(callable,
      this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result[] get(List<Get> gets) throws IOException {
    if (gets.size() == 1) {
      return new Result[]{get(gets.get(0))};
    }
    try {
      Object [] r1 = batch((List)gets);

      // translate.
      Result [] results = new Result[r1.length];
      int i=0;
      for (Object o : r1) {
        // batch ensures if there is a failure we get an exception instead
        results[i++] = (Result) o;
      }

      return results;
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void batch(final List<? extends Row> actions, final Object[] results)
      throws InterruptedException, IOException {
    batchCallback(actions, results, null);
  }

  /**
   * {@inheritDoc}
   * @deprecated 如果任何一个操作抛出异常，将无法检索部分执行的结果。请改用 {@link #batch(List, Object[])}。
   */
  @Override
  public Object[] batch(final List<? extends Row> actions)
     throws InterruptedException, IOException {
    return batchCallback(actions, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R> void batchCallback(
      final List<? extends Row> actions, final Object[] results, final Batch.Callback<R> callback)
      throws IOException, InterruptedException {
    connection.processBatchCallback(actions, tableName, pool, results, callback);
  }

  /**
   * {@inheritDoc}
   * @deprecated 如果任何一个操作抛出异常，将无法检索部分执行的结果。请改用
   * {@link #batchCallback(List, Object[], org.apache.hadoop.hbase.client.coprocessor.Batch.Callback)}。
   */
  @Override
  public <R> Object[] batchCallback(
    final List<? extends Row> actions, final Batch.Callback<R> callback) throws IOException,
      InterruptedException {
    Object[] results = new Object[actions.size()];
    batchCallback(actions, results, callback);
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final Delete delete)
  throws IOException {
    RegionServerCallable<Boolean> callable = new RegionServerCallable<Boolean>(connection,
        tableName, delete.getRow()) {
      public Boolean call() throws IOException {
        try {
          MutateRequest request = RequestConverter.buildMutateRequest(
            getLocation().getRegionInfo().getRegionName(), delete);
              PayloadCarryingRpcController controller = rpcControllerFactory.newController();
              controller.setPriority(tableName);
              MutateResponse response = getStub().mutate(controller, request);
          return Boolean.valueOf(response.getProcessed());
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }
      }
    };
    rpcCallerFactory.<Boolean> newCaller(rpcTimeout).callWithRetries(callable,
        this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final List<Delete> deletes)
  throws IOException {
    Object[] results = new Object[deletes.size()];
    try {
      batch(deletes, results);
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    } finally {
      // mutate list so that it is empty for complete success, or contains only failed records
      // results are returned in the same order as the requests in list
      // walk the list backwards, so we can remove from list without impacting the indexes of earlier members
      for (int i = results.length - 1; i>=0; i--) {
        // if result is not null, it succeeded
        if (results[i] instanceof Result) {
          deletes.remove(i);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final Put put)
      throws InterruptedIOException, RetriesExhaustedWithDetailsException {
    doPut(put);
    if (autoFlush) {
      flushCommits();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final List<Put> puts)
      throws InterruptedIOException, RetriesExhaustedWithDetailsException {
    for (Put put : puts) {
      doPut(put);
    }
    if (autoFlush) {
      flushCommits();
    }
  }


  /**
   * 将put操作添加到缓冲区。如果缓冲区太大，则将缓冲区发送到集群。
   * @throws RetriesExhaustedWithDetailsException 如果集群出现错误。
   * @throws InterruptedIOException 如果我们被中断。
   */
  private void doPut(Put put) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
    if (ap.hasError()){
      writeAsyncBuffer.add(put);
      backgroundFlushCommits(true);
    }

    validatePut(put);

    currentWriteBufferSize += put.heapSize();
    writeAsyncBuffer.add(put);

    while (currentWriteBufferSize > writeBufferSize) {
      backgroundFlushCommits(false);
    }
  }


  /**
   * 将缓冲区中的操作发送到服务器。不等待服务器的应答。
   * 如果出现错误（例如，从先前的刷新或错误操作达到最大重试次数），它会尝试发送缓冲区中的所有操作并发送异常。
   * @param synchronous - 如果为true，则发送所有写入并等待所有写入完成后再返回。
   */
  private void backgroundFlushCommits(boolean synchronous) throws
      InterruptedIOException, RetriesExhaustedWithDetailsException {

    try {
      do {
        ap.submit(writeAsyncBuffer, true);
      } while (synchronous && !writeAsyncBuffer.isEmpty());

      if (synchronous) {
        ap.waitUntilDone();
      }

      if (ap.hasError()) {
        LOG.debug(tableName + ": One or more of the operations have failed -" +
            " waiting for all operation in progress to finish (successfully or not)");
        while (!writeAsyncBuffer.isEmpty()) {
          ap.submit(writeAsyncBuffer, true);
        }
        ap.waitUntilDone();

        if (!clearBufferOnFail) {
          // if clearBufferOnFailed is not set, we're supposed to keep the failed operation in the
          //  write buffer. This is a questionable feature kept here for backward compatibility
          writeAsyncBuffer.addAll(ap.getFailedOperations());
        }
        RetriesExhaustedWithDetailsException e = ap.getErrors();
        ap.clearErrors();
        throw e;
      }
    } finally {
      currentWriteBufferSize = 0;
      for (Row mut : writeAsyncBuffer) {
        if (mut instanceof Mutation) {
          currentWriteBufferSize += ((Mutation) mut).heapSize();
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mutateRow(final RowMutations rm) throws IOException {
    final RetryingTimeTracker tracker = new RetryingTimeTracker();
    PayloadCarryingServerCallable<MultiResponse> callable =
      new PayloadCarryingServerCallable<MultiResponse>(connection, getName(), rm.getRow(),
            rpcControllerFactory) {
        @Override
        public MultiResponse call() throws IOException {
          tracker.start();
          controller.setPriority(tableName);
          int remainingTime = tracker.getRemainingTime(operationTimeout);
          if (remainingTime == 0) {
            throw new DoNotRetryIOException("Timeout for mutate row");
          }
          int timeout = remainingTime;
          if (rpcTimeout > 0 && rpcTimeout < timeout) {
            timeout = rpcTimeout;
          }
          RpcClient.setRpcTimeout(timeout);
          try {
            RegionAction.Builder regionMutationBuilder = RequestConverter.buildRegionAction(
              getLocation().getRegionInfo().getRegionName(), rm);
            regionMutationBuilder.setAtomic(true);
            MultiRequest request =
              MultiRequest.newBuilder().addRegionAction(regionMutationBuilder.build()).build();
            ClientProtos.MultiResponse response = getStub().multi(controller, request);
            ClientProtos.RegionActionResult res = response.getRegionActionResultList().get(0);
            if (res.hasException()) {
              Throwable ex = ProtobufUtil.toException(res.getException());
              if (ex instanceof IOException) {
                throw (IOException) ex;
              }
              throw new IOException("Failed to mutate row: " +
                Bytes.toStringBinary(rm.getRow()), ex);
            }
            return ResponseConverter.getResults(request, response, controller.cellScanner());
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      };
    ap.submitAll(rm.getMutations(), null, callable);
    ap.waitUntilDone();
    if (ap.hasError()) {
      throw ap.getErrors();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result append(final Append append) throws IOException {
    if (append.numFamilies() == 0) {
      throw new IOException(
          "Invalid arguments to append, no columns specified");
    }

    NonceGenerator ng = this.connection.getNonceGenerator();
    final long nonceGroup = ng.getNonceGroup(), nonce = ng.newNonce();
    RegionServerCallable<Result> callable =
      new RegionServerCallable<Result>(this.connection, getName(), append.getRow()) {
        public Result call() throws IOException {
          try {
            MutateRequest request = RequestConverter.buildMutateRequest(
              getLocation().getRegionInfo().getRegionName(), append, nonceGroup, nonce);
            PayloadCarryingRpcController rpcController = rpcControllerFactory.newController();
            rpcController.setPriority(getTableName());
            MutateResponse response = getStub().mutate(rpcController, request);
            if (!response.hasResult()) return null;
            return ProtobufUtil.toResult(response.getResult(), rpcController.cellScanner());
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      };
    return rpcCallerFactory.<Result> newCaller(rpcTimeout).callWithRetries(callable,
        this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result increment(final Increment increment) throws IOException {
    if (!increment.hasFamilies()) {
      throw new IOException(
          "Invalid arguments to increment, no columns specified");
    }
    NonceGenerator ng = this.connection.getNonceGenerator();
    final long nonceGroup = ng.getNonceGroup(), nonce = ng.newNonce();
    RegionServerCallable<Result> callable = new RegionServerCallable<Result>(this.connection,
        getName(), increment.getRow()) {
      public Result call() throws IOException {
        try {
          MutateRequest request = RequestConverter.buildMutateRequest(
            getLocation().getRegionInfo().getRegionName(), increment, nonceGroup, nonce);
          PayloadCarryingRpcController rpcController = rpcControllerFactory.newController();
          rpcController.setPriority(getTableName());
          MutateResponse response = getStub().mutate(rpcController, request);
          return ProtobufUtil.toResult(response.getResult(), rpcController.cellScanner());
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }
      }
    };
    return rpcCallerFactory.<Result> newCaller(rpcTimeout).callWithRetries(callable,
        this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, Durability.SYNC_WAL);
  }

  /**
   * @deprecated 请改用 {@link #incrementColumnValue(byte[], byte[], byte[], long, Durability)}
   */
  @Deprecated
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount,
      writeToWAL? Durability.SYNC_WAL: Durability.SKIP_WAL);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final Durability durability)
  throws IOException {
    NullPointerException npe = null;
    if (row == null) {
      npe = new NullPointerException("row is null");
    } else if (family == null) {
      npe = new NullPointerException("family is null");
    } else if (qualifier == null) {
      npe = new NullPointerException("qualifier is null");
    }
    if (npe != null) {
      throw new IOException(
          "Invalid arguments to incrementColumnValue", npe);
    }

    NonceGenerator ng = this.connection.getNonceGenerator();
    final long nonceGroup = ng.getNonceGroup(), nonce = ng.newNonce();
    RegionServerCallable<Long> callable =
      new RegionServerCallable<Long>(connection, getName(), row) {
        public Long call() throws IOException {
          try {
            MutateRequest request = RequestConverter.buildIncrementRequest(
              getLocation().getRegionInfo().getRegionName(), row, family,
              qualifier, amount, durability, nonceGroup, nonce);
            PayloadCarryingRpcController rpcController = rpcControllerFactory.newController();
            rpcController.setPriority(getTableName());
            MutateResponse response = getStub().mutate(rpcController, request);
            Result result =
              ProtobufUtil.toResult(response.getResult(), rpcController.cellScanner());
            return Long.valueOf(Bytes.toLong(result.getValue(family, qualifier)));
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      };
    return rpcCallerFactory.<Long> newCaller(rpcTimeout).callWithRetries(callable,
        this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put)
  throws IOException {
    RegionServerCallable<Boolean> callable =
      new RegionServerCallable<Boolean>(connection, getName(), row) {
        public Boolean call() throws IOException {
          try {
            MutateRequest request = RequestConverter.buildMutateRequest(
              getLocation().getRegionInfo().getRegionName(), row, family, qualifier,
                new BinaryComparator(value), CompareType.EQUAL, put);
            PayloadCarryingRpcController rpcController = rpcControllerFactory.newController();
            rpcController.setPriority(getTableName());
            MutateResponse response = getStub().mutate(rpcController, request);
            return Boolean.valueOf(response.getProcessed());
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      };
    return rpcCallerFactory.<Boolean> newCaller(rpcTimeout).callWithRetries(callable,
        this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Delete delete)
  throws IOException {
    RegionServerCallable<Boolean> callable =
      new RegionServerCallable<Boolean>(connection, getName(), row) {
        public Boolean call() throws IOException {
          try {
            MutateRequest request = RequestConverter.buildMutateRequest(
              getLocation().getRegionInfo().getRegionName(), row, family, qualifier,
                new BinaryComparator(value), CompareType.EQUAL, delete);
            PayloadCarryingRpcController rpcController = rpcControllerFactory.newController();
            rpcController.setPriority(getTableName());
            MutateResponse response = getStub().mutate(rpcController, request);
            return Boolean.valueOf(response.getProcessed());
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      };
    return rpcCallerFactory.<Boolean> newCaller(rpcTimeout).callWithRetries(callable,
        this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndMutate(final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final byte [] value, final RowMutations rm)
  throws IOException {
    final RetryingTimeTracker tracker = new RetryingTimeTracker();
    PayloadCarryingServerCallable<MultiResponse> callable =
      new PayloadCarryingServerCallable<MultiResponse>(connection, getName(), rm.getRow(),
        rpcControllerFactory) {
        @Override
        public MultiResponse call() throws IOException {
          tracker.start();
          controller.setPriority(tableName);
          int remainingTime = tracker.getRemainingTime(operationTimeout);
          if (remainingTime == 0) {
            throw new DoNotRetryIOException("Timeout for mutate row");
          }
          int timeout = remainingTime;
          if (rpcTimeout > 0 && rpcTimeout < timeout){
            timeout = rpcTimeout;
          }
          RpcClient.setRpcTimeout(timeout);
          try {
            MultiRequest request = RequestConverter.buildMutateRequest(
                    getLocation().getRegionInfo().getRegionName(), row, family, qualifier,
                    new BinaryComparator(value), CompareType.EQUAL, rm);
            ClientProtos.MultiResponse response = getStub().multi(controller, request);
            ClientProtos.RegionActionResult res = response.getRegionActionResultList().get(0);
            if (res.hasException()) {
              Throwable ex = ProtobufUtil.toException(res.getException());
              if (ex instanceof IOException) {
                throw (IOException) ex;
              }
              throw new IOException("Failed to mutate row: " +
                Bytes.toStringBinary(rm.getRow()), ex);
            }
            return ResponseConverter.getResults(request, response, controller.cellScanner());
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      };
    /**
     *  Currently, we use one array to store 'processed' flag which return by server.
     *  It is some excessive, but that its required by the framework right now
     * */
    final boolean[] processed = new boolean[1];
    ap.submitAll(rm.getMutations(), new Batch.Callback<Object>() {
      @Override
      public void update(byte[] region, byte[] row, Object result) {
        processed[0] = ((Result)result).getExists();
      }
    }, callable);
    ap.waitUntilDone();
    try {
      if (ap.hasError()) {
        throw ap.getErrors();
      }
    } finally {
      ap.clearErrors();
    }
    return processed[0];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean exists(final Get get) throws IOException {
    Result r = get(get, true);
    assert r.getExists() != null;
    return r.getExists();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Boolean[] exists(final List<Get> gets) throws IOException {
    if (gets.isEmpty()) return new Boolean[]{};
    if (gets.size() == 1) return new Boolean[]{exists(gets.get(0))};

    ArrayList<Get> exists = new ArrayList<Get>(gets.size());
    for (Get g: gets){
      Get ge = new Get(g);
      ge.setCheckExistenceOnly(true);
      exists.add(ge);
    }

    Object[] r1;
    try {
      r1 = batch(exists);
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }

    // translate.
    Boolean[] results = new Boolean[r1.length];
    int i = 0;
    for (Object o : r1) {
      // batch ensures if there is a failure we get an exception instead
      results[i++] = ((Result)o).getExists();
    }

    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flushCommits() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
    // As we can have an operation in progress even if the buffer is empty, we call
    //  backgroundFlushCommits at least one time.
    backgroundFlushCommits(true);
  }

  /**
   * Process a mixed batch of Get, Put and Delete actions. All actions for a
   * RegionServer are forwarded in one RPC call. Queries are executed in parallel.
   *
   * @param list The collection of actions.
   * @param results An empty array, same size as list. If an exception is thrown,
   * you can test here for partial results, and to determine which actions
   * processed successfully.
   * @throws IOException if there are problems talking to META. Per-item
   * exceptions are stored in the results array.
   */
  /**
   * 处理包含 Get、Put 和 Delete 操作的混合批量请求。同一 RegionServer 的所有操作会通过一次 RPC 调用提交。
   * 查询会并行执行。
   *
   * @param list 操作列表
   * @param results 一个与 list 大小相同的空数组。如果抛出异常，可以检查此数组以获取部分成功的结果，并确定哪些操作已成功处理。
   * @param callback 回调函数，用于处理每行的结果
   * @throws IOException 如果与 META 表通信时发生错误。每个操作的异常会存储在 results 数组中。
   * @throws InterruptedException 如果线程被中断
   */
  public <R> void processBatchCallback(
    final List<? extends Row> list, final Object[] results, final Batch.Callback<R> callback)
    throws IOException, InterruptedException {
    this.batchCallback(list, results, callback);
  }


  /**
   * 参数化的批量处理，允许不同的 {@link Row} 实现返回不同的类型。
   *
   * @param list 操作列表
   * @param results 一个与 list 大小相同的空数组，用于存放结果
   * @throws IOException 如果发生 I/O 错误
   * @throws InterruptedException 如果线程被中断
   */
  public void processBatch(final List<? extends Row> list, final Object[] results)
    throws IOException, InterruptedException {

    this.processBatchCallback(list, results, null);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    if (this.closed) {
      return;
    }
    flushCommits();
    if (cleanupPoolOnClose) {
      this.pool.shutdown();
    }
    if (cleanupConnectionOnClose) {
      if (this.connection != null) {
        this.connection.close();
      }
    }
    this.closed = true;
  }

  // 验证 put 操作的格式是否正确
  public void validatePut(final Put put) throws IllegalArgumentException {
    validatePut(put, tableConfiguration.getMaxKeyValueSize());
  }

  // 验证 put 操作的格式是否正确
  public static void validatePut(Put put, int maxKeyValueSize) throws IllegalArgumentException {
    if (put.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }
    if (maxKeyValueSize > 0) {
      for (List<Cell> list : put.getFamilyCellMap().values()) {
        for (Cell cell : list) {
          // KeyValue v1 expectation.  Cast for now.
          KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
          if (kv.getLength() > maxKeyValueSize) {
            throw new IllegalArgumentException("KeyValue size too large");
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAutoFlush() {
    return autoFlush;
  }

  /**
   * {@inheritDoc}
   * @deprecated 从 0.96 版本开始，此方法已不推荐使用，请改用 {@link #setAutoFlushTo(boolean)} 或 {@link #setAutoFlush(boolean, boolean)}
   */
  @Deprecated
  @Override
  public void setAutoFlush(boolean autoFlush) {
    setAutoFlush(autoFlush, autoFlush);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlushTo(boolean autoFlush) {
    setAutoFlush(autoFlush, clearBufferOnFail);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    this.autoFlush = autoFlush;
    this.clearBufferOnFail = autoFlush || clearBufferOnFail;
  }

  /**
   * 返回此 HTable 写缓冲区的最大大小（字节）。
   * <p>
   * 默认值来自配置参数 {@code hbase.client.write.buffer}。
   * @return 写缓冲区的大小（字节）。
   */
  @Override
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * 设置缓冲区的大小（字节）。
   * <p>
   * 如果新设置的大小小于写缓冲区中当前的数据量，缓冲区将被刷新。
   * @param writeBufferSize 新的写缓冲区大小（字节）。
   * @throws IOException 如果发生远程或网络异常。
   */
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    this.writeBufferSize = writeBufferSize;
    if(currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  /**
   * 获取用于此 HTable 的多请求线程池。
   * @return 用于多请求的线程池
   */
  ExecutorService getPool() {
    return this.pool;
  }

  /**
   * 启用或禁用表的区域缓存预取。此设置将应用于共享同一连接的所有 HTable 实例。
   * 默认情况下，缓存预取是启用的。
   * @param tableName 要配置的表名。
   * @param enable 设置为 true 以启用区域缓存预取，或设置为 false 以禁用它。
   * @throws IOException
   */
  public static void setRegionCachePrefetch(final byte[] tableName,
      final boolean enable) throws IOException {
    setRegionCachePrefetch(TableName.valueOf(tableName), enable);
  }

  public static void setRegionCachePrefetch(
      final TableName tableName,
      final boolean enable) throws IOException {
    HConnectionManager.execute(new HConnectable<Void>(HBaseConfiguration.create()) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        connection.setRegionCachePrefetch(tableName, enable);
        return null;
      }
    });
  }

  /**
   * 启用或禁用表的区域缓存预取。此设置将应用于共享同一连接的所有 HTable 实例。
   * 默认情况下，缓存预取是启用的。
   * @param conf 要使用的 Configuration 对象。
   * @param tableName 要配置的表名。
   * @param enable 设置为 true 以启用区域缓存预取，或设置为 false 以禁用它。
   * @throws IOException
   */
  public static void setRegionCachePrefetch(final Configuration conf,
      final byte[] tableName, final boolean enable) throws IOException {
    setRegionCachePrefetch(conf, TableName.valueOf(tableName), enable);
  }

  public static void setRegionCachePrefetch(final Configuration conf,
      final TableName tableName,
      final boolean enable) throws IOException {
    HConnectionManager.execute(new HConnectable<Void>(conf) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        connection.setRegionCachePrefetch(tableName, enable);
        return null;
      }
    });
  }

  /**
   * 检查表的区域缓存预取是否已启用。
   * @param conf 要使用的 Configuration 对象。
   * @param tableName 要检查的表名。
   * @return 如果表的区域缓存预取已启用，则返回 true；否则返回 false。
   * @throws IOException
   */
  public static boolean getRegionCachePrefetch(final Configuration conf,
      final byte[] tableName) throws IOException {
    return getRegionCachePrefetch(conf, TableName.valueOf(tableName));
  }

  public static boolean getRegionCachePrefetch(final Configuration conf,
      final TableName tableName) throws IOException {
    return HConnectionManager.execute(new HConnectable<Boolean>(conf) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.getRegionCachePrefetch(tableName);
      }
    });
  }

  /**
   * 检查表的区域缓存预取是否已启用。
   * @param tableName 要检查的表名。
   * @return 如果表的区域缓存预取已启用，则返回 true；否则返回 false。
   * @throws IOException
   */
  public static boolean getRegionCachePrefetch(final byte[] tableName) throws IOException {
    return getRegionCachePrefetch(TableName.valueOf(tableName));
  }

  public static boolean getRegionCachePrefetch(
      final TableName tableName) throws IOException {
    return HConnectionManager.execute(new HConnectable<Boolean>(
        HBaseConfiguration.create()) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.getRegionCachePrefetch(tableName);
      }
    });
  }

  /**
   * 显式清除区域缓存，以从 META 表中获取最新值。
   * 这是一个高级用户功能：除非您了解其后果，否则请避免使用。
   */
  public void clearRegionCache() {
    this.connection.clearRegionCache();
  }

  /**
   * {@inheritDoc}
   */
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    return new RegionCoprocessorRpcChannel(connection, tableName, row, rpcCallerFactory,
        rpcControllerFactory);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends Service, R> Map<byte[],R> coprocessorService(final Class<T> service,
      byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable)
      throws ServiceException, Throwable {
    final Map<byte[],R> results =  Collections.synchronizedMap(
        new TreeMap<byte[], R>(Bytes.BYTES_COMPARATOR));
    coprocessorService(service, startKey, endKey, callable, new Batch.Callback<R>() {
      public void update(byte[] region, byte[] row, R value) {
        if (region != null) {
          results.put(region, value);
        }
      }
    });
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends Service, R> void coprocessorService(final Class<T> service,
      byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable,
      final Batch.Callback<R> callback) throws ServiceException, Throwable {

    // get regions covered by the row range
    List<byte[]> keys = getStartKeysInRange(startKey, endKey);

    Map<byte[],Future<R>> futures =
        new TreeMap<byte[],Future<R>>(Bytes.BYTES_COMPARATOR);
    for (final byte[] r : keys) {
      final RegionCoprocessorRpcChannel channel =
          new RegionCoprocessorRpcChannel(connection, tableName, r, rpcCallerFactory,
              rpcControllerFactory);
      Future<R> future = pool.submit(
          new Callable<R>() {
            public R call() throws Exception {
              T instance = ProtobufUtil.newServiceStub(service, channel);
              R result = callable.call(instance);
              byte[] region = channel.getLastRegion();
              if (callback != null) {
                callback.update(region, r, result);
              }
              return result;
            }
          });
      futures.put(r, future);
    }
    for (Map.Entry<byte[],Future<R>> e : futures.entrySet()) {
      try {
        e.getValue().get();
      } catch (ExecutionException ee) {
        LOG.warn("Error calling coprocessor service " + service.getName() + " for row "
            + Bytes.toStringBinary(e.getKey()), ee);
        throw ee.getCause();
      } catch (InterruptedException ie) {
        throw new InterruptedIOException("Interrupted calling coprocessor service " + service.getName()
            + " for row " + Bytes.toStringBinary(e.getKey()))
            .initCause(ie);
      }
    }
  }

  private List<byte[]> getStartKeysInRange(byte[] start, byte[] end)
  throws IOException {
    if (start == null) {
      start = HConstants.EMPTY_START_ROW;
    }
    if (end == null) {
      end = HConstants.EMPTY_END_ROW;
    }
    return getKeysAndRegionsInRange(start, end, true).getFirst();
  }

  /**
   * 设置操作超时时间。
   * @param operationTimeout 操作超时时间（毫秒）
   */
  public void setOperationTimeout(int operationTimeout) {
    this.operationTimeout = operationTimeout;
  }

  /**
   * 获取操作超时时间。
   * @return 操作超时时间（毫秒）
   */
  public int getOperationTimeout() {
    return operationTimeout;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return tableName + ";" + connection;
  }

  /**
   * 运行基本测试。
   * @param args 传入表名和行键，将获取其内容。
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    HTable t = new HTable(HBaseConfiguration.create(), args[0]);
    try {
      System.out.println(t.get(new Get(Bytes.toBytes(args[1]))));
    } finally {
      t.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor, Message request,
      byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
    final Map<byte[], R> results = Collections.synchronizedMap(new TreeMap<byte[], R>(
        Bytes.BYTES_COMPARATOR));
    batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype,
        new Callback<R>() {

          @Override
          public void update(byte[] region, byte[] row, R result) {
            if (region != null) {
              results.put(region, result);
            }
          }
        });
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Message> void batchCoprocessorService(
      final Descriptors.MethodDescriptor methodDescriptor, final Message request,
      byte[] startKey, byte[] endKey, final R responsePrototype, final Callback<R> callback)
      throws ServiceException, Throwable {

    if (startKey == null) {
      startKey = HConstants.EMPTY_START_ROW;
    }
    if (endKey == null) {
      endKey = HConstants.EMPTY_END_ROW;
    }
    // get regions covered by the row range
    Pair<List<byte[]>, List<HRegionLocation>> keysAndRegions =
        getKeysAndRegionsInRange(startKey, endKey, true);
    List<byte[]> keys = keysAndRegions.getFirst();
    List<HRegionLocation> regions = keysAndRegions.getSecond();

    // check if we have any calls to make
    if (keys.isEmpty()) {
      LOG.info("No regions were selected by key range start=" + Bytes.toStringBinary(startKey) +
          ", end=" + Bytes.toStringBinary(endKey));
      return;
    }

    List<RegionCoprocessorServiceExec> execs = new ArrayList<RegionCoprocessorServiceExec>();
    final Map<byte[], RegionCoprocessorServiceExec> execsByRow =
        new TreeMap<byte[], RegionCoprocessorServiceExec>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < keys.size(); i++) {
      final byte[] rowKey = keys.get(i);
      final byte[] region = regions.get(i).getRegionInfo().getRegionName();
      RegionCoprocessorServiceExec exec =
          new RegionCoprocessorServiceExec(region, rowKey, methodDescriptor, request);
      execs.add(exec);
      execsByRow.put(rowKey, exec);
    }

    // tracking for any possible deserialization errors on success callback
    // TODO: it would be better to be able to reuse AsyncProcess.BatchErrors here
    final List<Throwable> callbackErrorExceptions = new ArrayList<Throwable>();
    final List<Row> callbackErrorActions = new ArrayList<Row>();
    final List<String> callbackErrorServers = new ArrayList<String>();

    AsyncProcess<ClientProtos.CoprocessorServiceResult> asyncProcess =
        new AsyncProcess<ClientProtos.CoprocessorServiceResult>(connection, tableName, pool,
            new AsyncProcess.AsyncProcessCallback<ClientProtos.CoprocessorServiceResult>() {
          @SuppressWarnings("unchecked")
          @Override
          public void success(int originalIndex, byte[] region, Row row,
              ClientProtos.CoprocessorServiceResult serviceResult) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Received result for endpoint " + methodDescriptor.getFullName() +
                " call #" + originalIndex + ": region=" + Bytes.toStringBinary(region) +
                ", row=" + Bytes.toStringBinary(row.getRow()) +
                ", value=" + serviceResult.getValue().getValue());
            }
            try {
              Message.Builder builder = responsePrototype.newBuilderForType();
              ProtobufUtil.mergeFrom(builder, serviceResult.getValue().getValue());
              callback.update(region, row.getRow(), (R) builder.build());
            } catch (IOException e) {
              LOG.error("Unexpected response type from endpoint " + methodDescriptor.getFullName(),
                e);
              callbackErrorExceptions.add(e);
              callbackErrorActions.add(row);
              callbackErrorServers.add("null");
            }
          }

          @Override
          public boolean failure(int originalIndex, byte[] region, Row row, Throwable t) {
            RegionCoprocessorServiceExec exec = (RegionCoprocessorServiceExec) row;
            LOG.error("Failed calling endpoint " + methodDescriptor.getFullName() + ": region="
                + Bytes.toStringBinary(exec.getRegion()), t);
            return true;
          }

          @Override
          public boolean retriableFailure(int originalIndex, Row row, byte[] region,
              Throwable exception) {
            RegionCoprocessorServiceExec exec = (RegionCoprocessorServiceExec) row;
            LOG.error("Failed calling endpoint " + methodDescriptor.getFullName() + ": region="
                + Bytes.toStringBinary(exec.getRegion()), exception);
            return !(exception instanceof DoNotRetryIOException);
          }
        },
        configuration, rpcCallerFactory, rpcControllerFactory);

    asyncProcess.submitAll(execs);
    asyncProcess.waitUntilDone();

    if (asyncProcess.hasError()) {
      throw asyncProcess.getErrors();
    } else if (!callbackErrorExceptions.isEmpty()) {
      throw new RetriesExhaustedWithDetailsException(callbackErrorExceptions, callbackErrorActions,
        callbackErrorServers);
    }
  }
}
