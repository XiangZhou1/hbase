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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Addressing;

/**
 * Data structure to hold HRegionInfo and the address for the hosting
 * HRegionServer.  Immutable.  Comparable, but we compare the 'location' only:
 * i.e. the hostname and port, and *not* the regioninfo.  This means two
 * instances are the same if they refer to the same 'location' (the same
 * hostname and port), though they may be carrying different regions.
 *
 * On a big cluster, each client will have thousands of instances of this object, often
 *  100 000 of them if not million. It's important to keep the object size as small
 *  as possible.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HRegionLocation implements Comparable<HRegionLocation> {
  private final HRegionInfo regionInfo;
  /**
   * 引用了一个 ServerName 对象，ServerName 封装了 RegionServer 的主机名（hostname）、
   *   端口（port）和一个启动代码（start code，用于唯一标识一个 RegionServer 进程实例）。
   *   这精确地指明了 Region 所在的物理服务器。
   */
  private final ServerName serverName;
  /**
   *  Region 在该 ServerName 上被打开（open）时，该 RegionServer 的 WAL (Write-Ahead Log) 的最新序列号。
   *  这个值会和位置信息一起被记录在 hbase:meta 表中。
   */
  private final long seqNum;

  public HRegionLocation(HRegionInfo regionInfo, ServerName serverName) {
    this(regionInfo, serverName, HConstants.NO_SEQNUM);
  }

  public HRegionLocation(HRegionInfo regionInfo, ServerName serverName, long seqNum) {
    this.regionInfo = regionInfo;
    this.serverName = serverName;
    this.seqNum = seqNum;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "region=" + this.regionInfo.getRegionNameAsString() +
        ", hostname=" + this.serverName + ", seqNum=" + seqNum;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof HRegionLocation)) {
      return false;
    }
    return this.compareTo((HRegionLocation)o) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return this.serverName.hashCode();
  }

  /** @return HRegionInfo */
  public HRegionInfo getRegionInfo(){
    return regionInfo;
  }

  public String getHostname() {
    return this.serverName.getHostname();
  }

  public int getPort() {
    return this.serverName.getPort();
  }

  public long getSeqNum() {
    return seqNum;
  }

  /**
   * @return String made of hostname and port formatted as per {@link Addressing#createHostAndPortStr(String, int)}
   */
  public String getHostnamePort() {
    return Addressing.createHostAndPortStr(this.getHostname(), this.getPort());
  }

  public ServerName getServerName() {
    return serverName;
  }

  public int compareTo(HRegionLocation o) {
    return serverName.compareTo(o.getServerName());
  }
}
