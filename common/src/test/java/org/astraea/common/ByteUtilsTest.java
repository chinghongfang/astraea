/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.it.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ByteUtilsTest {
  @Test
  void testInt2Bytes() {
    Assertions.assertArrayEquals(new byte[] {0, 0, 4, -46}, ByteUtils.toBytes(1234));
  }

  @Test
  void testString2Bytes() {
    var string = Utils.randomString();
    Assertions.assertEquals(string, new String(ByteUtils.toBytes(string), StandardCharsets.UTF_8));
  }

  @Test
  void testChar2Bytes() {
    Assertions.assertEquals(
        'c', new String(ByteUtils.toBytes('c'), StandardCharsets.UTF_8).charAt(0));
  }

  @Test
  void testLong2Bytes() {
    Assertions.assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 1, 123}, ByteUtils.toBytes(379L));
  }

  @Test
  void testFloat2Bytes() {
    Assertions.assertArrayEquals(new byte[] {63, -64, 0, 0}, ByteUtils.toBytes(1.5f));
  }

  @Test
  void testDouble2Bytes() {
    Assertions.assertArrayEquals(
        new byte[] {64, 94, -58, 102, 102, 102, 102, 102}, ByteUtils.toBytes(123.1D));
  }

  @Test
  void testBoolean2Bytes() {
    Assertions.assertArrayEquals(new byte[] {1}, ByteUtils.toBytes(true));
    Assertions.assertArrayEquals(new byte[] {0}, ByteUtils.toBytes(false));
  }

  @Test
  void testReadAndToBytesClusterInfo() {
    var topic = Utils.randomString();
    try (var service = Service.builder().numberOfBrokers(3).build()) {
      try (var admin = Admin.of(service.bootstrapServers())) {
        admin
            .creator()
            .topic(topic)
            .numberOfPartitions(1)
            .numberOfReplicas((short) 3)
            .run()
            .toCompletableFuture()
            .join();
        Utils.sleep(Duration.ofSeconds(1));
        var clusterInfo = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join();

        Assertions.assertDoesNotThrow(() -> ByteUtils.toBytes(clusterInfo));
        var bytes = ByteUtils.toBytes(clusterInfo);
        Assertions.assertDoesNotThrow(() -> ByteUtils.readClusterInfo(bytes));
        var deserializedClusterInfo = ByteUtils.readClusterInfo(bytes);

        Assertions.assertEquals(clusterInfo.clusterId(), deserializedClusterInfo.clusterId());
        Assertions.assertTrue(clusterInfo.nodes().containsAll(deserializedClusterInfo.nodes()));
        Assertions.assertEquals(clusterInfo.topics(), deserializedClusterInfo.topics());
        Assertions.assertEquals(clusterInfo.replicas(), deserializedClusterInfo.replicas());
      }
    }
  }

  @Test
  void testReadAndToBytesEmptyClusterInfo() {
    var clusterInfo = ClusterInfo.empty();
    var serializedInfo = ByteUtils.toBytes(clusterInfo);
    var deserializedClusterInfo = ByteUtils.readClusterInfo(serializedInfo);

    Assertions.assertEquals(clusterInfo.clusterId(), deserializedClusterInfo.clusterId());
    Assertions.assertEquals(clusterInfo.nodes(), deserializedClusterInfo.nodes());
    Assertions.assertEquals(clusterInfo.topics(), deserializedClusterInfo.topics());
    Assertions.assertEquals(clusterInfo.replicas(), deserializedClusterInfo.replicas());
  }
}
