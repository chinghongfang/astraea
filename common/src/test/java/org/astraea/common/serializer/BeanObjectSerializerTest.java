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
package org.astraea.common.serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.producer.Serializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BeanObjectSerializerTest {
  @Test
  void testSerialize() {
    var bean = new BeanObject("d", Map.of(), Map.of());
    var serializer = new Serializer.BeanSerializer();

    // 0x00, 0x01 => domain length is 1
    // 0x64       => "d"
    // 0x00       => no properties
    // 0x00       => no attributes
    var bytes = new byte[] {0x00, 0x01, 0x64, 0x00, 0x00};
    Assertions.assertArrayEquals(bytes, serializer.serialize("ignore", List.of(), bean));

    bean = new BeanObject("d", Map.of("a", "bc"), Map.of());
    // 0x00, 0x01 => domain length is 1
    // 0x64       => "d"
    // 0x01       => 1 properties
    // 0x00, 0x01 => key length is 1
    // 0x61       => "a"
    // 0x00, 0x02 => value length is 2
    // 0x62, 0x63 => "bc"
    // 0x00       => no attributes
    bytes = new byte[] {0x00, 0x01, 0x64, 0x01, 0x00, 0x01, 0x61, 0x00, 0x02, 0x62, 0x63, 0x00};
    Assertions.assertArrayEquals(bytes, serializer.serialize("ignore", List.of(), bean));

    bean = new BeanObject("d", Map.of("a", "b"), Map.of("a", "bc", "b", 5L));
    // 0x00, 0x01 => domain length is 1
    // 0x64       => "d"
    // 0x01       => 1 properties
    // 0x00, 0x01 => key length is 1
    // 0x61       => "a"
    // 0x00, 0x01 => value length is 1
    // 0x62       => "b"
    // 0x02       => 2 attributes
    // 0x00, 0x01 => key length is 1
    // 0x61       => "a"
    // 0x09       => type String
    // 0x00, 0x02 => value length is 2
    // 0x62, 0x63 => "bc"
    // 0x00, 0x01 => key length is 1
    // 0x61       => "a"
    // 0x04       => type long
    // 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05 => 5L
    bytes =
        new byte[] {
          0x00, 0x01, 0x64, 0x01, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62, 0x02, 0x00, 0x01, 0x61, 0x09,
          0x00, 0x02, 0x62, 0x63, 0x00, 0x01, 0x62, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x05
        };
    Assertions.assertArrayEquals(bytes, serializer.serialize("ignore", List.of(), bean));
  }

  @Test
  void testBeanSize(){
    var bean = new BeanObject("topicA", Map.of("name", "publisher"), Map.of("value", "3"));
    var serializer = new Serializer.BeanSerializer();
    List<byte[]> trash = new ArrayList<>(1000);

    var start = System.currentTimeMillis();
    for (int i = 0; i<100_000; ++i){
      for (int j = 0; j<1_000; ++j){
        trash.add(serializer.serialize("", List.of(), bean));
      }
      trash.clear();
    }

    System.out.println("  serialize time: "+ (System.currentTimeMillis() - start) );
  }
}
