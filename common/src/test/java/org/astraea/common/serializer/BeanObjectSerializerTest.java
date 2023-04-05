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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.astraea.common.BeanObjectOuterClass;
import org.astraea.common.metrics.BeanObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BeanObjectSerializerTest {
  @Test
  void testing() throws IOException {
    var bean =
        new BeanObject("topicA", Map.of("name", "publisher"), Map.of("count", 3, "mean", 2.4));

    var os = new ByteArrayOutputStream();
    var start = System.currentTimeMillis();
    for (int i = 0; i < 100_000_000; ++i) {
      os.reset();
      var builder = BeanObjectOuterClass.BeanObject.newBuilder().setTopic(bean.domainName());
      builder.putAllProperties(bean.properties());
      bean.attributes().forEach((key, val) -> builder.putAttributes(key, attribute(val)));
      var beanOuter = builder.build();

      beanOuter.writeTo(os);

      var deserialized = BeanObjectOuterClass.BeanObject.parseFrom(os.toByteArray());
      Assertions.assertEquals(bean.domainName(), deserialized.getTopic());
    }
    System.out.println("  serialize time: " + (System.currentTimeMillis() - start) + " ms");
  }

  private BeanObjectOuterClass.BeanObject.Primitive attribute(Object v) {
    if (v instanceof Integer) {
      return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setInt((int) v).build();
    } else if (v instanceof Long) {
      return BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setLong((long) v).build();
    } else if (v instanceof Float) {
      return
              BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setFloat((float) v).build();
    } else if (v instanceof Double) {
      return
              BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setDouble((double) v).build();
    } else if (v instanceof Boolean) {
      return
              BeanObjectOuterClass.BeanObject.Primitive.newBuilder()
                  .setBoolean((boolean) v)
                  .build();
    } else {
      return
              BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setStr(v.toString()).build();
    }
  }
}
