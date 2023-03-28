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
import org.junit.jupiter.api.Test;

public class BeanObjectSerializerTest {
  @Test
  void testing() throws IOException {
    var bean = new BeanObject("topicA", Map.of("name", "publisher"), Map.of("value", "3"));

    var builder = BeanObjectOuterClass.BeanObject.newBuilder().setTopic(bean.domainName());
    bean.properties().forEach((key, val) -> builder.addProperties(property(key, val)));
    bean.attributes().forEach((key, val) -> builder.addAttributes(attribute(key, val)));
    var beanOuter = builder.build();

    var os = new ByteArrayOutputStream();
    beanOuter.writeTo(os);

    var byteArr = os.toByteArray();
    System.out.println("  serialized size: " + byteArr.length + " bytes");
  }

  private BeanObjectOuterClass.BeanObject.Property property(String k, String v) {
    return BeanObjectOuterClass.BeanObject.Property.newBuilder().setKey(k).setValue(v).build();
  }

  private BeanObjectOuterClass.BeanObject.Attribute attribute(String k, Object v) {
    var tmpBuilder = BeanObjectOuterClass.BeanObject.Attribute.newBuilder().setKey(k);
    if (v instanceof Integer) {
      return tmpBuilder
          .setObj(BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setInt((int) v).build())
          .build();
    } else if (v instanceof Long) {
      return tmpBuilder
          .setObj(BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setLong((long) v).build())
          .build();
    } else if (v instanceof Float) {
      return tmpBuilder
          .setObj(
              BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setFloat((float) v).build())
          .build();
    } else if (v instanceof Double) {
      return tmpBuilder
          .setObj(
              BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setDouble((double) v).build())
          .build();
    } else if (v instanceof Boolean) {
      return tmpBuilder
          .setObj(
              BeanObjectOuterClass.BeanObject.Primitive.newBuilder()
                  .setBoolean((boolean) v)
                  .build())
          .build();
    } else {
      return tmpBuilder
          .setObj(
              BeanObjectOuterClass.BeanObject.Primitive.newBuilder().setStr(v.toString()).build())
          .build();
    }
  }
}
