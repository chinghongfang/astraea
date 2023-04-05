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
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.astraea.common.Utils;
import org.astraea.common.producer.Bean;
import org.junit.jupiter.api.Test;

public class BeanObjectSerializerTest {
  @Test
  void testing() throws IOException {
    String domain = "topicA";
    Map<CharSequence, CharSequence> properties = Map.of("name", "publisher");
    Map<CharSequence, Object> attributes = Map.of("value", "3");

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(os, null);
    DatumWriter<Bean> beanWriter = new SpecificDatumWriter<>(Bean.class);
    // List<byte[]> trash = new ArrayList<>(1000);

    long start = System.currentTimeMillis();
    //for (int i = 0; i < 100_000_000; ++i) {
      os.reset();
      Bean.Builder builder = Bean.newBuilder().setDomain(Utils.randomString(5));
      builder.setProperties(properties);
      builder.setAttributes(attributes);
      Bean bean = builder.build();

      beanWriter.write(bean, encoder);
      encoder.flush();
    //}
    var bytes = os.toByteArray();
    System.out.println("  serialize time: " + (System.currentTimeMillis() - start) + " ms");
    System.out.println("  serialized size: "+ bytes.length);
  }
}
