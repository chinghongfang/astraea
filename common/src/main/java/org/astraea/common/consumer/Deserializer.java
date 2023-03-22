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
package org.astraea.common.consumer;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.astraea.common.Header;
import org.astraea.common.backup.ByteUtils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.BeanObject;

@FunctionalInterface
public interface Deserializer<T> {

  /**
   * Deserialize a record value from a byte array into a value or object.
   *
   * @param topic topic associated with the data
   * @param headers headers associated with the record; may be empty.
   * @param data serialized bytes; may be null; implementations are recommended to handle null by
   *     returning a value or null rather than throwing an exception.
   * @return deserialized typed data; may be null
   */
  T deserialize(String topic, List<Header> headers, byte[] data);

  static <T> org.apache.kafka.common.serialization.Deserializer<T> of(
      Deserializer<T> deserializer) {
    return new org.apache.kafka.common.serialization.Deserializer<>() {

      @Override
      public T deserialize(String topic, byte[] data) {
        return deserializer.deserialize(topic, List.of(), data);
      }

      @Override
      public T deserialize(String topic, Headers headers, byte[] data) {
        return deserializer.deserialize(topic, Header.of(headers), data);
      }
    };
  }

  static <T> Deserializer<T> of(
      org.apache.kafka.common.serialization.Deserializer<T> deserializer) {
    return (topic, headers, data) -> deserializer.deserialize(topic, Header.of(headers), data);
  }

  Deserializer<String> BASE64 =
      (topic, headers, data) -> data == null ? null : Base64.getEncoder().encodeToString(data);
  Deserializer<byte[]> BYTE_ARRAY = of(new ByteArrayDeserializer());
  Deserializer<String> STRING = of(new StringDeserializer());
  Deserializer<Integer> INTEGER = of(new IntegerDeserializer());
  Deserializer<Long> LONG = of(new LongDeserializer());
  Deserializer<Float> FLOAT = of(new FloatDeserializer());
  Deserializer<Double> DOUBLE = of(new DoubleDeserializer());

  /**
   * create Custom JsonDeserializer
   *
   * @param typeRef The typeRef of message being output by the Deserializer
   * @return Custom JsonDeserializer
   * @param <T> The type of message being output by the Deserializer
   */
  static <T> Deserializer<T> of(TypeRef<T> typeRef) {
    return new JsonDeserializer<>(typeRef);
  }

  class JsonDeserializer<T> implements Deserializer<T> {
    private final TypeRef<T> typeRef;
    private final JsonConverter jackson = JsonConverter.jackson();

    private JsonDeserializer(TypeRef<T> typeRef) {
      this.typeRef = typeRef;
    }

    @Override
    public T deserialize(String topic, List<Header> headers, byte[] data) {
      if (data == null) return null;
      else {
        return jackson.fromJson(Deserializer.STRING.deserialize(topic, headers, data), typeRef);
      }
    }
  }

  class BeanDeserializer implements Deserializer<BeanObject> {
    @Override
    public BeanObject deserialize(String topic, List<Header> headers, byte[] data) {
      var byteBuffer = ByteBuffer.wrap(data);

      final short domainNameLen = byteBuffer.getShort();
      var domainName = ByteUtils.readString(byteBuffer, domainNameLen);

      final int propertiesSize = byteBuffer.get();
      Map<String, String> properties = new HashMap<>();
      for (int i = 0; i < propertiesSize; ++i) {
        final short keyLen = byteBuffer.getShort();
        var key = ByteUtils.readString(byteBuffer, keyLen);
        final short valueLen = byteBuffer.getShort();
        var value = ByteUtils.readString(byteBuffer, valueLen);
        properties.put(key, value);
      }

      final int attributesSize = byteBuffer.get();
      Map<String, Object> attributes = new HashMap<>();
      for (int i = 0; i < attributesSize; ++i) {
        final short keyLen = byteBuffer.getShort();
        var key = ByteUtils.readString(byteBuffer, keyLen);
        final byte type = byteBuffer.get();
        switch (type) {
          case 1:
            attributes.put(key, byteBuffer.get());
            break;
          case 2:
            attributes.put(key, byteBuffer.getShort());
            break;
          case 3:
            attributes.put(key, byteBuffer.getInt());
            break;
          case 4:
            attributes.put(key, byteBuffer.getLong());
            break;
          case 5:
            attributes.put(key, byteBuffer.getFloat());
            break;
          case 6:
            attributes.put(key, byteBuffer.getDouble());
            break;
          case 7:
            attributes.put(key, byteBuffer.get() != 0x00);
            break;
          case 8:
            attributes.put(key, byteBuffer.getChar());
            break;
          case 9:
            var strLen = byteBuffer.getShort();
            attributes.put(key, ByteUtils.readString(byteBuffer, strLen));
            break;
        }
      }
      return new BeanObject(domainName, properties, attributes);
    }
  }
}
