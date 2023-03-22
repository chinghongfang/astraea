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
package org.astraea.common.producer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.backup.ByteUtils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.BeanObject;

@FunctionalInterface
public interface Serializer<T> {

  /**
   * Convert {@code data} into a byte array.
   *
   * @param topic topic associated with data
   * @param headers headers associated with the record
   * @param data typed data
   * @return serialized bytes
   */
  byte[] serialize(String topic, Collection<Header> headers, T data);

  static <T> org.apache.kafka.common.serialization.Serializer<T> of(Serializer<T> serializer) {
    return new org.apache.kafka.common.serialization.Serializer<>() {

      @Override
      public byte[] serialize(String topic, T data) {
        return serializer.serialize(topic, List.of(), data);
      }

      @Override
      public byte[] serialize(String topic, Headers headers, T data) {
        return serializer.serialize(topic, Header.of(headers), data);
      }
    };
  }

  static <T> Serializer<T> of(org.apache.kafka.common.serialization.Serializer<T> serializer) {
    return (topic, headers, data) -> serializer.serialize(topic, Header.of(headers), data);
  }

  Serializer<byte[]> BYTE_ARRAY = of(new ByteArraySerializer());
  Serializer<String> STRING = of(new StringSerializer());
  Serializer<Integer> INTEGER = of(new IntegerSerializer());
  Serializer<Long> LONG = of(new LongSerializer());
  Serializer<Float> FLOAT = of(new FloatSerializer());
  Serializer<Double> DOUBLE = of(new DoubleSerializer());

  /**
   * create Custom JsonSerializer
   *
   * @return Custom JsonSerializer
   * @param <T> The type of message being output by the serializer
   */
  static <T> Serializer<T> of(TypeRef<T> typeRef) {
    return new JsonSerializer<>(typeRef);
  }

  class JsonSerializer<T> implements Serializer<T> {
    private final String encoding = StandardCharsets.UTF_8.name();
    private final JsonConverter jackson = JsonConverter.jackson();

    private JsonSerializer(TypeRef<T> typeRef) {}

    @Override
    public byte[] serialize(String topic, Collection<Header> headers, T data) {
      if (data == null) {
        return null;
      }

      return Utils.packException(() -> jackson.toJson(data).getBytes(encoding));
    }
  }

  /**
   * Serialize `BeanObject` by domain name, properties, and attributes.
   *
   * <p>Serialize the map. If the key or value in map is not java primitive type, convert it to
   * string and then convert it to byte array.
   */
  class BeanSerializer implements Serializer<BeanObject> {
    @Override
    public byte[] serialize(String topic, Collection<Header> headers, BeanObject data) {
      final int propertiesLen =
          data.properties().entrySet().stream()
              .mapToInt(e -> 2 + e.getKey().getBytes().length + 2 + e.getValue().getBytes().length)
              .sum();
      final int attributesLen =
          data.attributes().entrySet().stream()
              // 2 (string length) + ? (string) + 1 (type) + ? (object)
              .mapToInt(e -> 2 + e.getKey().getBytes().length + 1 + primitiveLen(e.getValue()))
              .sum();

      final int size =
          2 + data.domainName().getBytes().length + 1 + propertiesLen + 1 + attributesLen;
      var byteBuffer = ByteBuffer.allocate(size);

      // Serialize the domain name
      ByteUtils.putLengthString(byteBuffer, data.domainName());

      // Serialize the properties
      // The number of key-value pair (<= 127)
      byteBuffer.put((byte) data.properties().entrySet().size());
      data.properties()
          .forEach(
              (key, value) -> {
                ByteUtils.putLengthString(byteBuffer, key);
                ByteUtils.putLengthString(byteBuffer, value);
              });

      /*
       * Serialize the attributes. The type of BeanObject attribute
       * value may be double or integer. For example, the type of attribute "count" is integer, but
       * the type of attribute "meanRate" is double.
       */
      byteBuffer.put((byte) data.attributes().entrySet().size());
      data.attributes()
          .forEach(
              (key, value) -> {
                ByteUtils.putLengthString(byteBuffer, key);
                final byte type = primitiveId(value);
                byteBuffer.put(type);
                switch (type) {
                  case 1:
                  case 8:
                    byteBuffer.put((byte) value);
                    break;
                  case 2:
                    byteBuffer.put(ByteUtils.toBytes((short) value));
                    break;
                  case 3:
                    byteBuffer.put(ByteUtils.toBytes((int) value));
                    break;
                  case 4:
                    byteBuffer.put(ByteUtils.toBytes((long) value));
                    break;
                  case 5:
                    byteBuffer.put(ByteUtils.toBytes((float) value));
                    break;
                  case 6:
                    byteBuffer.put(ByteUtils.toBytes((double) value));
                    break;
                  case 7:
                    byteBuffer.put(ByteUtils.toBytes((boolean) value));
                  default:
                    ByteUtils.putLengthString(byteBuffer, value.toString());
                }
              });

      return byteBuffer.array();
    }

    /** Convert given primitive to length in byte-array expression */
    private int primitiveLen(Object value) {
      if (value.getClass().equals(Byte.class)) {
        return 1;
      } else if (value.getClass().equals(Short.class)) {
        return 2;
      } else if (value.getClass().equals(Integer.class)) {
        return 4;
      } else if (value.getClass().equals(Long.class)) {
        return 8;
      } else if (value.getClass().equals(Float.class)) {
        return 4;
      } else if (value.getClass().equals(Double.class)) {
        return 8;
      } else if (value.getClass().equals(Boolean.class)) {
        return 1;
      } else if (value.getClass().equals(Character.class)) {
        return 1;
      } else {
        // Otherwise serialize by string
        // 2 bytes added for length of string-bytes
        return 2 + value.toString().getBytes().length;
      }
    }

    private byte primitiveId(Object value) {
      if (value.getClass().equals(Byte.class)) {
        return 1;
      } else if (value.getClass().equals(Short.class)) {
        return 2;
      } else if (value.getClass().equals(Integer.class)) {
        return 3;
      } else if (value.getClass().equals(Long.class)) {
        return 4;
      } else if (value.getClass().equals(Float.class)) {
        return 5;
      } else if (value.getClass().equals(Double.class)) {
        return 6;
      } else if (value.getClass().equals(Boolean.class)) {
        return 7;
      } else if (value.getClass().equals(Character.class)) {
        return 8;
      } else {
        // Otherwise
        return 9;
      }
    }
  }
}
