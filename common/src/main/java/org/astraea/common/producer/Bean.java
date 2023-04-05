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

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;

@org.apache.avro.specific.AvroGenerated
public class Bean extends org.apache.avro.specific.SpecificRecordBase
    implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -3093937929027110194L;

  public static final org.apache.avro.Schema SCHEMA$ =
      new org.apache.avro.Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"Bean\",\"namespace\":\"org.astraea.common.producer\",\"fields\":[{\"name\":\"domain\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"properties\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"avro.java.string\":\"String\",\"default\":{}}},{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"values\":[{\"type\":\"string\",\"avro.java.string\":\"String\"},\"int\",\"long\",\"null\",\"float\",\"double\",\"boolean\"],\"avro.java.string\":\"String\",\"default\":{}}}]}");

  public static org.apache.avro.Schema getClassSchema() {
    return SCHEMA$;
  }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<Bean> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Bean> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   *
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<Bean> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   *
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<Bean> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link
   * SchemaStore}.
   *
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<Bean> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this Bean to a ByteBuffer.
   *
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a Bean from a ByteBuffer.
   *
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a Bean instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of
   *     this class
   */
  public static Bean fromByteBuffer(java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private java.lang.String domain;
  private java.util.Map<java.lang.String, java.lang.String> properties;
  private java.util.Map<java.lang.String, java.lang.Object> attributes;

  /**
   * Default constructor. Note that this does not initialize fields to their default values from the
   * schema. If that is desired then one should use <code>newBuilder()</code>.
   */
  public Bean() {}

  /**
   * All-args constructor.
   *
   * @param domain The new value for domain
   * @param properties The new value for properties
   * @param attributes The new value for attributes
   */
  public Bean(
      java.lang.String domain,
      java.util.Map<java.lang.String, java.lang.String> properties,
      java.util.Map<java.lang.String, java.lang.Object> attributes) {
    this.domain = domain;
    this.properties = properties;
    this.attributes = attributes;
  }

  @Override
  public org.apache.avro.specific.SpecificData getSpecificData() {
    return MODEL$;
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter.  Applications should not call.
  @Override
  public java.lang.Object get(int field$) {
    switch (field$) {
      case 0:
        return domain;
      case 1:
        return properties;
      case 2:
        return attributes;
      default:
        throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value = "unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
      case 0:
        domain = value$ != null ? value$.toString() : null;
        break;
      case 1:
        properties = (java.util.Map<java.lang.String, java.lang.String>) value$;
        break;
      case 2:
        attributes = (java.util.Map<java.lang.String, java.lang.Object>) value$;
        break;
      default:
        throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'domain' field.
   *
   * @return The value of the 'domain' field.
   */
  public java.lang.String getDomain() {
    return domain;
  }

  /**
   * Sets the value of the 'domain' field.
   *
   * @param value the value to set.
   */
  public void setDomain(java.lang.String value) {
    this.domain = value;
  }

  /**
   * Gets the value of the 'properties' field.
   *
   * @return The value of the 'properties' field.
   */
  public java.util.Map<java.lang.String, java.lang.String> getProperties() {
    return properties;
  }

  /**
   * Sets the value of the 'properties' field.
   *
   * @param value the value to set.
   */
  public void setProperties(java.util.Map<java.lang.String, java.lang.String> value) {
    this.properties = value;
  }

  /**
   * Gets the value of the 'attributes' field.
   *
   * @return The value of the 'attributes' field.
   */
  public java.util.Map<java.lang.String, java.lang.Object> getAttributes() {
    return attributes;
  }

  /**
   * Sets the value of the 'attributes' field.
   *
   * @param value the value to set.
   */
  public void setAttributes(java.util.Map<java.lang.String, java.lang.Object> value) {
    this.attributes = value;
  }

  /**
   * Creates a new Bean RecordBuilder.
   *
   * @return A new Bean RecordBuilder
   */
  public static org.astraea.common.producer.Bean.Builder newBuilder() {
    return new org.astraea.common.producer.Bean.Builder();
  }

  /**
   * Creates a new Bean RecordBuilder by copying an existing Builder.
   *
   * @param other The existing builder to copy.
   * @return A new Bean RecordBuilder
   */
  public static org.astraea.common.producer.Bean.Builder newBuilder(
      org.astraea.common.producer.Bean.Builder other) {
    if (other == null) {
      return new org.astraea.common.producer.Bean.Builder();
    } else {
      return new org.astraea.common.producer.Bean.Builder(other);
    }
  }

  /**
   * Creates a new Bean RecordBuilder by copying an existing Bean instance.
   *
   * @param other The existing instance to copy.
   * @return A new Bean RecordBuilder
   */
  public static org.astraea.common.producer.Bean.Builder newBuilder(
      org.astraea.common.producer.Bean other) {
    if (other == null) {
      return new org.astraea.common.producer.Bean.Builder();
    } else {
      return new org.astraea.common.producer.Bean.Builder(other);
    }
  }

  /** RecordBuilder for Bean instances. */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Bean>
      implements org.apache.avro.data.RecordBuilder<Bean> {

    private java.lang.String domain;
    private java.util.Map<java.lang.String, java.lang.String> properties;
    private java.util.Map<java.lang.String, java.lang.Object> attributes;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     *
     * @param other The existing Builder to copy.
     */
    private Builder(org.astraea.common.producer.Bean.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.domain)) {
        this.domain = data().deepCopy(fields()[0].schema(), other.domain);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.properties)) {
        this.properties = data().deepCopy(fields()[1].schema(), other.properties);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.attributes)) {
        this.attributes = data().deepCopy(fields()[2].schema(), other.attributes);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing Bean instance
     *
     * @param other The existing instance to copy.
     */
    private Builder(org.astraea.common.producer.Bean other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.domain)) {
        this.domain = data().deepCopy(fields()[0].schema(), other.domain);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.properties)) {
        this.properties = data().deepCopy(fields()[1].schema(), other.properties);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.attributes)) {
        this.attributes = data().deepCopy(fields()[2].schema(), other.attributes);
        fieldSetFlags()[2] = true;
      }
    }

    /**
     * Gets the value of the 'domain' field.
     *
     * @return The value.
     */
    public java.lang.String getDomain() {
      return domain;
    }

    /**
     * Sets the value of the 'domain' field.
     *
     * @param value The value of 'domain'.
     * @return This builder.
     */
    public org.astraea.common.producer.Bean.Builder setDomain(java.lang.String value) {
      validate(fields()[0], value);
      this.domain = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
     * Checks whether the 'domain' field has been set.
     *
     * @return True if the 'domain' field has been set, false otherwise.
     */
    public boolean hasDomain() {
      return fieldSetFlags()[0];
    }

    /**
     * Clears the value of the 'domain' field.
     *
     * @return This builder.
     */
    public org.astraea.common.producer.Bean.Builder clearDomain() {
      domain = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
     * Gets the value of the 'properties' field.
     *
     * @return The value.
     */
    public java.util.Map<java.lang.String, java.lang.String> getProperties() {
      return properties;
    }

    /**
     * Sets the value of the 'properties' field.
     *
     * @param value The value of 'properties'.
     * @return This builder.
     */
    public org.astraea.common.producer.Bean.Builder setProperties(
        java.util.Map<java.lang.String, java.lang.String> value) {
      validate(fields()[1], value);
      this.properties = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
     * Checks whether the 'properties' field has been set.
     *
     * @return True if the 'properties' field has been set, false otherwise.
     */
    public boolean hasProperties() {
      return fieldSetFlags()[1];
    }

    /**
     * Clears the value of the 'properties' field.
     *
     * @return This builder.
     */
    public org.astraea.common.producer.Bean.Builder clearProperties() {
      properties = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
     * Gets the value of the 'attributes' field.
     *
     * @return The value.
     */
    public java.util.Map<java.lang.String, java.lang.Object> getAttributes() {
      return attributes;
    }

    /**
     * Sets the value of the 'attributes' field.
     *
     * @param value The value of 'attributes'.
     * @return This builder.
     */
    public org.astraea.common.producer.Bean.Builder setAttributes(
        java.util.Map<java.lang.String, java.lang.Object> value) {
      validate(fields()[2], value);
      this.attributes = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
     * Checks whether the 'attributes' field has been set.
     *
     * @return True if the 'attributes' field has been set, false otherwise.
     */
    public boolean hasAttributes() {
      return fieldSetFlags()[2];
    }

    /**
     * Clears the value of the 'attributes' field.
     *
     * @return This builder.
     */
    public org.astraea.common.producer.Bean.Builder clearAttributes() {
      attributes = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Bean build() {
      try {
        Bean record = new Bean();
        record.domain =
            fieldSetFlags()[0] ? this.domain : (java.lang.String) defaultValue(fields()[0]);
        record.properties =
            fieldSetFlags()[1]
                ? this.properties
                : (java.util.Map<java.lang.String, java.lang.String>) defaultValue(fields()[1]);
        record.attributes =
            fieldSetFlags()[2]
                ? this.attributes
                : (java.util.Map<java.lang.String, java.lang.Object>) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Bean> WRITER$ =
      (org.apache.avro.io.DatumWriter<Bean>) MODEL$.createDatumWriter(SCHEMA$);

  @Override
  public void writeExternal(java.io.ObjectOutput out) throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Bean> READER$ =
      (org.apache.avro.io.DatumReader<Bean>) MODEL$.createDatumReader(SCHEMA$);

  @Override
  public void readExternal(java.io.ObjectInput in) throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }
}
