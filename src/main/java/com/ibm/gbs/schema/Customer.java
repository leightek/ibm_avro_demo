/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.ibm.gbs.schema;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Customer extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -6847842158517248348L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Customer\",\"namespace\":\"com.ibm.gbs.schema\",\"fields\":[{\"name\":\"customerId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"phoneNumber\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"accountId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<Customer> ENCODER =
      new BinaryMessageEncoder<Customer>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Customer> DECODER =
      new BinaryMessageDecoder<Customer>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<Customer> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<Customer> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<Customer>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this Customer to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a Customer from a ByteBuffer. */
  public static Customer fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private String customerId;
   private String name;
   private String phoneNumber;
   private String accountId;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Customer() {}

  /**
   * All-args constructor.
   * @param customerId The new value for customerId
   * @param name The new value for name
   * @param phoneNumber The new value for phoneNumber
   * @param accountId The new value for accountId
   */
  public Customer(String customerId, String name, String phoneNumber, String accountId) {
    this.customerId = customerId;
    this.name = name;
    this.phoneNumber = phoneNumber;
    this.accountId = accountId;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return customerId;
    case 1: return name;
    case 2: return phoneNumber;
    case 3: return accountId;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: customerId = (String)value$; break;
    case 1: name = (String)value$; break;
    case 2: phoneNumber = (String)value$; break;
    case 3: accountId = (String)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'customerId' field.
   * @return The value of the 'customerId' field.
   */
  public String getCustomerId() {
    return customerId;
  }


  /**
   * Gets the value of the 'name' field.
   * @return The value of the 'name' field.
   */
  public String getName() {
    return name;
  }


  /**
   * Gets the value of the 'phoneNumber' field.
   * @return The value of the 'phoneNumber' field.
   */
  public String getPhoneNumber() {
    return phoneNumber;
  }


  /**
   * Gets the value of the 'accountId' field.
   * @return The value of the 'accountId' field.
   */
  public String getAccountId() {
    return accountId;
  }


  /**
   * Creates a new Customer RecordBuilder.
   * @return A new Customer RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new Customer RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Customer RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    return new Builder(other);
  }

  /**
   * Creates a new Customer RecordBuilder by copying an existing Customer instance.
   * @param other The existing instance to copy.
   * @return A new Customer RecordBuilder
   */
  public static Builder newBuilder(Customer other) {
    return new Builder(other);
  }

  /**
   * RecordBuilder for Customer instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Customer>
    implements org.apache.avro.data.RecordBuilder<Customer> {

    private String customerId;
    private String name;
    private String phoneNumber;
    private String accountId;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.customerId)) {
        this.customerId = data().deepCopy(fields()[0].schema(), other.customerId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.name)) {
        this.name = data().deepCopy(fields()[1].schema(), other.name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.phoneNumber)) {
        this.phoneNumber = data().deepCopy(fields()[2].schema(), other.phoneNumber);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.accountId)) {
        this.accountId = data().deepCopy(fields()[3].schema(), other.accountId);
        fieldSetFlags()[3] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing Customer instance
     * @param other The existing instance to copy.
     */
    private Builder(Customer other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.customerId)) {
        this.customerId = data().deepCopy(fields()[0].schema(), other.customerId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.name)) {
        this.name = data().deepCopy(fields()[1].schema(), other.name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.phoneNumber)) {
        this.phoneNumber = data().deepCopy(fields()[2].schema(), other.phoneNumber);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.accountId)) {
        this.accountId = data().deepCopy(fields()[3].schema(), other.accountId);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'customerId' field.
      * @return The value.
      */
    public String getCustomerId() {
      return customerId;
    }

    /**
      * Sets the value of the 'customerId' field.
      * @param value The value of 'customerId'.
      * @return This builder.
      */
    public Builder setCustomerId(String value) {
      validate(fields()[0], value);
      this.customerId = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'customerId' field has been set.
      * @return True if the 'customerId' field has been set, false otherwise.
      */
    public boolean hasCustomerId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'customerId' field.
      * @return This builder.
      */
    public Builder clearCustomerId() {
      customerId = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'name' field.
      * @return The value.
      */
    public String getName() {
      return name;
    }

    /**
      * Sets the value of the 'name' field.
      * @param value The value of 'name'.
      * @return This builder.
      */
    public Builder setName(String value) {
      validate(fields()[1], value);
      this.name = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'name' field has been set.
      * @return True if the 'name' field has been set, false otherwise.
      */
    public boolean hasName() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'name' field.
      * @return This builder.
      */
    public Builder clearName() {
      name = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'phoneNumber' field.
      * @return The value.
      */
    public String getPhoneNumber() {
      return phoneNumber;
    }

    /**
      * Sets the value of the 'phoneNumber' field.
      * @param value The value of 'phoneNumber'.
      * @return This builder.
      */
    public Builder setPhoneNumber(String value) {
      validate(fields()[2], value);
      this.phoneNumber = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'phoneNumber' field has been set.
      * @return True if the 'phoneNumber' field has been set, false otherwise.
      */
    public boolean hasPhoneNumber() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'phoneNumber' field.
      * @return This builder.
      */
    public Builder clearPhoneNumber() {
      phoneNumber = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'accountId' field.
      * @return The value.
      */
    public String getAccountId() {
      return accountId;
    }

    /**
      * Sets the value of the 'accountId' field.
      * @param value The value of 'accountId'.
      * @return This builder.
      */
    public Builder setAccountId(String value) {
      validate(fields()[3], value);
      this.accountId = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'accountId' field has been set.
      * @return True if the 'accountId' field has been set, false otherwise.
      */
    public boolean hasAccountId() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'accountId' field.
      * @return This builder.
      */
    public Builder clearAccountId() {
      accountId = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Customer build() {
      try {
        Customer record = new Customer();
        record.customerId = fieldSetFlags()[0] ? this.customerId : (String) defaultValue(fields()[0]);
        record.name = fieldSetFlags()[1] ? this.name : (String) defaultValue(fields()[1]);
        record.phoneNumber = fieldSetFlags()[2] ? this.phoneNumber : (String) defaultValue(fields()[2]);
        record.accountId = fieldSetFlags()[3] ? this.accountId : (String) defaultValue(fields()[3]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Customer>
    WRITER$ = (org.apache.avro.io.DatumWriter<Customer>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Customer>
    READER$ = (org.apache.avro.io.DatumReader<Customer>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
