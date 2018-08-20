/*
 * Copyright © 2014-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.api.data.format;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.LogicalType;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Instance of a record structured by a {@link Schema}. Fields are accessible by name.
 */
@Beta
public class StructuredRecord implements Serializable {
  private static final SimpleDateFormat DEFAULT_FORMAT = new SimpleDateFormat("YYYY-MM-DD'T'HH:mm:ss z");
  private final Schema schema;
  private final Map<String, Object> fields;

  private static final long serialVersionUID = 4599226147667105318L;

  {
    DEFAULT_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  private StructuredRecord(Schema schema, Map<String, Object> fields) {
    this.schema = schema;
    this.fields = fields;
  }

  /**
   * Get the schema of the record.
   *
   * @return schema of the record.
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Get the value of a field in the record.
   *
   * @param fieldName field to get.
   * @param <T> type of object of the field value.
   * @return value of the field.
   */
  @SuppressWarnings("unchecked")
  public <T> T get(String fieldName) {
    return (T) fields.get(fieldName);
  }

  /**
   * Get {@link LocalDate} from field of logical type date.
   *
   * @param fieldName date field to get.
   * @return value of the field as an object of {@link LocalDate}, returns null if given field has null value
   * @throws IllegalArgumentException if the field name is not present in schema or the field provided
   *                                  is not of {@link LogicalType#DATE} type.
   */
  @Nullable
  public LocalDate getDate(String fieldName) {
    validateAndGetLogicalType(fieldName, schema.getField(fieldName), EnumSet.of(LogicalType.DATE));
    Object value = fields.get(fieldName);
    return value == null ? null : LocalDate.ofEpochDay(((Integer) value).longValue());
  }

  /**
   * Get {@link LocalTime} from field of logical type time.
   *
   * @param fieldName time field to get.
   * @return value of the field as an object of {@link LocalTime}, returns null if given field has null value
   * @throws IllegalArgumentException if the field name is not present in schema or the field provided
   *                                  is not of {@link LogicalType#TIME_MILLIS} or
   *                                  {@link LogicalType#TIME_MICROS} type.
   */
  @Nullable
  public LocalTime getTime(String fieldName) {
    LogicalType logicalType =  validateAndGetLogicalType(fieldName, schema.getField(fieldName),
                                                                EnumSet.of(LogicalType.TIME_MILLIS,
                                                                           LogicalType.TIME_MICROS));
    Object value = fields.get(fieldName);
    if (value == null) {
      return null;
    }

    if (logicalType == LogicalType.TIME_MILLIS) {
      return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(((Integer) value)));
    }

    return LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos((Long) value));
  }

  /**
   * Get {@link ZonedDateTime} from field of logical type timestamp. Default {@link ZoneId} is system default.
   *
   * @param fieldName zoned date time field to get.
   * @return value of the field as an object of {@link ZonedDateTime}, returns null if the field has null value
   * logical type timestamp
   * @throws IllegalArgumentException if the field name is not found in the map of fields or the field provided
   *                                  is not of {@link LogicalType#TIMESTAMP_MILLIS} or
   *                                  {@link LogicalType#TIMESTAMP_MICROS} type.
   */
  @Nullable
  public ZonedDateTime getTimestamp(String fieldName) {
   return getTimestamp(fieldName, ZoneId.systemDefault());
  }

  /**
   * Get {@link ZonedDateTime} from field of logical type timestamp.
   *
   * @param fieldName zoned date time field to get.
   * @param zoneId    zone id for the field
   * @return value of the field as an object of {@link ZonedDateTime}
   * @throws IllegalArgumentException if the field name is not found in the map of fields or the field provided
   *                                  is not of {@link LogicalType#TIMESTAMP_MILLIS} or
   *                                  {@link LogicalType#TIMESTAMP_MICROS} type.
   */
  @Nullable
  public ZonedDateTime getTimestamp(String fieldName, ZoneId zoneId) {
    LogicalType logicalType = validateAndGetLogicalType(fieldName, schema.getField(fieldName),
                                                               EnumSet.of(LogicalType.TIMESTAMP_MILLIS,
                                                                          LogicalType.TIMESTAMP_MICROS));
    Object value = fields.get(fieldName);
    if (value == null) {
      return null;
    }

    if (logicalType == LogicalType.TIMESTAMP_MILLIS) {
      return getZonedDateTime((long) value, zoneId, TimeUnit.MILLISECONDS);
    }

    return getZonedDateTime((long) value, zoneId, TimeUnit.MICROSECONDS);
  }

  /**
   * Get zoned date and time represented by the field.
   *
   * @param zoneId    zone id for the field
   * @return {@link ZonedDateTime} represented by field.
   */
  private ZonedDateTime getZonedDateTime(long ts, ZoneId zoneId, TimeUnit unit) {
    // get fractional portion of timestamp, we always store timestamp in micro seconds, so first get that portion.
    // for example: timestamp is 1970-01-01T00:00:01.112456Z, here fraction = 112456, tsInSeconds = 1
    long divisor = unit == TimeUnit.MICROSECONDS ? 1000000L : 1000L;
    int fraction = (int) (ts % divisor);
    long tsInSeconds = unit.toSeconds(ts);
    // create an Instant with time in seconds and fraction which will be stored as nano seconds.
    Instant instant = Instant.ofEpochSecond(tsInSeconds, unit.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, zoneId);
  }

  /**
   * Validate and get underlying logical type if the schema is a union type.
   *
   * @param fieldName    name of the field
   * @param field        field with logical type
   * @param logicalTypes acceptable logical types
   * @return {@link LogicalType} underlying logical type that field represents
   * @throws IllegalArgumentException if the field name is not found in the map of fields or the field provided
   *                                  is not an acceptable type
   */
  private static LogicalType validateAndGetLogicalType(String fieldName, Schema.Field field,
                                                       EnumSet<LogicalType> logicalTypes) {
    if (field == null) {
      throw new IllegalArgumentException(String.format("Field %s provided does not exist in the schema.", fieldName));
    }

    Schema fieldSchema = field.getSchema();

    if (fieldSchema.getType() != Schema.Type.UNION) {
      if (!fieldSchema.getType().isSimpleType() || fieldSchema.getLogicalType() == null) {
        throw new IllegalArgumentException(String.format("Field %s does not have a logical type.", fieldName));
      }

      if (!logicalTypes.contains(fieldSchema.getLogicalType())) {
        throw new IllegalArgumentException(String.format("Field %s must be of logical type %s, " +
                                                           "instead it is of type %s",
                                                         fieldName, logicalTypes,
                                                         fieldSchema.getLogicalType()));
      }
    }

    LogicalType parsedType = getLogicalTypeFromUnionSchema(fieldSchema, logicalTypes);
    if (parsedType != null) {
      return parsedType;
    }

    return fieldSchema.getLogicalType();
  }

  /**
   * Parse through logical type and get underlying {@link LogicalType}
   * @param fieldSchema name of the field
   * @param logicalTypes acceptable logical types
   * @return {@link LogicalType} represented by the union schema
   */
  private static LogicalType getLogicalTypeFromUnionSchema(Schema fieldSchema, EnumSet<LogicalType> logicalTypes) {
    if (fieldSchema.getType().isSimpleType()) {
      if (fieldSchema.getLogicalType() == null || !logicalTypes.contains(fieldSchema.getLogicalType())) {
        return null;
      }
    }

    if (fieldSchema.getType() == Schema.Type.UNION) {
      for (Schema unionSchema : fieldSchema.getUnionSchemas()) {
        if (unionSchema.getLogicalType() != null && logicalTypes.contains(unionSchema.getLogicalType())) {
          return unionSchema.getLogicalType();
        }

        LogicalType logicalType = getLogicalTypeFromUnionSchema(unionSchema, logicalTypes);
        if (logicalType != null) {
          return logicalType;
        }
      }
    }

    return null;
  }

  /**
   * Get a builder for creating a record with the given schema.
   *
   * @param schema schema for the record to build.
   * @return builder for creating a record with the given schema.
   * @throws UnexpectedFormatException if the given schema is not a record with at least one field.
   */
  public static Builder builder(Schema schema) throws UnexpectedFormatException {
    if (schema == null || schema.getType() != Schema.Type.RECORD || schema.getFields().size() < 1) {
      throw new UnexpectedFormatException("Schema must be a record with at least one field.");
    }
    return new Builder(schema);
  }

  /**
   * Builder for creating a {@link StructuredRecord}.
   * TODO: enforce schema correctness?
   */
  public static class Builder {
    private final Schema schema;
    private Map<String, Object> fields;

    private Builder(Schema schema) {
      this.schema = schema;
      this.fields = new HashMap<>();
    }

    /**
     * Set the field to the given value.
     *
     * @param fieldName name of the field to set
     * @param value value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given
     */
    public Builder set(String fieldName, @Nullable Object value) {
      createField(fieldName, value);
      fields.put(fieldName, value);
      return this;
    }

    /**
     * Sets the date value for {@link LogicalType#DATE} field
     *
     * @param fieldName name of the field to set
     * @param localDate value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given
     * @throws IllegalArgumentException if the provided date is an invalid date
     */
    public Builder setDate(String fieldName, @Nullable LocalDate localDate) {
      Schema.Field field = createField(fieldName, localDate);
      validateAndGetLogicalType(fieldName, field, EnumSet.of(LogicalType.DATE));
      if (localDate == null) {
        fields.put(fieldName, null);
        return this;
      }
      try {
        fields.put(fieldName, Math.toIntExact(localDate.toEpochDay()));
      } catch (ArithmeticException e) {
        // Highest integer is 2,147,483,647 which is Jan 1 2038.
        throw new IllegalArgumentException(String.format("Field %s is setting an invalid date. " +
                                                           "Valid date should be below Jan 1 2038", fieldName));
      }
      return this;
    }

    /**
     * Sets the time value for {@link LogicalType#TIME_MILLIS} or {@link LogicalType#TIME_MICROS} field
     *
     * @param fieldName name of the field to set
     * @param localTime value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given
     */
    public Builder setTime(String fieldName, @Nullable LocalTime localTime) {
      Schema.Field field = createField(fieldName, localTime);
      LogicalType logicalType = validateAndGetLogicalType(fieldName, field, EnumSet.of(LogicalType.TIME_MILLIS,
                                                                                       LogicalType.TIME_MICROS));

      if (localTime == null) {
        fields.put(fieldName, null);
        return this;
      }

      long nanos = localTime.toNanoOfDay();
      if (logicalType == LogicalType.TIME_MILLIS) {
        int millis = (int) TimeUnit.NANOSECONDS.toMillis(nanos);
        fields.put(fieldName, millis);
        return this;
      }

      long micros = TimeUnit.NANOSECONDS.toMicros(nanos);
      fields.put(fieldName, micros);
      return this;

    }

    /**
     * Sets the timestamp value for {@link LogicalType#TIMESTAMP_MILLIS} or
     * {@link LogicalType#TIMESTAMP_MICROS} field
     *
     * @param fieldName name of the field to set
     * @param zonedDateTime value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given
     * @throws IllegalArgumentException if the provided date is an invalid timestamp
     */
    public Builder setTimestamp(String fieldName, @Nullable ZonedDateTime zonedDateTime) {
      Schema.Field field = createField(fieldName, zonedDateTime);
      LogicalType logicalType = validateAndGetLogicalType(fieldName, field, EnumSet.of(LogicalType.TIMESTAMP_MILLIS,
                                                                                       LogicalType.TIMESTAMP_MICROS));

      if (zonedDateTime == null) {
        fields.put(fieldName, null);
        return this;
      }

      Instant instant = zonedDateTime.toInstant();
      try {
        if (logicalType == LogicalType.TIMESTAMP_MILLIS) {
          long millis = Math.multiplyExact(instant.getEpochSecond(), 1000L);
          TimeUnit.NANOSECONDS.toMillis(instant.getEpochSecond());
          long tsMillis = Math.addExact(millis, instant.getNano() / 1000000L);
          fields.put(fieldName, tsMillis);
          return this;
        }

        long micros = Math.multiplyExact(instant.getEpochSecond(), 1000000L);
        long tsMicros = Math.addExact(micros, instant.getNano() / 1000L);
        fields.put(fieldName, tsMicros);
        return this;
      } catch (ArithmeticException e) {
        throw new IllegalArgumentException(String.format("Field %s is setting an invalid timestamp", fieldName));
      }
    }

    /**
     * Convert the given date into the type of the given field, and set the value for that field.
     * A Date can be converted into a long or a string.
     * This method does not support {@link LogicalType#DATE}
     *
     * @deprecated As of release 5.1.0, use {@link StructuredRecord.Builder#setDate(String, LocalDate)} instead.
     *
     * @param fieldName name of the field to set
     * @param date date value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given, or the date cannot be converted to the type for the field
     */
    @Deprecated
    public Builder convertAndSet(String fieldName, @Nullable Date date) throws UnexpectedFormatException {
      return convertAndSet(fieldName, date, DEFAULT_FORMAT);
    }

    /**
     * Convert the given date into the type of the given field, and set the value for that field.
     * A Date can be converted into a long or a string, using a date format, if supplied.
     * This method does not support {@link LogicalType#DATE}
     *
     * @deprecated As of release 5.1.0, use {@link StructuredRecord.Builder#setDate(String, LocalDate)} instead.
     *
     * @param fieldName name of the field to set
     * @param date date value for the field
     * @param dateFormat format for the date if it is a string. If null, the default format of
     *                   "YYYY-MM-DD'T'HH:mm:ss z" will be used
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given, or the date cannot be converted to the type for the field
     */
    @Deprecated
    public Builder convertAndSet(String fieldName, @Nullable Date date,
                                 @Nullable DateFormat dateFormat) throws UnexpectedFormatException {
      Schema.Field field = createField(fieldName, date);
      boolean isNullable = field.getSchema().isNullable();
      if (isNullable && date == null) {
        fields.put(fieldName, null);
        return this;
      }

      Schema.Type fieldType = isNullable ? field.getSchema().getNonNullable().getType() : field.getSchema().getType();
      if (fieldType == Schema.Type.LONG) {
        fields.put(fieldName, date.getTime());
      } else if (fieldType == Schema.Type.STRING) {
        DateFormat format = dateFormat == null ? DEFAULT_FORMAT : dateFormat;
        fields.put(fieldName, format.format(date));
      } else {
        throw new UnexpectedFormatException("Date must be either a long or a string, not a " + fieldType);
      }
      return this;
    }

    /**
     * Convert the given string into the type of the given field, and set the value for that field. A String can be
     * converted to a boolean, int, long, float, double, bytes, string, or null.
     *
     * @param fieldName name of the field to set
     * @param strVal string value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given, or the string cannot be converted to the type for the field
     */
    public Builder convertAndSet(String fieldName, @Nullable String strVal) throws UnexpectedFormatException {
      Schema.Field field = createField(fieldName, strVal);
      fields.put(fieldName, convertString(field.getSchema(), strVal));
      return this;
    }

    /**
     * Build a {@link StructuredRecord} with the fields set by this builder.
     *
     * @return a {@link StructuredRecord} with the fields set by this builder
     * @throws UnexpectedFormatException if there is at least one non-nullable field without a value
     */
    public StructuredRecord build() throws UnexpectedFormatException {
      // check that all non-nullable fields have a value.
      for (Schema.Field field : schema.getFields()) {
        String fieldName = field.getName();
        if (!fields.containsKey(fieldName)) {
          // if the field is not nullable and there is no value set for the field, this is invalid.
          if (!field.getSchema().isNullable()) {
            throw new UnexpectedFormatException("Field " + fieldName + " must contain a value.");
          } else {
            // otherwise, set the value for the field to null
            fields.put(fieldName, null);
          }
        }
      }
      return new StructuredRecord(schema, fields);
    }

    private Object convertString(Schema schema, String strVal) throws UnexpectedFormatException {
      Schema.Type simpleType;
      if (schema.getType().isSimpleType()) {
        simpleType = schema.getType();
        if (strVal == null && simpleType != Schema.Type.NULL) {
          throw new UnexpectedFormatException("Cannot set non-nullable field to a null value.");
        }
      } else if (schema.isNullable()) {
        if (strVal == null) {
          return null;
        }
        simpleType = schema.getNonNullable().getType();
      } else {
        throw new UnexpectedFormatException("Cannot convert a string to schema " + schema);
      }

      switch (simpleType) {
        case BOOLEAN:
          return Boolean.parseBoolean(strVal);
        case INT:
          return Integer.parseInt(strVal);
        case LONG:
          return Long.parseLong(strVal);
        case FLOAT:
          return Float.parseFloat(strVal);
        case DOUBLE:
          return Double.parseDouble(strVal);
        case BYTES:
          return Bytes.toBytesBinary(strVal);
        case STRING:
          return strVal;
        case NULL:
          return null;
        default:
          // shouldn't ever get here
          throw new UnexpectedFormatException("Cannot convert a string to schema " + schema);
      }
    }

    private Schema.Field createField(String fieldName, Object val) {
      Schema.Field field = schema.getField(fieldName);
      if (field == null) {
        throw new UnexpectedFormatException("field " + fieldName + " is not in the schema.");
      }
      Schema fieldSchema = field.getSchema();
      if (val == null) {
        if (fieldSchema.getType() == Schema.Type.NULL) {
          return field;
        }
        if (fieldSchema.getType() != Schema.Type.UNION) {
          throw new UnexpectedFormatException("field " + fieldName + " cannot be set to a null value.");
        }
        for (Schema unionSchema : fieldSchema.getUnionSchemas()) {
          if (unionSchema.getType() == Schema.Type.NULL) {
            return field;
          }
        }
        throw new UnexpectedFormatException("field " + fieldName + " cannot be set to a null value.");
      }
      return field;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StructuredRecord that = (StructuredRecord) o;

    return Objects.equals(schema, that.schema) && Objects.equals(fields, that.fields);

  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, fields);
  }
}
