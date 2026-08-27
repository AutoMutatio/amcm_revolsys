package com.revolsys.data.type;

import java.awt.Color;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Blob;

// TODO manage data types by classloader and allow unloading of registered classes.

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.xml.namespace.QName;

import org.slf4j.LoggerFactory;

import com.revolsys.awt.WebColors;
import com.revolsys.collection.list.ListEx;
import com.revolsys.data.identifier.Identifier;
import com.revolsys.date.Dates;
import com.revolsys.exception.Exceptions;
import com.revolsys.io.PathName;
import com.revolsys.net.UrlProxy;

public final class DataTypes {

  private static final Map<String, DataType> CLASS_TYPE_MAP = new HashMap<>();

  private static final Map<String, DataType> NAME_TYPE_MAP = new HashMap<>();

  public static final DataType ANY_URI = FunctionDataType.builder("anyURI", URI.class)
    .toObjectFunction(value -> {
      try {
        if (value instanceof URL) {
          final URL url = (URL)value;
          return url.toURI();
        } else if (value instanceof UrlProxy) {
          final UrlProxy proxy = (UrlProxy)value;
          return proxy.getUri();
        } else if (value instanceof File) {
          final File file = (File)value;
          return file.toURI();
        } else if (value instanceof Path) {
          final Path path = (Path)value;
          return path.toUri();
        } else {
          final String string = DataTypes.toString(value);
          try {
            return new URI(string);
          } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Unknown URI: " + string, e);
          }
        }
      } catch (final URISyntaxException e) {
        throw Exceptions.toRuntimeException(e);
      }
    })
    .build();

  public static final DataType BASE64_BINARY = new SimpleDataType("base64Binary", byte[].class);

  public static final DataType BASE64_URL_BINARY = FunctionDataType
    .builder("base64UrlBinary", byte[].class)
    .toObjectFunction(s -> Base64.getUrlDecoder()
      .decode(s.toString()))
    .toStringFunction(v -> Base64.getUrlEncoder()
      .encodeToString((byte[])v))
    .build();

  public static final DataType BINARY = new SimpleDataType("binary", byte[].class);

  public static final DataType BIG_INTEGER = new BigIntegerDataType();

  public static final DataType BLOB = new SimpleDataType("blob", Blob.class);

  public static final DataType BOOLEAN = FunctionDataType.builder("boolean", Boolean.class)
    .requiresQuotes(false)
    .toObjectFunction(value -> {
      if (value instanceof Boolean) {
        return (Boolean)value;
      } else {
        final String string = DataTypes.toString(value);
        if ("1".equals(string)) {
          return true;
        } else if ("Y".equalsIgnoreCase(string)) {
          return true;
        } else if ("on".equals(string)) {
          return true;
        } else if ("true".equalsIgnoreCase(string)) {
          return true;
        } else if ("0".equals(string)) {
          return false;
        } else if ("N".equalsIgnoreCase(string)) {
          return false;
        } else if ("off".equals(string)) {
          return false;
        } else if ("false".equalsIgnoreCase(string)) {
          return false;
        } else {
          throw new IllegalArgumentException(string + " is not a valid boolean");
        }
      }
    })
    .build();

  public static final DataType BYTE = new ByteDataType();

  public static final DataType UBYTE = new UnsignedByteDataType();

  public static final ClobDataType CLOB = new ClobDataType();

  public static final DataType CODE = new CodeDataType();

  public static final DataType COLOR = FunctionDataType.builder("color", Color.class)
    .toObjectFunction(WebColors::toColor)
    .toStringFunction(WebColors::toString)
    .build();

  public static final DataType BYTE_BUFFER = FunctionDataType
    .builder("byteBuffer", ByteBuffer.class)
    .toObjectFunction(v -> {
      if (v instanceof final byte[] bytes) {
        return ByteBuffer.wrap(bytes);
      } else if (v instanceof final ByteBuffer buffer) {
        return buffer;
      } else {
        return v;
      }
    })
    .build();

  public static final DataType UTIL_DATE = FunctionDataType
    .builder("utilDate", java.util.Date.class)
    .requiresQuotes(false)
    .toObjectFunction(Dates::getDate)
    .toStringFunction(Dates::toDateTimeIsoString)
    .equalsFunction(Dates::equalsNotNull)
    .build();

  public static final DataType DATE_TIME = FunctionDataType.builder("dateTime", Timestamp.class)
    .requiresQuotes(false)
    .toObjectFunction(Dates::getTimestamp)
    .toStringFunction(Dates::toTimestampIsoString)
    .equalsFunction(Dates::equalsNotNull)
    .build();

  public static final DataType DECIMAL = new BigDecimalDataType();

  public static final DataType DOUBLE = new DoubleDataType();

  public static final DataType FLOAT = new FloatDataType();

  public static final DataType IDENTIFIER = FunctionDataType.builder("identifier", Identifier.class)
    .toObjectFunction(Identifier::newIdentifier)
    .build();

  public static final IntegerDataType INT = new IntegerDataType();

  public static final LongDataType LONG = new LongDataType();

  @SuppressWarnings({
    "rawtypes",
  })
  public static final DataType MAP = FunctionDataType.builder("Map", Map.class)
    .toObjectFunction(value -> {
      if (value instanceof Map) {
        return (Map)value;
      } else {
        return value;
      }
    })
    .equalsFunction(FunctionDataType.MAP_EQUALS)
    .equalsExcludesFunction(FunctionDataType.MAP_EQUALS_EXCLUDES)
    .build();

  public static final DataType OBJECT = new ObjectDataType();

  public static final DataType PATH_NAME = FunctionDataType.builder("pathName", PathName.class)
    .toObjectFunction(PathName::newPathName)
    .build();

  public static final DataType QNAME = new SimpleDataType("QName", QName.class);

  public static final DataType SHORT = new ShortDataType();

  public static final DataType SQL_DATE = FunctionDataType.builder("date", java.sql.Date.class)
    .requiresQuotes(false)
    .toObjectFunction(Dates::getSqlDate)
    .toStringFunction(Dates::toSqlDateString)
    .equalsFunction(Dates::equalsNotNull)
    .plusFunction((d, n) -> Date.valueOf(d.toLocalDate()
      .plusDays(n.longValue())))
    .build();

  public static final DataType STRING = FunctionDataType.builder("string", String.class)
    .toObjectFunction(DataTypes::toString)
    .plusFunction(
      (s, n) -> s.substring(0, s.length() - 1) + (char)(s.charAt(s.length() - 1) + n.intValue()))
    .build();

  public static final DataType DURATION = FunctionDataType.builder("duration", Duration.class)
    .requiresQuotes(false)
    .toObjectFunction(v -> {
      if (v instanceof final Number number) {
        return Duration.ofMillis(number.longValue());
      } else if (v instanceof final Duration duration) {
        return duration;
      } else if (v instanceof final CharSequence duration) {
        return Duration.parse(duration);
      } else {
        return Duration.parse(v.toString());
      }
    })
    .build();

  public static final DataType TIME = FunctionDataType.builder("time", LocalTime.class)
    .requiresQuotes(false)
    .toObjectFunction(Dates::getTime)
    .toStringFunction(Dates::toTimeIsoString)
    .equalsFunction(Dates::equalsNotNull)
    .build();

  public static final DataType TIMESTAMP = FunctionDataType.builder("timestamp", Timestamp.class)
    .requiresQuotes(false)
    .toObjectFunction(Dates::getTimestamp)
    .toStringFunction(Dates::toTimestampIsoString)
    .equalsFunction(Dates::equalsNotNull)
    .build();

  public static final DataType INSTANT = FunctionDataType.builder("instant", Instant.class)
    .requiresQuotes(false)
    .toObjectFunction(Dates::getInstant)
    .toStringFunction(Dates::toInstantIsoString)
    .equalsFunction(Object::equals)
    .plusFunction((d, n) -> d.plusMillis(n.longValue()))
    .build();

  public static final DataType LOCAL_DATE = FunctionDataType.builder("localDate", LocalDate.class)
    .requiresQuotes(false)
    .toObjectFunction(Dates::getLocalDate)
    .toStringFunction(Dates::toLocalDateIsoString)
    .equalsFunction(Object::equals)
    .plusFunction((d, n) -> d.plusDays(n.longValue()))
    .build();

  public static final DataType URL = FunctionDataType.builder("url", java.net.URL.class)
    .toObjectFunction(value -> {
      if (value instanceof URL) {
        return (URL)value;
      } else if (value instanceof URI) {
        final URI uri = (URI)value;
        try {
          return uri.toURL();
        } catch (final MalformedURLException e) {
          throw new IllegalArgumentException("Cannot get url " + uri, e);
        }
      } else if (value instanceof UrlProxy) {
        final UrlProxy proxy = (UrlProxy)value;
        return proxy.getUrl();
      } else if (value instanceof File) {
        final File file = (File)value;
        try {
          final URI uri = file.toURI();
          return uri.toURL();
        } catch (final MalformedURLException e) {
          throw new IllegalArgumentException("Cannot get url " + file, e);
        }
      } else if (value instanceof Path) {
        final Path path = (Path)value;
        try {
          return path.toUri()
            .toURL();
        } catch (final MalformedURLException e) {
          throw new IllegalArgumentException("Cannot get url " + path, e);
        }
      } else {
        final String string = DataTypes.toString(value);
        try {
          return new URL(string);
        } catch (final MalformedURLException e) {
          throw new IllegalArgumentException("Unknown URL", e);
        }
      }
    })
    .build();

  public static final DataType UUID = FunctionDataType.builder("uuid", UUID.class)
    .toObjectFunction(value -> {
      if (value instanceof UUID) {
        return (UUID)value;
      } else {
        return java.util.UUID.fromString(value.toString());
      }
    })
    .plusFunction((u, n) -> {
      final var msb = u.getMostSignificantBits();
      var lsb = u.getLeastSignificantBits();
      lsb = lsb + n.longValue();
      return new UUID(msb, lsb);
    })
    .build();

  public static final DataType XML = FunctionDataType.builder("xml", String.class)
    .build();

  public static final DataType COLLECTION = new CollectionDataType("Collection", Collection.class,
    OBJECT);

  public static final DataType LIST = new ListDataType(ListEx.class, OBJECT);

  public static final DataType RELATION = new CollectionDataType("Relation", Collection.class,
    OBJECT);

  public static final DataType SET = new SetDataType(Set.class, OBJECT);

  static {
    registerDataTypes(DataTypes.class);

    register(Boolean.TYPE, BOOLEAN);
    register(Byte.TYPE, BYTE);
    register(Short.TYPE, SHORT);
    register(Integer.TYPE, INT);
    register(Long.TYPE, LONG);
    register(Float.TYPE, FLOAT);
    register(Double.TYPE, DOUBLE);
  }

  public static CollectionDataType collection(final String name, final Class<?> collectionClass,
    final DataType valueType) {
    return new CollectionDataType(name, collectionClass, valueType);
  }

  public static DataType getDataType(final Class<?> clazz) {
    if (clazz == null) {
      return DataTypes.OBJECT;
    } else {
      DataType dataType = CLASS_TYPE_MAP.get(clazz.getName());
      if (dataType == null) {
        final Class<?>[] interfaces = clazz.getInterfaces();
        if (interfaces != null) {
          for (final Class<?> inter : interfaces) {
            dataType = getDataType(inter);
            if (dataType != null && dataType != DataTypes.OBJECT) {
              return dataType;
            }
          }
        }
        return getDataType(clazz.getSuperclass());
      } else {
        return dataType;
      }
    }
  }

  public static DataType getDataType(final Object object) {
    if (object == null) {
      return DataTypes.OBJECT;
    } else if (object instanceof DataTypeProxy) {
      final DataTypeProxy proxy = (DataTypeProxy)object;
      return proxy.getDataType();
    } else if (object instanceof DataType) {
      final DataType type = (DataType)object;
      return type;
    } else {
      final Class<?> clazz = object.getClass();
      return getDataType(clazz);
    }
  }

  public static DataType getDataType(final String name) {
    if (name == null) {
      return DataTypes.OBJECT;
    } else {
      final DataType type = NAME_TYPE_MAP.get(name.toLowerCase());
      if (type == null) {
        if (name.endsWith("[]")) {
          final DataType elementType = getDataType(name.substring(0, name.length() - 2));
          return ListDataType.of(elementType);
        } else {
          return DataTypes.OBJECT;
        }
      } else {
        return type;
      }
    }
  }

  public static DataType getDataType(final Type type) {
    if (type instanceof Class) {
      final Class<?> clazz = (Class<?>)type;
      return getDataType(clazz);
    } else {
      throw new IllegalArgumentException("Cannot get dataType for: " + type);
    }
  }

  public static ListDataType list(final DataType valueType) {
    return new ListDataType(List.class, valueType);
  }

  public static ListDataType list(final String name, final DataType valueType) {
    return new ListDataType(name, List.class, valueType);
  }

  public static void register(final Class<?> typeClass, final DataType type) {
    final String typeClassName = typeClass.getName();
    if (!CLASS_TYPE_MAP.containsKey(typeClassName)) {
      CLASS_TYPE_MAP.put(typeClassName, type);
    }
  }

  public static void register(final DataType type) {
    final String name = type.getName()
      .toLowerCase();
    if (!NAME_TYPE_MAP.containsKey(name)) {
      NAME_TYPE_MAP.put(name, type);
    }
    final Class<?> typeClass = type.getJavaClass();
    register(typeClass, type);
  }

  public static void register(final String name, final Class<?> javaClass) {
    final DataType type = new SimpleDataType(name, javaClass);
    register(type);
  }

  /**
   * <p> Register the data types specified as public static fields (constants) on the registry class.</p>
   *
   * <pre>public static final DataType CUSTOM_DATA_TYPE = ...;</pre>
   *
   * @param registryClass The class containing the data type constants.
   */
  public static void registerDataTypes(final Class<?> registryClass) {
    final Field[] fields = registryClass.getDeclaredFields();
    for (final Field field : fields) {
      if (Modifier.isStatic(field.getModifiers())) {
        if (DataType.class.isAssignableFrom(field.getType())) {
          try {
            final DataType type = (DataType)field.get(null);
            register(type);
          } catch (final Throwable e) {
            LoggerFactory.getLogger(registryClass)
              .error("Error registering type " + field.getName(), e);
          }
        }
      }
    }
  }

  @SuppressWarnings({
    "unchecked", "rawtypes"
  })
  public static <V> V toObject(final Class<?> clazz, final Object value) {
    // TODO enum
    if (clazz == null) {
      return (V)value;
    } else if (value == null) {
      return null;
    } else if (clazz.isAssignableFrom(value.getClass())) {
      return (V)value;
    } else {
      if (clazz.isEnum()) {
        try {
          return (V)Enum.valueOf((Class<Enum>)clazz, value.toString());
        } catch (final Throwable e) {
        }
      }
      final DataType dataType = getDataType(clazz);
      if (dataType == null) {
        return (V)value;
      } else {
        return dataType.toObject(value);
      }
    }
  }

  public static String toString(final Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof String) {
      return (String)value;
    } else {
      final Class<?> valueClass = value.getClass();
      final DataType dataType = getDataType(valueClass);
      if (dataType == null) {
        return value.toString();
      } else {
        return dataType.toString(value);
      }
    }
  }

  private DataTypes() {
  }
}
