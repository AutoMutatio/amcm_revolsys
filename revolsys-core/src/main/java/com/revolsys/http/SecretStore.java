package com.revolsys.http;

import org.jeometry.common.data.type.DataType;

import com.revolsys.io.map.ObjectFactoryConfig;
import com.revolsys.record.io.format.json.JsonObject;
import com.revolsys.record.io.format.json.JsonParser;

public interface SecretStore {

  static JsonObject getSecretJsonObject(final ObjectFactoryConfig factoryConfig,
    final String secretName) {
    final SecretStore secretStore = factoryConfig.getValue("secretStore");
    if (secretStore != null) {
      return secretStore.getSecretJsonObject(secretName);
    }
    return JsonObject.hash();
  }

  static String getSecretValue(final ObjectFactoryConfig factoryConfig, final String secretName) {
    final SecretStore secretStore = factoryConfig.getValue("secretStore");
    if (secretStore != null) {
      return secretStore.getSecretValue(secretName);
    }
    return null;
  }

  static <V> V getSecretValue(final ObjectFactoryConfig factoryConfig, final String secretName,
    final DataType dataType) {
    final String value = getSecretValue(factoryConfig, secretName);
    return dataType.toObject(value);
  }

  static String getSecretValue(final ObjectFactoryConfig factoryConfig, final String secretName,
    final String propertyName) {
    final SecretStore secretStore = factoryConfig.getValue("secretStore");
    if (secretStore != null) {
      final String value = secretStore.getSecretValue(secretName);
      if (value != null && value.charAt(0) == '{') {
        final JsonObject json = JsonParser.read(value);
        return json.getString(propertyName);
      }
    }
    return null;
  }

  static void setSecretValue(final ObjectFactoryConfig factoryConfig, final String secretName,
    final String value) {
    final SecretStore secretStore = factoryConfig.getValue("secretStore");
    if (secretStore != null) {
      secretStore.setSecretValue(secretName, value);
    }
  }

  static String setSecretValue(final ObjectFactoryConfig factoryConfig, final String secretName,
    final String propertyName, final String value) {
    final SecretStore secretStore = factoryConfig.getValue("secretStore");
    if (secretStore != null) {
      final String config = secretStore.getSecretValue(secretName);
      if (config != null && config.charAt(0) == '{') {
        final JsonObject json = JsonParser.read(config);
        json.addValue(secretName, value);
        secretStore.setSecretValue(propertyName, value);
      }
    }
    return null;
  }

  default ObjectFactoryConfig addTo(final ObjectFactoryConfig factoryConfig) {
    factoryConfig.addValue("secretStore", this);
    return factoryConfig;
  }

  default JsonObject getSecretJsonObject(final String secretId) {
    final String value = getSecretValue(secretId);
    if (value != null) {
      return JsonParser.read(value);
    }
    return JsonObject.hash();
  }

  String getSecretValue(String secretId);

  default void setSecretValue(final String name, final JsonObject value) {
    setSecretValue(name, value.toJsonString(false));
  }

  void setSecretValue(String name, String value);
}
