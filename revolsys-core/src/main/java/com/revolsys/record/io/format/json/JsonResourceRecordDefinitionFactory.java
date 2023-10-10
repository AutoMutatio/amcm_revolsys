package com.revolsys.record.io.format.json;

import java.util.HashMap;
import java.util.Map;

import org.jeometry.common.util.BaseObjectWithProperties;

import com.revolsys.io.map.MapObjectFactory;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionFactory;
import com.revolsys.spring.resource.ClassPathResource;
import com.revolsys.spring.resource.Resource;

public class JsonResourceRecordDefinitionFactory extends BaseObjectWithProperties
  implements RecordDefinitionFactory {
  private final Map<String, RecordDefinition> recordDefinitionMap = new HashMap<>();

  public JsonResourceRecordDefinitionFactory(final Resource resource, final String... fileNames) {
    // for (final Resource childResource : resource.getChildren((fileName) -> {
    // return fileName.endsWith(".json");
    // })) {
    // final RecordDefinition recordDefinition =
    // MapObjectFactory.toObject(childResource);
    // final String name = recordDefinition.getPath();
    // this.recordDefinitionMap.put(name, recordDefinition);
    // }
    for (final String fileName : fileNames) {
      final Resource childResource = resource.createRelative(fileName);
      final RecordDefinition recordDefinition = MapObjectFactory.toObject(childResource);
      final String name = recordDefinition.getPath();
      this.recordDefinitionMap.put(name, recordDefinition);
    }
  }

  public JsonResourceRecordDefinitionFactory(final String locationPattern,
    final String... fileNames) {
    this(new ClassPathResource(locationPattern), fileNames);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RD extends RecordDefinition> RD getRecordDefinition(final CharSequence typePath) {
    return (RD)this.recordDefinitionMap.get(typePath.toString());
  }
}
