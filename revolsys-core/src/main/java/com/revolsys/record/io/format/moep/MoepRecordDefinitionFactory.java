package com.revolsys.record.io.format.moep;

import java.util.HashMap;
import java.util.Map;

import org.jeometry.common.util.BaseObjectWithProperties;

import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionFactory;

public class MoepRecordDefinitionFactory extends BaseObjectWithProperties
  implements RecordDefinitionFactory {
  private static final Map<String, RecordDefinition> RECORD_DEFINITION_CACHE = new HashMap<>();

  @SuppressWarnings("unchecked")
  @Override
  public <RD extends RecordDefinition> RD getRecordDefinition(final CharSequence typePath) {
    synchronized (RECORD_DEFINITION_CACHE) {
      RecordDefinition recordDefinition = RECORD_DEFINITION_CACHE.get(typePath.toString());
      if (recordDefinition == null) {
        recordDefinition = MoepConstants.newRecordDefinition(typePath.toString());
        RECORD_DEFINITION_CACHE.put(typePath.toString(), recordDefinition);
      }
      return (RD)recordDefinition;
    }
  }

}
