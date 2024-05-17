package com.revolsys.record.io.format.odata;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.record.code.SingleValueCodeTable;

public class ODataCodeTable extends SingleValueCodeTable implements ODataTypeDefinition {

  private DataType underlyingType = DataTypes.STRING;

  public ODataCodeTable(final JsonObject definition, final String name) {
    super(name);
    final var underlyingType = definition.getString("UnderlyingType");
    if (underlyingType != null) {
      this.underlyingType = OData.getDataTypeFromEdm(underlyingType);
    }
  }

  @Override
  public String getODataName() {
    return getName();
  }

  public DataType getUnderlyingType() {
    return this.underlyingType;
  }
}
