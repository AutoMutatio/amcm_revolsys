package com.revolsys.swing.field;

import java.util.List;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.type.DataTypes;
import com.revolsys.record.code.CodeTable;
import com.revolsys.util.Strings;

public class ObjectLabelField extends TextField {
  private static final long serialVersionUID = 1L;

  private final CodeTable codeTable;

  public ObjectLabelField() {
    this("fieldValue", null);
  }

  public ObjectLabelField(final String fieldName) {
    this(fieldName, null);
  }

  public ObjectLabelField(final String fieldName, final CodeTable codeTable) {
    super(fieldName);
    this.codeTable = codeTable;
  }

  public ObjectLabelField(final String fieldName, final int columns, final CodeTable codeTable) {
    super(fieldName, columns);
    this.codeTable = codeTable;
  }

  @Override
  public String getDisplayText(final Object fieldValue) {
    if (fieldValue == null) {
      return "-";
    } else if (this.codeTable == null) {
      return DataTypes.toString(fieldValue);
    } else {
      final List<Object> values = this.codeTable.getValues(Identifier.newIdentifier(fieldValue));
      if (values == null || values.isEmpty()) {
        return DataTypes.toString(fieldValue);
      } else {
        return " " + Strings.toString(values);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getFieldValue() {
    return (T)getFieldValueInternal();
  }

  @Override
  public ObjectLabelField setFieldEditable(final boolean editable) {
    return this;
  }

  @Override
  public void updateFieldValue() {
  }

}
