package com.revolsys.odata.model;

import java.util.Collections;
import java.util.List;

import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationPropertyBinding;
import org.apache.olingo.server.api.ODataRequest;

import com.revolsys.io.PathName;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.util.CaseConverter;

public abstract class AbstractODataEntitySet extends CsdlEntitySet
  implements RecordDefinitionProxy {

  protected ODataEntityType entityType;

  protected final String name;

  protected final ODataSchema schema;

  protected final PathName typeName;

  public AbstractODataEntitySet(final ODataSchema schema, final String typeName,
    final PathName pathName) {
    this.entityType = new ODataEntityType(this, schema, typeName, pathName);
    this.schema = schema;
    this.name = typeName;
    this.typeName = schema.getQualifiedName(typeName);
    setName(typeName);
    setType(this.typeName);

    schema.addEntitySet(this);
  }

  public ODataNavigationProperty addForeignKey(final AbstractODataEntitySet referencedTable) {
    final String propertyName = CaseConverter.toLowerFirstChar(referencedTable.getName());
    return addForeignKey(referencedTable, propertyName);
  }

  public ODataNavigationProperty addForeignKey(final AbstractODataEntitySet referencedTable,
    final String propertyName) {
    final ODataNavigationProperty navigationProperty = new ODataNavigationProperty(referencedTable,
      propertyName);

    final var navigationPropertyBinding = new CsdlNavigationPropertyBinding(propertyName,
      referencedTable.getName());
    addNavigationPropertyBinding(navigationPropertyBinding);

    this.entityType.addNavigationProperty(navigationProperty);
    return navigationProperty;
  }

  public void addNavigationPropertyBinding(
    final CsdlNavigationPropertyBinding navigationPropertyBinding) {
    this.navigationPropertyBindings.add(navigationPropertyBinding);
  }

  public ODataEntityType getEntityType() {
    return this.entityType;
  }

  @Override
  public String getName() {
    return this.name;
  }

  public PathName getNamespace() {
    return this.schema.getNamespace();
  }

  @Override
  public List<CsdlNavigationPropertyBinding> getNavigationPropertyBindings() {
    if (this.entityType.getRecordDefinition() == null) {
      return Collections.emptyList();
    }
    return super.getNavigationPropertyBindings();
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    return this.entityType.getRecordDefinition();
  }

  public PathName getTypeName() {
    return this.typeName;
  }

  public abstract Query newQuery(ODataRequest request);

}
