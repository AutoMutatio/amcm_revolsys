package com.revolsys.odata.model;

import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;

import com.revolsys.io.PathName;
import com.revolsys.record.schema.RecordStore;

public class ODataSchema extends CsdlSchema {
  private final ODataEntityContainer entityContainer;

  private final ODataEdmProvider provider;

  public ODataSchema(final ODataEdmProvider provider, final PathName namespace) {
    this.provider = provider;
    setNamespace(namespace);
    this.entityContainer = new ODataEntityContainer(namespace);
    setEntityContainer(this.entityContainer);

  }

  public void addEntitySet(final AbstractODataEntitySet entitySet) {
    this.entityContainer.addEntitySet(entitySet);
    final ODataEntityType entityType = entitySet.getEntityType();
    addEntityType(entityType);
  }

  private boolean equalsNamespace(final PathName qualifiedName) {
    final PathName namespaceThis = getNamespace();
    final PathName namespaceOther = qualifiedName.getParent();
    return namespaceThis.equals(namespaceOther);
  }

  @Override
  public ODataEntityContainer getEntityContainer() {
    return this.entityContainer;
  }

  public ODataEntityContainer getEntityContainer(final PathName entityContainerName) {
    if (this.entityContainer.getQualifiedName()
      .equals(entityContainerName)) {
      return this.entityContainer;
    }
    return null;
  }

  public CsdlEntityContainerInfo getEntityContainerInfo() {
    return this.entityContainer.getEntityContainerInfo();
  }

  public CsdlEntityType getEntityType(final PathName entityTypeName) {
    if (equalsNamespace(entityTypeName)) {
      final String name = entityTypeName.getName();
      return getEntityType(name);
    }
    return null;
  }

  @Override
  public CsdlEntityType getEntityType(final String name) {
    return super.getEntityType(name);
  }

  public ODataEdmProvider getProvider() {
    return this.provider;
  }

  public PathName getQualifiedName(final String name) {
    final var namespace = getNamespace();
    return namespace.newChild(name);
  }

  public RecordStore getRecordStore() {
    return this.provider.getRecordStore();
  }

}
