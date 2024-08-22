package com.revolsys.odata.model;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;

import com.revolsys.io.PathName;

public class ODataEntityContainer extends CsdlEntityContainer {
  public static final String CONTAINER = "Container";

  private final CsdlEntityContainerInfo entityContainerInfo = new CsdlEntityContainerInfo();

  private final Map<String, AbstractODataEntitySet> entitySetByName = new TreeMap<>();

  private final PathName qualifiedName;

  public ODataEntityContainer(final PathName namespace) {
    setName(CONTAINER);
    this.qualifiedName = namespace.newChild(getName());
    this.entityContainerInfo.setContainerName(this.qualifiedName);
  }

  public void addEntitySet(final AbstractODataEntitySet entitySet) {
    final List<CsdlEntitySet> entitySets = getEntitySets();
    entitySets.add(entitySet);
    final String name = entitySet.getName();
    this.entitySetByName.put(name, entitySet);
  }

  public CsdlEntityContainerInfo getEntityContainerInfo() {
    return this.entityContainerInfo;
  }

  @SuppressWarnings("unchecked")
  public <S extends AbstractODataEntitySet> S getODataEntitySet(final String name) {
    return (S)this.entitySetByName.get(name);
  }

  public PathName getQualifiedName() {
    return this.qualifiedName;
  }

  @Override
  public String toString() {
    return this.qualifiedName.toString();
  }
}
