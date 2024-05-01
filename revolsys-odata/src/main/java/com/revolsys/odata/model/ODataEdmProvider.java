package com.revolsys.odata.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlAbstractEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlFunction;
import org.apache.olingo.commons.api.edm.provider.CsdlFunctionImport;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.edm.provider.CsdlTerm;
import org.apache.olingo.commons.api.ex.ODataException;
import org.apache.olingo.commons.core.edm.Edm;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.core.ServiceMetadataImpl;

import com.revolsys.collection.list.Lists;
import com.revolsys.odata.service.processor.ODataEntityCollectionProcessor;
import com.revolsys.odata.service.processor.ODataEntityProcessor;
import com.revolsys.odata.service.processor.ODataPrimitiveProcessor;
import com.revolsys.odata.service.processor.ODataServiceDocumentMetadataProcessor;
import com.revolsys.record.schema.RecordStore;

public abstract class ODataEdmProvider extends CsdlAbstractEdmProvider {

  private final CsdlEntityContainer allEntityContainer = new CsdlEntityContainer()
    .setName("Container");

  private ODataSchema defaultSchema;

  private RecordStore recordStore;

  private final Map<String, ODataSchema> schemaByNamespace = new TreeMap<>();

  private final List<CsdlSchema> schemas = new ArrayList<>();

  private ODataHttpHandler handler;

  private String serviceRoot;

  private final Map<FullQualifiedName, CsdlTerm> termByName = new HashMap<>();

  private final Map<FullQualifiedName, List<CsdlFunction>> functionByName = new LinkedHashMap<>();

  private final Map<String, CsdlFunctionImport> functionImportByName = new LinkedHashMap<>();

  private final Map<CsdlFunction, ODataEntityCollectionProcessor.Handler> functionEntitSetHandlerByFunction = new HashMap<>();

  private Edm edm;

  public ODataEdmProvider() {
    addTerm("Geometry", "axisCount", "Int");
    addTerm("Geometry", "scaleX", "Float");
    addTerm("Geometry", "scaleY", "Float");
    addTerm("Geometry", "scaleZ", "Float");
  }

  public ODataEdmProvider addEntitySetFunction(final String namespace, final String name,
    final Consumer<CsdlFunction> configurer, final ODataEntityCollectionProcessor.Handler handler) {
    return addFunction(namespace, name, function -> {
      configurer.accept(function);
      this.functionEntitSetHandlerByFunction.put(function, handler);
    });
  }

  public ODataEdmProvider addFunction(final FullQualifiedName qName,
    final Consumer<CsdlFunction> configurer) {
    final var function = new CsdlFunction();
    final var name = qName.getName();
    function.setName(name);
    configurer.accept(function);
    this.functionByName.put(qName, Lists.newArray(function));
    this.functionImportByName.put(name, new CsdlFunctionImport(function));
    return this;
  }

  public ODataEdmProvider addFunction(final String namespace, final String name,
    final Consumer<CsdlFunction> configurer) {
    return addFunction(new FullQualifiedName(namespace, name), configurer);
  }

  protected ODataSchema addSchema(final String namespace) {
    final ODataSchema schema = new ODataSchema(this, namespace);
    this.schemaByNamespace.put(namespace, schema);
    this.schemas.add(schema);
    if (this.defaultSchema == null) {
      this.defaultSchema = schema;
    }
    return schema;
  }

  private CsdlTerm addTerm(final String namespace, final String name, final String type) {
    final CsdlTerm term = new CsdlTerm().setName(name)
      .setType(type);
    this.termByName.put(new FullQualifiedName(namespace, name), term);
    return term;
  }

  public void close() {
    this.recordStore.close();
  }

  public Edm getEdm() {
    return this.edm;
  }

  @Override
  public CsdlEntityContainer getEntityContainer() throws ODataException {
    return this.allEntityContainer;
  }

  private ODataEntityContainer getEntityContainer(final FullQualifiedName entityContainerName) {
    final ODataSchema schema = getSchema(entityContainerName);
    if (schema == null) {
      return null;
    } else {
      return schema.getEntityContainer(entityContainerName);
    }
  }

  @Override
  public CsdlEntityContainerInfo getEntityContainerInfo(final FullQualifiedName entityContainerName)
    throws ODataException {
    if (entityContainerName == null) {
      if (this.defaultSchema != null) {
        return this.defaultSchema.getEntityContainerInfo();
      }
    } else {
      final ODataEntityContainer entityContainer = getEntityContainer(entityContainerName);
      if (entityContainer != null) {
        return entityContainer.getEntityContainerInfo();
      }
    }
    return null;
  }

  @Override
  public CsdlEntitySet getEntitySet(final FullQualifiedName entityContainerName,
    final String entitySetName) throws ODataException {
    final ODataEntityContainer entityContainer = getEntityContainer(entityContainerName);
    if (entityContainer != null) {
      return entityContainer.getEntitySet(entitySetName);
    }
    return null;
  }

  @Override
  public CsdlEntityType getEntityType(final FullQualifiedName entityTypeName) {
    final ODataSchema schema = getSchema(entityTypeName);
    if (schema != null) {
      return schema.getEntityType(entityTypeName);
    }
    return null;
  }

  public ODataEntityCollectionProcessor.Handler getFunctionEntitySetHandler(
    final CsdlFunction function) {
    return this.functionEntitSetHandlerByFunction.get(function);
  }

  @Override
  public CsdlFunctionImport getFunctionImport(final FullQualifiedName containerQName,
    final String name) throws ODataException {
    return this.functionImportByName.get(name);
  }

  @Override
  public List<CsdlFunction> getFunctions(final FullQualifiedName functionName)
    throws ODataException {
    return this.functionByName.get(functionName);
  }

  public ODataHttpHandler getHandler() {
    return this.handler;
  }

  public RecordStore getRecordStore() {
    return this.recordStore;
  }

  private ODataSchema getSchema(final FullQualifiedName qualifiedName) {
    final String namespace = qualifiedName.getNamespace();
    return this.schemaByNamespace.get(namespace);
  }

  @Override
  public List<CsdlSchema> getSchemas() {
    return this.schemas;
  }

  public String getServiceRoot() {
    return this.serviceRoot;
  }

  @Override
  public CsdlTerm getTerm(final FullQualifiedName termName) {
    return this.termByName.get(termName);
  }

  public void init() {
    for (final CsdlSchema schema : this.schemas) {
      for (final CsdlEntitySet entitySet : schema.getEntityContainer()
        .getEntitySets()) {
        this.allEntityContainer.getEntitySets()
          .add(entitySet);
      }
    }
    final ServiceMetadata edm = new ServiceMetadataImpl(this, new ArrayList<>(), null);
    this.edm = edm.getEdm();
    final var handler = new ODataHttpHandler(edm);
    handler.register(new ODataEntityCollectionProcessor(this));
    handler.register(new ODataEntityProcessor(this));
    handler.register(new ODataPrimitiveProcessor(this));
    handler.register(new ODataServiceDocumentMetadataProcessor(this));
    this.handler = handler;
  }

  protected void setRecordStore(final RecordStore recordStore) {
    this.recordStore = recordStore;
  }

  public void setServiceRoot(final String serviceRoot) {
    this.serviceRoot = serviceRoot;
  }

}
