package com.revolsys.odata.model;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.olingo.commons.api.data.ODataEntity;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlReferentialConstraint;

import com.revolsys.record.query.And;
import com.revolsys.record.query.Condition;
import com.revolsys.record.query.TableReference;

public class ODataNavigationProperty extends CsdlNavigationProperty {

  private final AbstractODataEntitySet targetEntitySet;

  private Function<ODataEntity, Condition> whereConstructor;

  public ODataNavigationProperty(final AbstractODataEntitySet targetEntitySet, final String name) {
    this.targetEntitySet = targetEntitySet;
    setName(name);
    final var referencedType = targetEntitySet.getTypePathName();
    setType(referencedType);
    setNullable(false);
  }

  public ODataNavigationProperty addReferentialConstraint(final String sourceFieldName) {
    final String targetFieldName = this.targetEntitySet.getIdFieldName();
    return addReferentialConstraint(sourceFieldName, targetFieldName);
  }

  public ODataNavigationProperty addReferentialConstraint(final String sourceFieldName,
    final String targetFieldName) {
    final List<CsdlReferentialConstraint> referentialConstraints = getReferentialConstraints();
    referentialConstraints.add(new CsdlReferentialConstraint().setProperty(sourceFieldName)
      .setReferencedProperty(targetFieldName));
    updateWhereConstructor();
    return this;
  }

  private Function<ODataEntity, Condition> newEqualCondition(final String sourcePropertyName,
    final String targetPropertyName) {
    final TableReference table = this.targetEntitySet.getRecordDefinition();
    final Function<ODataEntity, Condition> newWhere = entity -> {
      final Object value = entity.getValue(sourcePropertyName);
      return table.equal(targetPropertyName, value);
    };
    return newWhere;
  }

  private void updateWhereConstructor() {
    final List<CsdlReferentialConstraint> constraints = getReferentialConstraints();
    if (constraints.isEmpty()) {
      throw new IllegalArgumentException("referentialConstraints names must not be empty");
    } else if (constraints.size() == 1) {
      final CsdlReferentialConstraint constraint = constraints.get(0);
      final String targetPropertyName = constraint.getProperty();
      final String sourcePropertyName = constraint.getReferencedProperty();
      this.whereConstructor = newEqualCondition(sourcePropertyName, targetPropertyName);
    } else {
      final List<Function<ODataEntity, Condition>> constructors = new ArrayList<>();
      for (final CsdlReferentialConstraint constraint : constraints) {
        final String targetPropertyName = constraint.getProperty();
        final String sourcePropertyName = constraint.getReferencedProperty();
        final var constructor = newEqualCondition(sourcePropertyName, targetPropertyName);
        constructors.add(constructor);
      }
      this.whereConstructor = entity -> {
        final And and = new And();
        for (final var constructor : constructors) {
          final Condition condition = constructor.apply(entity);
          and.addCondition(condition);
        }
        return and;
      };
    }
  }

  public Condition whereCondition(final ODataEntity entity) {
    return this.whereConstructor.apply(entity);
  }
}
