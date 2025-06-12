package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.MapEx;
import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.record.query.functions.Exists;
import com.revolsys.record.query.functions.F;
import com.revolsys.record.query.functions.JsonValue;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.util.Property;

public class Q {
  public static BiFunction<QueryValue, QueryValue, QueryValue> ADD = Add::new;

  public static BiFunction<QueryValue, QueryValue, Condition> ILIKE = ILike::new;

  public static Function<QueryValue, Condition> IS_NOT_NULL = IsNotNull::new;

  public static Function<QueryValue, Condition> IS_NULL = IsNull::new;

  public static BiFunction<QueryValue, QueryValue, Condition> IS_DISTINCT_FROM = IsDistinctFrom::new;

  public static BiFunction<QueryValue, QueryValue, Condition> IS_NOT_DISTINCT_FROM = IsNotDistinctFrom::new;

  public static BiFunction<QueryValue, QueryValue, Condition> EQUAL = Q::equal;

  public static BiFunction<QueryValue, QueryValue, Condition> NOT_EQUAL = Q::notEqual;

  public static BiFunction<QueryValue, QueryValue, Condition> GREATER_THAN = GreaterThan::new;

  public static BiFunction<QueryValue, QueryValue, Condition> GREATER_THAN_EQUAL = GreaterThanEqual::new;

  public static BiFunction<QueryValue, QueryValue, Condition> LESS_THAN = LessThan::new;

  public static BiFunction<QueryValue, QueryValue, Condition> LESS_THAN_EQUAL = LessThanEqual::new;

  public static BiFunction<QueryValue, QueryValue, Condition> IN = In::new;

  public static Add add(final QueryValue left, final QueryValue right) {
    return new Add(left, right);
  }

  public static And and(final Condition... conditions) {
    final List<Condition> list = Arrays.asList(conditions);
    return and(list);
  }

  public static Condition and(final Condition a, final Condition b) {
    if (a == null) {
      return b;
    } else {
      return a.and(b);
    }
  }

  public static And and(final List<? extends Condition> conditions) {
    return new And(conditions);
  }

  public static Any any(final ColumnReference column) {
    return new Any(column);
  }

  public static Any any(final TableReferenceProxy table, final String columnName) {
    final ColumnReference column = table.getColumn(columnName);
    return any(column);
  }

  public static QueryValue arithmatic(final FieldDefinition field, final String operator,
    final Object value) {
    final Value queryValue = Value.newValue(field, value);
    return arithmatic((QueryValue)field, operator, queryValue);
  }

  public static QueryValue arithmatic(final QueryValue left, final String operator,
    final QueryValue right) {
    if ("+".equals(operator)) {
      return Q.add(left, right);
    } else if ("-".equals(operator)) {
      return Q.subtract(left, right);
    } else if ("*".equals(operator)) {
      return Q.multiply(left, right);
    } else if ("/".equals(operator)) {
      return Q.divide(left, right);
    } else if ("%".equals(operator) || "mod".equals(operator)) {
      return Q.mod(left, right);
    } else {
      throw new IllegalArgumentException("Operator " + operator + " not supported");
    }
  }

  public static QueryValue arithmatic(final String fieldName, final String operator,
    final Object value) {
    final Column column = new Column(fieldName);
    final Value queryValue = Value.newValue(value);
    return arithmatic(column, operator, queryValue);

  }

  public static Between between(final FieldDefinition field, final Object min, final Object max) {
    final Value minCondition = Value.newValue(field, min);
    final Value maxCondition = Value.newValue(field, max);
    return new Between(field, minCondition, maxCondition);
  }

  public static Between between(final QueryValue field, final Object min, final Object max) {
    final Value minCondition = Value.newValue(min);
    final Value maxCondition = Value.newValue(max);
    return new Between(field, minCondition, maxCondition);
  }

  public static Condition binary(final FieldDefinition field, final String operator,
    final Object value) {
    final Value queryValue = Value.newValue(field, value);
    return binary((QueryValue)field, operator, queryValue);
  }

  public static Condition binary(final QueryValue left, final String operator,
    final QueryValue right) {
    if ("=".equals(operator)) {
      return Q.equal(left, right);
    } else if ("<>".equals(operator) || "!=".equals(operator)) {
      return Q.notEqual(left, right);
    } else if ("<".equals(operator)) {
      return Q.lessThan(left, right);
    } else if ("<=".equals(operator)) {
      return Q.lessThanEqual(left, right);
    } else if (">".equals(operator)) {
      return Q.greaterThan(left, right);
    } else if (">=".equals(operator)) {
      return Q.greaterThanEqual(left, right);
    } else {
      throw new IllegalArgumentException("Operator " + operator + " not supported");
    }
  }

  public static Condition binary(final String fieldName, final String operator,
    final Object value) {
    final Column column = new Column(fieldName);
    final Value queryValue = Value.newValue(value);
    return binary(column, operator, queryValue);

  }

  public static QueryValue count(final TableReference table, final String fieldName) {
    return new Count(table.getColumn(fieldName));
  }

  public static Divide divide(final QueryValue left, final QueryValue right) {
    return new Divide(left, right);
  }

  public static Condition equal(final QueryValue field, final Object value) {
    QueryValue right;
    if (value == null) {
      return new IsNull(field);
    } else if (value instanceof final Value queryValue) {
      if (queryValue.getValue() == null) {
        return new IsNull(field);
      } else {
        right = queryValue;
      }
    } else if (value instanceof final QueryValue queryValue) {
      right = queryValue;
    } else {
      right = Value.newValue(field, value);
    }
    return new Equal(field, right);
  }

  public static Condition equal(final String name, final Object value) {
    final Column leftCondition = new Column(name);
    return equal(leftCondition, value);
  }

  public static Condition equal(final TableReferenceProxy table, final CharSequence fieldName,
    final Object value) {
    final var column = table.getColumn(fieldName);
    return equal(column, value);
  }

  public static Condition equalId(final List<?> fields, final Identifier identifier) {
    final And and = new And();
    List<Object> values;
    if (identifier == null) {
      values = Arrays.asList(new Object[fields.size()]);
    } else {
      values = identifier.getValues();
    }
    if (fields.size() == values.size()) {
      for (int i = 0; i < fields.size(); i++) {
        final Object fieldKey = fields.get(i);
        Object value = values.get(i);

        Condition condition;
        if (value == null) {
          if (fieldKey instanceof FieldDefinition) {
            final FieldDefinition field = (FieldDefinition)fieldKey;
            condition = isNull(field);
          } else {
            condition = isNull(fieldKey.toString());
          }
        } else {
          if (fieldKey instanceof FieldDefinition) {
            final FieldDefinition fieldDefinition = (FieldDefinition)fieldKey;
            value = fieldDefinition.toFieldValue(value);
            condition = equal(fieldDefinition, value);
          } else {
            condition = equal(fieldKey.toString(), value);
          }
        }
        and.and(condition);
      }
    } else {
      throw new IllegalArgumentException(
        "Field count for " + fields + " != count for values " + values);
    }
    return and;
  }

  public static Exists exists(final QueryValue expression) {
    return new Exists(expression);
  }

  public static GreaterThan greaterThan(final FieldDefinition fieldDefinition, final Object value) {
    final String name = fieldDefinition.getName();
    final Value valueCondition = Value.newValue(fieldDefinition, value);
    return greaterThan(name, valueCondition);
  }

  public static GreaterThan greaterThan(final QueryValue left, final QueryValue right) {
    return new GreaterThan(left, right);
  }

  public static GreaterThan greaterThan(final String name, final Object value) {
    final Value valueCondition = Value.newValue(value);
    return greaterThan(name, valueCondition);
  }

  public static GreaterThan greaterThan(final String name, final QueryValue right) {
    final Column column = new Column(name);
    return new GreaterThan(column, right);
  }

  public static GreaterThanEqual greaterThanEqual(final FieldDefinition fieldDefinition,
    final Object value) {
    final String name = fieldDefinition.getName();
    final Value valueCondition = Value.newValue(fieldDefinition, value);
    return greaterThanEqual(name, valueCondition);
  }

  public static GreaterThanEqual greaterThanEqual(final QueryValue left, final QueryValue right) {
    return new GreaterThanEqual(left, right);
  }

  public static GreaterThanEqual greaterThanEqual(final String name, final Object value) {
    final Value valueCondition = Value.newValue(value);
    return greaterThanEqual(name, valueCondition);
  }

  public static GreaterThanEqual greaterThanEqual(final String name, final QueryValue right) {
    final Column column = new Column(name);
    return greaterThanEqual(column, right);
  }

  public static ILike iLike(final ColumnReference column, final Object value) {
    final String name = column.getName();
    final Value valueCondition = Value.newValue(column, value);
    return iLike(name, valueCondition);
  }

  public static ILike iLike(final QueryValue left, final Object value) {
    final Value valueCondition = Value.newValue(value);
    return new ILike(left, valueCondition);
  }

  public static ILike iLike(final String name, final Object value) {
    final Value valueCondition = Value.newValue(value);
    return iLike(name, valueCondition);
  }

  public static ILike iLike(final String left, final QueryValue right) {
    final Column leftCondition = new Column(left);
    return new ILike(leftCondition, right);
  }

  public static Condition iLike(final String left, final String right) {
    final Column leftCondition = new Column(left);
    final Value valueCondition = Value.newValue("%" + right + "%");
    return new ILike(leftCondition, valueCondition);
  }

  public static ILike iLike(final TableReferenceProxy table, final CharSequence fieldName,
    final Object value) {
    final var column = table.getColumn(fieldName);
    return iLike(column, value);
  }

  public static In in(final ColumnReference fieldDefinition,
    final Collection<? extends Object> values) {
    return new In(fieldDefinition, values);
  }

  public static In in(final ColumnReference fieldDefinition, final Object... values) {
    final List<Object> list = Arrays.asList(values);
    return new In(fieldDefinition, list);
  }

  public static In in(final String name, final Collection<? extends Object> values) {
    final Column left = new Column(name);
    final CollectionValue collectionValue = new CollectionValue(values);
    return new In(left, collectionValue);
  }

  public static In in(final String name, final Object... values) {
    final List<Object> list = Arrays.asList(values);
    return new In(name, list);
  }

  public static In in(final TableReferenceProxy table, final CharSequence fieldName,
    final Object... values) {
    final var column = table.getColumn(fieldName);
    return in(column, values);
  }

  public static IsNotNull isNotNull(final FieldDefinition fieldDefinition) {
    final String name = fieldDefinition.getName();
    return isNotNull(name);
  }

  public static IsNotNull isNotNull(final QueryValue queryValue) {
    return new IsNotNull(queryValue);
  }

  public static IsNotNull isNotNull(final String name) {
    final Column condition = new Column(name);
    return new IsNotNull(condition);
  }

  public static IsNull isNull(final QueryValue queryValue) {
    return new IsNull(queryValue);
  }

  public static IsNull isNull(final String name) {
    final Column condition = new Column(name);
    return new IsNull(condition);
  }

  public static JsonValue jsonRawValue(final QueryValue left, final QueryValue right) {
    return new JsonValue(Arrays.asList(left, right)).setText(false);
  }

  public static JsonValue jsonRawValue(final QueryValue left, final String right) {
    return jsonRawValue(left, Value.newValue(right)).setText(false);
  }

  public static JsonValue jsonValue(final QueryValue left, final QueryValue right) {
    return new JsonValue(Arrays.asList(left, right));
  }

  public static JsonValue jsonValue(final QueryValue left, final String right) {
    return jsonValue(left, Value.newValue(right));
  }

  public static JsonValue jsonValue(final TableReferenceProxy table, final String fieldName,
    final String name) {
    final var column = table.getColumn(fieldName);
    return jsonValue(column, name);
  }

  public static Condition jsonValueEqual(final QueryValue left, final String key,
    final Object value) {
    final var jsonValue = jsonValue(left, key);
    return equal(jsonValue, value);
  }

  public static LessThan lessThan(final FieldDefinition fieldDefinition, final Object value) {
    final String name = fieldDefinition.getName();
    final Value valueCondition = Value.newValue(fieldDefinition, value);
    return lessThan(name, valueCondition);
  }

  public static LessThan lessThan(final QueryValue left, final QueryValue right) {
    return new LessThan(left, right);
  }

  public static LessThan lessThan(final String name, final Object value) {
    final Value valueCondition = Value.newValue(value);
    return lessThan(name, valueCondition);
  }

  public static LessThan lessThan(final String name, final QueryValue right) {
    final Column column = new Column(name);
    return lessThan(column, right);
  }

  public static LessThanEqual lessThanEqual(final FieldDefinition fieldDefinition,
    final Object value) {
    final String name = fieldDefinition.getName();
    final Value valueCondition = Value.newValue(fieldDefinition, value);
    return lessThanEqual(name, valueCondition);
  }

  public static LessThanEqual lessThanEqual(final QueryValue left, final QueryValue right) {
    return new LessThanEqual(left, right);
  }

  public static LessThanEqual lessThanEqual(final String name, final Object value) {
    final Value valueCondition = Value.newValue(value);
    return lessThanEqual(name, valueCondition);
  }

  public static LessThanEqual lessThanEqual(final String name, final QueryValue right) {
    final Column column = new Column(name);
    return new LessThanEqual(column, right);
  }

  public static Like like(final FieldDefinition fieldDefinition, final Object value) {
    final String name = fieldDefinition.getName();
    final Value valueCondition = Value.newValue(fieldDefinition, value);
    return like(name, valueCondition);
  }

  public static Like like(final QueryValue left, final Object value) {
    final QueryValue valueCondition;
    if (value instanceof QueryValue) {
      valueCondition = (QueryValue)value;
    } else {
      valueCondition = Value.newValue(value);
    }
    return new Like(left, valueCondition);
  }

  public static Like like(final String name, final Object value) {
    final Value valueCondition = Value.newValue(value);
    return like(name, valueCondition);
  }

  public static Like like(final String left, final QueryValue right) {
    final Column leftCondition = new Column(left);
    return like(leftCondition, right);
  }

  public static Condition likeRegEx(final RecordStore recordStore, final String fieldName,
    final Object value) {
    QueryValue left;
    if (recordStore.getClass()
      .getName()
      .contains("Oracle")) {
      left = F.regexpReplace(F.upper(fieldName), "[^A-Z0-9]", "");
    } else {
      left = F.regexpReplace(F.upper(fieldName), "[^A-Z0-9]", "", "g");
    }
    final String right = "%" + DataTypes.toString(value)
      .toUpperCase()
      .replaceAll("[^A-Z0-9]", "") + "%";
    return Q.like(left, right);
  }

  public static StringLiteral literal(final String string) {
    return new StringLiteral(string);
  }

  public static Mod mod(final QueryValue left, final QueryValue right) {
    return new Mod(left, right);
  }

  public static Multiply multiply(final QueryValue left, final QueryValue right) {
    return new Multiply(left, right);
  }

  public static Not not(final Condition condition) {
    return new Not(condition);
  }

  public static Condition notEqual(final QueryValue field, final Object value) {
    QueryValue right;
    if (value == null) {
      return new IsNotNull(field);
    } else if (value instanceof final Value queryValue) {
      if (queryValue.getValue() == null) {
        return new IsNotNull(field);
      } else {
        right = queryValue;
      }
    } else if (value instanceof final QueryValue queryValue) {
      right = queryValue;
    } else {
      right = Value.newValue(field, value);
    }
    return new NotEqual(field, right);
  }

  public static Condition notEqual(final String name, final Object value) {
    final QueryValue column = new Column(name);
    return notEqual(column, value);
  }

  public static Not notExists(final QueryValue expression) {
    return not(exists(expression));
  }

  public static Or or(final Condition... conditions) {
    final List<Condition> list = Arrays.asList(conditions);
    return or(list);
  }

  public static Condition or(final Condition a, final Condition b) {
    if (a == null) {
      return b;
    } else {
      return a.or(b);
    }
  }

  public static Or or(final List<? extends Condition> conditions) {
    return new Or(conditions);
  }

  public static Condition predicate(final Predicate<MapEx> predicate) {
    return new Condition() {
      @Override
      public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
        final SqlAppendable sql) {
        throw new UnsupportedOperationException(
          "Predicate conditions cannot be used to create a SQL expression");
      }

      @Override
      public int appendParameters(final int index, final PreparedStatement statement) {
        throw new UnsupportedOperationException(
          "Predicate conditions cannot be used to append SQL parameters");
      }

      @Override
      public Condition clone() {
        return this;
      }

      @Override
      public Condition clone(final TableReference oldTable, final TableReference newTable) {
        return this;
      }

      @Override
      public boolean test(final MapEx record) {
        return predicate.test(record);
      }
    };

  }

  public static void setValue(final int index, final Condition condition, final Object value) {
    setValueInternal(-1, index, condition, value);

  }

  public static int setValueInternal(int i, final int index, final QueryValue condition,
    final Object value) {
    for (final QueryValue subCondition : condition.getQueryValues()) {
      if (subCondition instanceof Value) {
        final Value valueCondition = (Value)subCondition;
        i++;
        if (i == index) {
          valueCondition.setValue(value);
          return i;
        }
        i = setValueInternal(i, index, subCondition, value);
        if (i >= index) {
          return i;
        }
      }
    }
    return i;
  }

  public static QueryValue sql(final DataType dataType, final Object... fragments) {
    final ListEx<QueryValue> values = Lists.newArray(fragments)
      .map(fragment -> {
        if (fragment instanceof final QueryValue queryValue) {
          return queryValue;
        } else {
          return sql(fragment.toString());
        }
      })
      .toList();
    if (values.size() == 1) {
      return values.get(0);
    } else {
      return new SqlFragments(dataType, values);
    }
  }

  public static SqlCondition sql(final String sql) {
    if (Property.hasValue(sql)) {
      return new SqlCondition(sql);
    } else {
      return null;
    }
  }

  public static SqlCondition sql(final String sql, final Iterable<Object> parameters) {
    return new SqlCondition(sql, parameters);
  }

  public static SqlCondition sql(final String sql, final Object... parameters) {
    return new SqlCondition(sql, parameters);
  }

  public static QueryValue sqlExpression(final String sql, final DataType dataType) {
    return new SqlExpression(sql, dataType);
  }

  public static Subtract subtract(final QueryValue left, final QueryValue right) {
    return new Subtract(left, right);
  }
}
