package com.revolsys.beans;

import static javax.measure.Quantity.Scale.ABSOLUTE;
import static javax.measure.Quantity.Scale.RELATIVE;

import java.util.function.UnaryOperator;

import javax.measure.Quantity;
import javax.measure.Unit;

import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlExpression;

import com.revolsys.beans.ScaleHelper.ToSystemUnitConverter;
import com.revolsys.util.JexlUtil;

import tech.units.indriya.AbstractQuantity;
import tech.units.indriya.ComparableQuantity;
import tech.units.indriya.function.Calculus;
import tech.units.indriya.quantity.Quantities;
import tech.units.indriya.spi.NumberSystem;

public class ExpressionMeasurable<Q extends Quantity<Q>> extends AbstractQuantity<Q> {

  private static final NumberSystem NUMBER_SYSTEM = Calculus.currentNumberSystem();

  private static final long serialVersionUID = 1L;

  public static boolean isAbsolute(final Quantity<?> quantity) {
    return ABSOLUTE == quantity.getScale();
  }

  public static boolean isRelative(final Quantity<?> quantity) {
    return RELATIVE == quantity.getScale();
  }

  public static <Q extends Quantity<Q>> ComparableQuantity<Q> scalarMultiplication(
    final Quantity<Q> quantity, final UnaryOperator<Number> operator) {

    // if operand has scale RELATIVE, multiplication is trivial
    if (isRelative(quantity)) {
      return Quantities.getQuantity(operator.apply(quantity.getValue()), quantity.getUnit(),
        RELATIVE);
    }

    final ToSystemUnitConverter toSystemUnits = ScaleHelper.toSystemUnitConverterForMul(quantity);

    final Number thisValueWithAbsoluteScale = toSystemUnits.apply(quantity.getValue());
    final Number resultValueInAbsUnits = operator.apply(thisValueWithAbsoluteScale);
    final boolean needsInvering = !toSystemUnits.isNoop();

    final Number resultValueInThisUnit = needsInvering ? quantity.getUnit()
      .getConverterTo(quantity.getUnit()
        .getSystemUnit())
      .inverse()
      .convert(resultValueInAbsUnits) : resultValueInAbsUnits;

    return Quantities.getQuantity(resultValueInThisUnit, quantity.getUnit(), quantity.getScale());
  }

  private JexlContext context;

  private final JexlExpression expression;

  protected ExpressionMeasurable(final JexlExpression expression, final JexlContext context,
    final Unit<Q> unit) {
    super(unit, ABSOLUTE);
    this.expression = expression;
    this.context = context;
  }

  public ExpressionMeasurable(final String expression, final Unit<Q> unit) {
    super(unit, ABSOLUTE);
    try {
      this.expression = JexlUtil.newExpression(expression);
    } catch (final Exception e) {
      throw new IllegalArgumentException("Expression " + expression + " is not valid", e);
    }
  }

  @Override
  public ComparableQuantity<Q> add(final Quantity<Q> that) {
    return ScaleHelper.addition(this, that,
      (thisValue, thatValue) -> NUMBER_SYSTEM.add(thisValue, thatValue));
  }

  @Override
  public ComparableQuantity<Q> divide(final Number divisor) {
    return ScaleHelper.scalarMultiplication(this,
      thisValue -> NUMBER_SYSTEM.divide(thisValue, divisor));
  }

  @Override
  public ComparableQuantity<?> divide(final Quantity<?> that) {
    return ScaleHelper.multiplication(this, that,
      (thisValue, thatValue) -> NUMBER_SYSTEM.divide(thisValue, thatValue), Unit::divide);
  }

  @Override
  public Double getValue() {
    if (this.expression == null) {
      return Double.NaN;
    } else {
      try {
        return Double.valueOf(JexlUtil.evaluateExpression(this.context, this.expression)
          .toString());
      } catch (final NullPointerException e) {
        return 0.0;
      }
    }
  }

  @Override
  public ComparableQuantity<?> inverse() {
    final Number resultValueInThisUnit = NUMBER_SYSTEM.reciprocal(getValue());
    return Quantities.getQuantity(resultValueInThisUnit, getUnit().inverse(), getScale());
  }

  @Override
  public ComparableQuantity<Q> multiply(final Number factor) {
    return ScaleHelper.scalarMultiplication(this,
      thisValue -> NUMBER_SYSTEM.multiply(thisValue, factor));
  }

  @Override
  public ComparableQuantity<?> multiply(final Quantity<?> that) {
    return ScaleHelper.multiplication(this, that,
      (thisValue, thatValue) -> NUMBER_SYSTEM.multiply(thisValue, thatValue), Unit::multiply);
  }

  @Override
  public Quantity<Q> negate() {
    final Number resultValueInThisUnit = NUMBER_SYSTEM.negate(getValue());
    return Quantities.getQuantity(resultValueInThisUnit, getUnit(), getScale());
  }

  @Override
  public ComparableQuantity<Q> subtract(final Quantity<Q> that) {
    return ScaleHelper.addition(this, that,
      (thisValue, thatValue) -> NUMBER_SYSTEM.subtract(thisValue, thatValue));
  }

}
