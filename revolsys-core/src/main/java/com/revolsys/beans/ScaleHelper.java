/*
 * Units of Measurement Reference Implementation
 * Copyright (c) 2005-2025, Jean-Marie Dautelle, Werner Keil, Otavio Santana.
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 *    and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of JSR-385, Indriya nor the names of their contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.revolsys.beans;

import static javax.measure.Quantity.Scale.ABSOLUTE;
import static javax.measure.Quantity.Scale.RELATIVE;

import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

import javax.measure.Quantity;
import javax.measure.Quantity.Scale;
import javax.measure.Unit;
import javax.measure.UnitConverter;

import tech.units.indriya.ComparableQuantity;
import tech.units.indriya.function.AbstractConverter;
import tech.units.indriya.function.Calculus;
import tech.units.indriya.quantity.Quantities;
import tech.units.indriya.spi.NumberSystem;

/**
 * Encapsulates scale-honoring quantity arithmetics.
 *
 * @author Andi Huber
 */
public final class ScaleHelper {

  private static enum OperandMode {
    ALL_ABSOLUTE, ALL_RELATIVE, MIXED;

    public static OperandMode get(final Quantity<?> q1, final Quantity<?> q2) {
      if (q1.getScale() != q2.getScale()) {
        return OperandMode.MIXED;
      }
      return isAbsolute(q1) ? OperandMode.ALL_ABSOLUTE : OperandMode.ALL_RELATIVE;
    }

    // public boolean isAllAbsolute() {
    // return this==ALL_ABSOLUTE;
    // }
    public boolean isAllRelative() {
      return this == ALL_RELATIVE;
    }
  }

  // honors RELATIVE scale
  public static class ToSystemUnitConverter implements UnaryOperator<Number> {
    public static ToSystemUnitConverter factor(final Number factor) {
      return new ToSystemUnitConverter(number -> NUMBER_SYSTEM.multiply(number, factor),
        number -> NUMBER_SYSTEM.divide(number, factor));
    }

    public static <Q extends Quantity<Q>> ToSystemUnitConverter forQuantity(
      final Quantity<Q> quantity, final Unit<Q> systemUnit) {
      if (quantity.getUnit()
        .equals(systemUnit)) {
        return ToSystemUnitConverter.noop(); // no conversion required
      }

      final UnitConverter converter = quantity.getUnit()
        .getConverterTo(systemUnit);

      if (isAbsolute(quantity)) {

        return ToSystemUnitConverter.of(converter::convert); // convert to
                                                             // system units

      } else {
        final Number linearFactor = linearFactorOf(converter).orElse(null);
        if (linearFactor != null) {
          // conversion by factor required ... Δ2°C -> Δ2K , Δ2°F -> 5/9 * Δ2K
          return ToSystemUnitConverter.factor(linearFactor);
        }
        // convert any other cases of RELATIVE scale to system unit (ABSOLUTE)
        // ...
        throw unsupportedConverter(converter, quantity.getUnit());
      }
    }

    public static ToSystemUnitConverter noop() {
      return new ToSystemUnitConverter(null, null);
    }

    public static ToSystemUnitConverter of(final UnaryOperator<Number> unaryOperator) {
      return new ToSystemUnitConverter(unaryOperator, null);
    }

    private final UnaryOperator<Number> unaryOperator;

    private final UnaryOperator<Number> inverseOperator;

    private ToSystemUnitConverter(final UnaryOperator<Number> unaryOperator,
      final UnaryOperator<Number> inverseOperator) {
      this.unaryOperator = unaryOperator;
      this.inverseOperator = inverseOperator;
    }

    @Override
    public Number apply(final Number x) {
      return isNoop() ? x : this.unaryOperator.apply(x);
    }

    public Number invert(final Number x) {
      return isNoop() ? x : this.inverseOperator.apply(x);
    }

    public boolean isNoop() {
      return this.unaryOperator == null;
    }

  }

  private static final NumberSystem NUMBER_SYSTEM = Calculus.currentNumberSystem();

  public static <Q extends Quantity<Q>> ComparableQuantity<Q> addition(final Quantity<Q> q1,
    final Quantity<Q> q2, final BinaryOperator<Number> operator) {

    final boolean yieldsRelativeScale = OperandMode.get(q1, q2)
      .isAllRelative();

    // converting almost all, except system units and those that are shifted and
    // relative like eg. Δ2°C == Δ2K
    final ToSystemUnitConverter thisConverter = toSystemUnitConverterForAdd(q1, q1);
    final ToSystemUnitConverter thatConverter = toSystemUnitConverterForAdd(q1, q2);

    final Number thisValueInSystemUnit = thisConverter.apply(q1.getValue());
    final Number thatValueInSystemUnit = thatConverter.apply(q2.getValue());

    final Number resultValueInSystemUnit = operator.apply(thisValueInSystemUnit,
      thatValueInSystemUnit);

    if (yieldsRelativeScale) {
      return Quantities.getQuantity(thisConverter.invert(resultValueInSystemUnit), q1.getUnit(),
        RELATIVE);
    }

    final boolean needsInverting = !thisConverter.isNoop() || !thatConverter.isNoop();
    final Number resultValueInThisUnit = needsInverting ? q1.getUnit()
      .getConverterTo(q1.getUnit()
        .getSystemUnit())
      .inverse()
      .convert(resultValueInSystemUnit) : resultValueInSystemUnit;

    return Quantities.getQuantity(resultValueInThisUnit, q1.getUnit(), ABSOLUTE);
  }

  public static <Q extends Quantity<Q>> ComparableQuantity<Q> convertTo(final Quantity<Q> quantity,
    final Unit<Q> anotherUnit) {

    final UnitConverter converter = quantity.getUnit()
      .getConverterTo(anotherUnit);

    if (isRelative(quantity)) {
      final Number linearFactor = linearFactorOf(converter).orElse(null);
      if (linearFactor == null) {
        throw unsupportedRelativeScaleConversion(quantity, anotherUnit);
      }
      final Number valueInOtherUnit = NUMBER_SYSTEM.multiply(linearFactor, quantity.getValue());
      return Quantities.getQuantity(valueInOtherUnit, anotherUnit, RELATIVE);
    }

    final Number convertedValue = converter.convert(quantity.getValue());
    return Quantities.getQuantity(convertedValue, anotherUnit, ABSOLUTE);
  }

  public static boolean isAbsolute(final Quantity<?> quantity) {
    return ABSOLUTE == quantity.getScale();
  }

  public static boolean isRelative(final Quantity<?> quantity) {
    return RELATIVE == quantity.getScale();
  }

  // -- HELPER

  private static Optional<Number> linearFactorOf(final UnitConverter converter) {
    return converter instanceof AbstractConverter ? ((AbstractConverter)converter).linearFactor()
      : Optional.empty();
  }

  public static ComparableQuantity<?> multiplication(final Quantity<?> q1, final Quantity<?> q2,
    final BinaryOperator<Number> amountOperator, final BinaryOperator<Unit<?>> unitOperator) {

    final Quantity<?> absQ1 = toAbsoluteLinear(q1);
    final Quantity<?> absQ2 = toAbsoluteLinear(q2);
    return Quantities.getQuantity(amountOperator.apply(absQ1.getValue(), absQ2.getValue()),
      unitOperator.apply(absQ1.getUnit(), absQ2.getUnit()));
  }

  public static <Q extends Quantity<Q>> ComparableQuantity<Q> scalarMultiplication(
    final Quantity<Q> quantity, final UnaryOperator<Number> operator) {

    // if operand has scale RELATIVE, multiplication is trivial
    if (isRelative(quantity)) {
      return Quantities.getQuantity(operator.apply(quantity.getValue()), quantity.getUnit(),
        RELATIVE);
    }

    final ToSystemUnitConverter toSystemUnits = toSystemUnitConverterForMul(quantity);

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

  private static <Q extends Quantity<Q>> Quantity<Q> toAbsoluteLinear(final Quantity<Q> quantity) {
    final Unit<Q> systemUnit = quantity.getUnit()
      .getSystemUnit();
    final UnitConverter toSystemUnit = quantity.getUnit()
      .getConverterTo(systemUnit);
    if (toSystemUnit.isLinear()) {
      if (isAbsolute(quantity)) {
        return quantity;
      }
      return Quantities.getQuantity(quantity.getValue(), quantity.getUnit());
    }
    // convert to system units
    if (isAbsolute(quantity)) {
      return Quantities.getQuantity(toSystemUnit.convert(quantity.getValue()), systemUnit,
        Scale.ABSOLUTE);
    } else {
      final Number linearFactor = linearFactorOf(toSystemUnit).orElse(null);
      if (linearFactor == null) {
        throw unsupportedRelativeScaleConversion(quantity, systemUnit);
      }
      final Number valueInSystemUnits = NUMBER_SYSTEM.multiply(linearFactor, quantity.getValue());
      return Quantities.getQuantity(valueInSystemUnits, systemUnit, ABSOLUTE);
    }
  }

  // used for addition, honors RELATIVE scale
  private static <Q extends Quantity<Q>> ToSystemUnitConverter toSystemUnitConverterForAdd(
    final Quantity<Q> q1, final Quantity<Q> q2) {
    final Unit<Q> systemUnit = q1.getUnit()
      .getSystemUnit();
    return ToSystemUnitConverter.forQuantity(q2, systemUnit);
  }

  // -- OPERANDS

  // used for multiplication, honors RELATIVE scale
  public static <T extends Quantity<T>> ToSystemUnitConverter toSystemUnitConverterForMul(
    final Quantity<T> quantity) {
    final Unit<T> systemUnit = quantity.getUnit()
      .getSystemUnit();
    return ToSystemUnitConverter.forQuantity(quantity, systemUnit);
  }

  // -- EXCEPTIONS

  private static UnsupportedOperationException unsupportedConverter(final UnitConverter converter,
    final Unit<?> unit) {
    return new UnsupportedOperationException(String.format(
      "Scale conversion from RELATIVE to ABSOLUTE for Unit %s having Converter %s is not implemented.",
      unit, converter));
  }

  private static <Q extends Quantity<Q>> UnsupportedOperationException unsupportedRelativeScaleConversion(
    final Quantity<Q> quantity, final Unit<Q> anotherUnit) {
    return new UnsupportedOperationException(
      String.format("Conversion of Quantitity %s to Unit %s is not supported for realtive scale.",
        quantity, anotherUnit));
  }
}
