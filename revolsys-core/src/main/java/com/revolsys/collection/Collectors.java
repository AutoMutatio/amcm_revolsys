package com.revolsys.collection;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.Maps;

public interface Collectors {

  static <V, C extends Collection<V>> Collector<V, C> collection(final Supplier<C> supplier) {
    final BiConsumer<C, V> collect = Collection::add;
    return new SimpleCollector<>(supplier, collect);
  }

  static <K, V> Collector<V, Map<K, V>> hashMap(final Function<V, K> keyFunction) {
    return map(Maps.factoryHash(), keyFunction);
  }

  static <K, V> Collector<V, Map<K, V>> linkedHashMap(final Function<V, K> keyFunction) {
    return map(Maps.factoryLinkedHash(), keyFunction);
  }

  static <IN, K, V> Collector<IN, Map<K, V>> map(final Function<IN, K> keyFunction,
    final Function<IN, V> valueFunction) {
    return map(Maps.factoryLinkedHash(), keyFunction, valueFunction);
  }

  static <K, V> Collector<V, Map<K, V>> map(final Function<V, K> keyFunction) {
    return map(Maps.factoryLinkedHash(), keyFunction);
  }

  static <IN, K, V, M extends Map<K, V>> Collector<IN, M> map(final Supplier<M> supplier,
    final Function<IN, K> keyFunction, final Function<IN, V> valueFunction) {
    final BiConsumer<M, IN> collect = (map, in) -> {
      final var key = keyFunction.apply(in);
      final var value = valueFunction.apply(in);
      map.put(key, value);
    };
    return new SimpleCollector<>(supplier, collect);
  }

  static <K, V, M extends Map<K, V>> Collector<V, M> map(final Supplier<M> supplier,
    final Function<V, K> keyFunction) {
    final BiConsumer<M, V> collect = (map, value) -> {
      final var key = keyFunction.apply(value);
      map.put(key, value);
    };
    return new SimpleCollector<>(supplier, collect);
  }

  static <K, V, C extends Collection<V>> Collector<V, Map<K, C>> mapCollection(
    final Supplier<C> collectionSuppier, final Function<V, K> keyFunction) {
    return mapCollection(Maps.factoryLinkedHash(), collectionSuppier, keyFunction);
  }

  static <K, V, C extends Collection<V>, M extends Map<K, C>> Collector<V, M> mapCollection(
    final Supplier<M> mapSupplier, final Supplier<C> collectionSuppier,
    final Function<V, K> keyFunction) {
    final BiConsumer<M, V> collect = (map, value) -> {
      final var key = keyFunction.apply(value);
      var c = map.get(key);
      if (c == null) {
        c = collectionSuppier.get();
        map.put(key, c);
      }
      c.add(value);
    };
    return new SimpleCollector<>(mapSupplier, collect);
  }

  static <K, V> Collector<V, Map<K, ListEx<V>>> mapList(final Function<V, K> keyFunction) {
    return mapCollection(Maps.factoryLinkedHash(), Lists.factoryArray(), keyFunction);
  }

  static <IN, K1, K2, V> Collector<IN, Map<K1, Map<K2, V>>> mapMap(
    final Function<IN, K1> key1Function, final Function<IN, K2> key2Function,
    final Function<IN, V> valueFunction) {
    return mapMap(Maps.factoryLinkedHash(), Maps.factoryLinkedHash(), key1Function, key2Function,
      valueFunction);
  }

  static <K1, K2, V> Collector<V, Map<K1, Map<K2, V>>> mapMap(final Function<V, K1> key1Function,
    final Function<V, K2> key2Function) {
    return mapMap(key1Function, key2Function, v -> v);
  }

  static <IN, K1, K2, V, M1 extends Map<K1, M2>, M2 extends Map<K2, V>> Collector<IN, M1> mapMap(
    final Supplier<M1> supplier1, final Supplier<M2> supplier2, final Function<IN, K1> key1Function,
    final Function<IN, K2> key2Function, final Function<IN, V> valueFunction) {
    final BiConsumer<M1, IN> collect = (map, in) -> {
      final var key1 = key1Function.apply(in);
      final var key2 = key2Function.apply(in);
      final var value = valueFunction.apply(in);
      final var map2 = map.computeIfAbsent(key1, k -> supplier2.get());
      map2.put(key2, value);
    };
    return new SimpleCollector<>(supplier1, collect);
  }

  static <K, V> Collector<V, Map<K, V>> treeMap(final Function<V, K> keyFunction) {
    return map(Maps.factoryTree(), keyFunction);
  }

  // default <K, C extends Collection<T>> Map<K, C> collectMapCollection(
  // final Supplier<C> collectionSupplier, final Function<T, K> keyFunction) {
  // final Map<K, C> map = new LinkedHashMap<>();
  // forEach(value -> {
  // final K key = keyFunction.apply(value);
  // C collection = map.get(key);
  // if (collection == null) {
  // collection = collectionSupplier.get();
  // map.put(key, collection);
  // }
  // collection.add(value);
  // });
  // return map;
  // }
  //
  // default <K> Map<K, ListEx<T>> collectMapList(final Function<T, K>
  // keyFunction) {
  // return collectMapCollection(Lists.factoryArray(), keyFunction);
  // }
  //
  // default <K> Map<K, Set<T>> collectMapSet(final Function<T, K> keyFunction)
  // {
  // return collectMapCollection(LinkedHashSet::new, keyFunction);
  // }
  //
  // default <K> Map<K, Set<T>> collectMapTreeSet(final Function<T, K>
  // keyFunction) {
  // return collectMapCollection(TreeSet::new, keyFunction);
  // }
}
