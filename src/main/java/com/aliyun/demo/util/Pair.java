package com.aliyun.demo.util;

import java.util.Map;

public class Pair<K, V> implements Map.Entry<K, V> {
    private K key;
    private V value;
    public Pair(K key, V value) {
        setKey(key).setValue(value);
    }
    @Override
    public K getKey() {
        return key;
    }

    public Pair setKey(K key) {
        this.key = key;
        return this;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        this.value = value;
        return value;
    }
}
