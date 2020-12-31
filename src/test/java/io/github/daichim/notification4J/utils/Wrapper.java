package io.github.daichim.notification4J.utils;

public class Wrapper<T> {

    private T instance;

    public Wrapper(T instance) {
        this.instance = instance;
    }

    public T get() {
        return this.instance;
    }

    public void set(T instance) {
        this.instance = instance;
    }

}
