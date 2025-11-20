package com.apacy.storagemanager.index;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CompositeKey<A extends Comparable<A>, P extends Comparable<P>>
        implements Comparable<CompositeKey<A, P>> {

    private final A attr;
    private final P pk;

    public CompositeKey(A attr, P pk) {
        this.attr = attr;
        this.pk = pk;
    }

    public A getAttr() {
        return attr;
    }

    public P getPk() {
        return pk;
    }

    @Override
    public int compareTo(CompositeKey<A, P> o) {
        int c = this.attr.compareTo(o.attr);
        if (c != 0) return c;
        return this.pk.compareTo(o.pk);
    }

    @Override
    public String toString() {
        return "(" + attr + ", " + pk + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CompositeKey<?,?> ck)) return false;
        return attr.equals(ck.attr) && pk.equals(ck.pk);
    }

    @Override
    public int hashCode() {
        int r = attr.hashCode();
        r = 31 * r + pk.hashCode();
        return r;
    }
}
