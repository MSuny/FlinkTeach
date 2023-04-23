package com.ccy._05window.util;

import java.util.ArrayList;
import java.util.List;

public class MyUtil {
    public static <T> List<T> toList(Iterable<T> iterable) {
        List<T> result = new ArrayList<>();
        for (T t : iterable) {
            result.add(t);
        }
        return result;
    }
}