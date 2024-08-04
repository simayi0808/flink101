package com.sfl.flink.test;

import org.apache.flink.util.MathUtils;

public class Test00 {

    public static void main(String[] args) {

        for (int i = 1; i <= 3; i++) {
            System.out.println(MathUtils.murmurHash(i) % 128);
        }

        System.out.println(12345);
        System.out.println(11111);

    }

}
