package com.kafka.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public interface RandomGenerator {

    /**
     * 1 ~ 10 사이에 있는 임의의 숫자를 생성한다
     *
     * @return
     */
    static int generateRangedNumberDecade() {
        return (int) (Math.random() * 10 + 1);
    }

    static double generateRangedNumber() {
        return (Math.random() * 10 + 1);
    }

    static List<Integer> generateRangedNumberDecades(int count) {

        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(RandomGenerator.generateRangedNumberDecade());
        }
        return result;
    }

    static List<Integer> randomAccumulationNumberList(int count) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {

            if (i == 0) {
                result.add(RandomGenerator.generateRangedNumberDecade());
            } else {
                result.add(result.get(i - 1) + RandomGenerator.generateRangedNumberDecade());
            }
        }
        return result;
    }

    static int generateRangedNumber(int min, int max) {
        return (int) (Math.random() * ((max - min) + 1)) + min;
    }

    static String generateRandomString(){
        Random random = new Random();
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        return random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

    }

}
