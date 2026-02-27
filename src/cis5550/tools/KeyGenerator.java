package cis5550.tools;

import java.security.SecureRandom;

public class KeyGenerator {
    private static final SecureRandom random = new SecureRandom();

    public static String generateUniqueKey() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 12; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }
}
