package com.lukaulcar.model;

import java.util.List;

/**
 * Represents a candlestick timeframe interval (e.g. "1m", "5m", "1h", "1d").
 *
 * @param name   the canonical interval identifier (e.g. "1m", "1h", "1M")
 * @param millis the duration of one candle in milliseconds
 */
public record Interval(String name, long millis) {

    public static final Interval ONE_SECOND = new Interval("1s", 1_000L);
    public static final Interval ONE_MINUTE = new Interval("1m", 60_000L);
    public static final Interval THREE_MINUTES = new Interval("3m", 180_000L);
    public static final Interval FIVE_MINUTES = new Interval("5m", 300_000L);
    public static final Interval FIFTEEN_MINUTES = new Interval("15m", 900_000L);
    public static final Interval THIRTY_MINUTES = new Interval("30m", 1_800_000L);
    public static final Interval ONE_HOUR = new Interval("1h", 3_600_000L);
    public static final Interval TWO_HOURS = new Interval("2h", 7_200_000L);
    public static final Interval FOUR_HOURS = new Interval("4h", 14_400_000L);
    public static final Interval SIX_HOURS = new Interval("6h", 21_600_000L);
    public static final Interval EIGHT_HOURS = new Interval("8h", 28_800_000L);
    public static final Interval TWELVE_HOURS = new Interval("12h", 43_200_000L);
    public static final Interval ONE_DAY = new Interval("1d", 86_400_000L);
    public static final Interval THREE_DAYS = new Interval("3d", 259_200_000L);
    public static final Interval ONE_WEEK = new Interval("1w", 604_800_000L);
    public static final Interval ONE_MONTH = new Interval("1M", 2_592_000_000L);

    /**
     * Unmodifiable list of standard Binance intervals.
     */
    public static final List<Interval> STANDARD_INTERVALS = List.of(
            ONE_SECOND,
            ONE_MINUTE,
            THREE_MINUTES,
            FIVE_MINUTES,
            FIFTEEN_MINUTES,
            THIRTY_MINUTES,
            ONE_HOUR,
            TWO_HOURS,
            FOUR_HOURS,
            SIX_HOURS,
            EIGHT_HOURS,
            TWELVE_HOURS,
            ONE_DAY,
            THREE_DAYS,
            ONE_WEEK,
            ONE_MONTH
    );

    /**
     * Resolves an {@link Interval} from a string representation.
     * Supports standard Binance names like "1m", "5m", "1h", "1d", "1M" as well as custom intervals like "10m".
     *
     * @param interval the interval string (e.g. "1m", "5m", "1h")
     * @return the matching {@link Interval}
     * @throws IllegalArgumentException if the interval string is invalid or cannot be parsed
     */
    public static Interval of(String interval) {
        if (interval == null || interval.isBlank()) {
            return ONE_MINUTE;
        }
        String clean = interval.trim();
        if (clean.equals("1M")) {
            return ONE_MONTH;
        }
        return switch (clean.toLowerCase()) {
            case "1s" -> ONE_SECOND;
            case "1m" -> ONE_MINUTE;
            case "3m" -> THREE_MINUTES;
            case "5m" -> FIVE_MINUTES;
            case "15m" -> FIFTEEN_MINUTES;
            case "30m" -> THIRTY_MINUTES;
            case "1h" -> ONE_HOUR;
            case "2h" -> TWO_HOURS;
            case "4h" -> FOUR_HOURS;
            case "6h" -> SIX_HOURS;
            case "8h" -> EIGHT_HOURS;
            case "12h" -> TWELVE_HOURS;
            case "1d" -> ONE_DAY;
            case "3d" -> THREE_DAYS;
            case "1w" -> ONE_WEEK;
            case "1mo" -> ONE_MONTH;
            default -> {
                if (clean.length() < 2) {
                    throw new IllegalArgumentException("Unsupported interval: " + interval);
                }
                char unit = clean.charAt(clean.length() - 1);
                long amount;
                try {
                    amount = Long.parseLong(clean.substring(0, clean.length() - 1));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Unsupported interval format: " + interval, e);
                }
                if (amount <= 0) {
                    throw new IllegalArgumentException("Interval duration must be positive: " + interval);
                }
                long millis = switch (unit) {
                    case 's' -> amount * 1_000L;
                    case 'm' -> amount * 60_000L;
                    case 'h' -> amount * 3_600_000L;
                    case 'd' -> amount * 86_400_000L;
                    case 'w' -> amount * 604_800_000L;
                    case 'M' -> amount * 2_592_000_000L;
                    default -> throw new IllegalArgumentException("Unsupported interval unit: " + unit + " in " + interval);
                };
                yield new Interval(clean, millis);
            }
        };
    }
}

