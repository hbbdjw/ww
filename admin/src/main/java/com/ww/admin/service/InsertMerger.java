package com.ww.admin.service;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 合并连续的同表 INSERT 语句，减少 SQL 解析与执行开销。
 *
 * <p>将 {@code INSERT INTO t (cols) VALUES (1); INSERT INTO t (cols) VALUES (2);}
 * 合并为 {@code INSERT INTO t (cols) VALUES (1),(2);}
 *
 * <p>仅合并纯 VALUES INSERT（无 ON CONFLICT / RETURNING / ON DUPLICATE KEY 后缀）。
 * 不同表或不同列列表的 INSERT 不合并。字符串字面量中的括号/逗号/引号正确处理。
 */
final class InsertMerger {

    /** 匹配 INSERT ... VALUES 前缀 + 值部分。 */
    private static final Pattern INSERT_VALUES_PATTERN = Pattern.compile(
            "^\\s*(INSERT\\s+(?:OR\\s+\\w+\\s+)?INTO\\s+.+?\\s+VALUES\\s*)(.+?)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** VALUES 关键字验证（避免误匹配列名含 values 的情况）。 */
    private static final Pattern VALUES_KEYWORD = Pattern.compile(
            "\\bVALUES\\s*", Pattern.CASE_INSENSITIVE);

    private final int maxTuples;
    private String prefix;
    private final List<String> tuples = new ArrayList<>();

    InsertMerger(int maxTuples) {
        this.maxTuples = maxTuples;
    }

    /**
     * 尝试吸收一条 INSERT 语句。同表同列前缀则合并，否则返回 false。
     *
     * @return true 表示已吸收；false 表示不是 INSERT 或前缀不同
     */
    boolean tryAbsorb(String sql) {
        Matcher m = INSERT_VALUES_PATTERN.matcher(sql);
        if (!m.matches()) return false;

        String stmtPrefix = m.group(1).trim();
        String valuesPart = m.group(2).trim();

        // 验证 VALUES 是关键字（不是列名的一部分）
        if (!VALUES_KEYWORD.matcher(stmtPrefix).find()) return false;

        // 解析值元组，检查是否有后缀（ON CONFLICT 等）
        TupleParseResult parsed = parseTuples(valuesPart);
        if (parsed.tuples.isEmpty() || !parsed.suffix.isEmpty()) {
            return false; // 有后缀或解析失败，不合并
        }

        if (prefix == null) {
            prefix = stmtPrefix;
        } else if (!prefix.equalsIgnoreCase(stmtPrefix)) {
            return false; // 不同表或列列表
        }

        tuples.addAll(parsed.tuples);
        return true;
    }

    boolean isFull() {
        return tuples.size() >= maxTuples;
    }

    boolean isEmpty() {
        return tuples.isEmpty();
    }

    /**
     * 刷出合并后的 INSERT 语句。
     *
     * @return 合并后的 SQL，或 null 如果无待刷数据
     */
    FlushResult flush() {
        if (tuples.isEmpty()) return null;
        StringBuilder sb = new StringBuilder(prefix.length() + tuples.size() * 32);
        sb.append(prefix).append(' ');
        for (int i = 0; i < tuples.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('(').append(tuples.get(i)).append(')');
        }
        sb.append(';');
        int count = tuples.size();
        prefix = null;
        tuples.clear();
        return new FlushResult(sb.toString(), count);
    }

    /**
     * 解析值部分为元组列表，返回元组列表和可能的后缀。
     *
     * <p>输入 {@code (1,'a'),(2,'b') ON CONFLICT DO NOTHING}
     * 返回 tuples=[{@code 1,'a'}, {@code 2,'b'}], suffix={@code ON CONFLICT DO NOTHING}
     */
    private static TupleParseResult parseTuples(String s) {
        List<String> tuples = new ArrayList<>();
        int i = 0;
        int n = s.length();

        while (i < n) {
            // 跳过空白
            while (i < n && Character.isWhitespace(s.charAt(i))) i++;
            if (i >= n) break;

            if (s.charAt(i) != '(') break; // 不是元组起始

            int start = i + 1;
            int depth = 1;
            boolean inSingleQuote = false;
            boolean inDoubleQuote = false;

            i++;
            while (i < n && depth > 0) {
                char c = s.charAt(i);
                if (inSingleQuote) {
                    if (c == '\'') {
                        if (i + 1 < n && s.charAt(i + 1) == '\'') {
                            i += 2; // 跳过转义引号
                            continue;
                        }
                        inSingleQuote = false;
                    }
                } else if (inDoubleQuote) {
                    if (c == '"') inDoubleQuote = false;
                } else {
                    switch (c) {
                        case '\'' -> inSingleQuote = true;
                        case '"' -> inDoubleQuote = true;
                        case '(' -> depth++;
                        case ')' -> depth--;
                    }
                }
                i++;
            }

            if (depth != 0) break; // 括号不匹配，放弃

            tuples.add(s.substring(start, i - 1));

            // 跳过空白
            while (i < n && Character.isWhitespace(s.charAt(i))) i++;

            // 检查逗号（更多元组）或结束
            if (i < n && s.charAt(i) == ',') {
                i++;
            } else {
                break;
            }
        }

        // 检查剩余文本（ON CONFLICT 等后缀）
        while (i < n && Character.isWhitespace(s.charAt(i))) i++;
        String suffix = (i < n) ? s.substring(i).trim() : "";

        return new TupleParseResult(tuples, suffix);
    }

    /** 刷出结果。 */
    record FlushResult(String sql, int tupleCount) {}

    /** 元组解析结果。 */
    private record TupleParseResult(List<String> tuples, String suffix) {}
}
