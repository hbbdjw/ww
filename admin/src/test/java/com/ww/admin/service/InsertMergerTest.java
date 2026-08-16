package com.ww.admin.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.ww.admin.service.InsertMerger.FlushResult;

class InsertMergerTest {

    @Test
    void mergeSimpleInserts() {
        InsertMerger m = new InsertMerger(100);
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (1);")).isTrue();
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (2);")).isTrue();
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (3);")).isTrue();

        FlushResult fr = m.flush();
        assertThat(fr).isNotNull();
        assertThat(fr.tupleCount()).isEqualTo(3);
        assertThat(fr.sql()).isEqualTo("INSERT INTO t VALUES (1),(2),(3);");
    }

    @Test
    void mergeInsertsWithColumns() {
        InsertMerger m = new InsertMerger(100);
        assertThat(m.tryAbsorb("INSERT INTO users (id, name) VALUES (1, 'alice');")).isTrue();
        assertThat(m.tryAbsorb("INSERT INTO users (id, name) VALUES (2, 'bob');")).isTrue();

        FlushResult fr = m.flush();
        assertThat(fr.tupleCount()).isEqualTo(2);
        assertThat(fr.sql()).contains("INSERT INTO users (id, name) VALUES");
        assertThat(fr.sql()).contains("(1, 'alice')").contains("(2, 'bob')");
    }

    @Test
    void differentTableNotAbsorbed() {
        InsertMerger m = new InsertMerger(100);
        assertThat(m.tryAbsorb("INSERT INTO t1 VALUES (1);")).isTrue();
        assertThat(m.tryAbsorb("INSERT INTO t2 VALUES (2);")).isFalse();

        FlushResult fr = m.flush();
        assertThat(fr.tupleCount()).isEqualTo(1);
    }

    @Test
    void differentColumnsNotAbsorbed() {
        InsertMerger m = new InsertMerger(100);
        assertThat(m.tryAbsorb("INSERT INTO t (a, b) VALUES (1, 2);")).isTrue();
        assertThat(m.tryAbsorb("INSERT INTO t (a, c) VALUES (1, 3);")).isFalse();
    }

    @Test
    void nonInsertNotAbsorbed() {
        InsertMerger m = new InsertMerger(100);
        assertThat(m.tryAbsorb("UPDATE t SET a = 1")).isFalse();
        assertThat(m.tryAbsorb("DELETE FROM t")).isFalse();
        assertThat(m.tryAbsorb("CREATE TABLE t (a INT)")).isFalse();
        assertThat(m.tryAbsorb("SELECT * FROM t")).isFalse();
    }

    @Test
    void insertWithOnConflictNotAbsorbed() {
        InsertMerger m = new InsertMerger(100);
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (1);")).isTrue();
        // 带 ON CONFLICT 的 INSERT 不合并
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (2) ON CONFLICT DO NOTHING;")).isFalse();
    }

    @Test
    void stringValueWithParenthesis() {
        InsertMerger m = new InsertMerger(100);
        // 字符串中包含括号和逗号
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (1, 'hello(world)', 'a,b');")).isTrue();
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (2, 'test)', 'x');")).isTrue();

        FlushResult fr = m.flush();
        assertThat(fr.tupleCount()).isEqualTo(2);
        assertThat(fr.sql()).contains("(1, 'hello(world)', 'a,b')");
        assertThat(fr.sql()).contains("(2, 'test)', 'x')");
    }

    @Test
    void escapedSingleQuote() {
        InsertMerger m = new InsertMerger(100);
        // PG 转义单引号：''
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (1, 'it''s ok');")).isTrue();
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (2, 'a''b''c');")).isTrue();

        FlushResult fr = m.flush();
        assertThat(fr.tupleCount()).isEqualTo(2);
        assertThat(fr.sql()).contains("(1, 'it''s ok')");
        assertThat(fr.sql()).contains("(2, 'a''b''c')");
    }

    @Test
    void multiValueInsertAbsorbed() {
        InsertMerger m = new InsertMerger(100);
        // pg_dump 有时输出多值 INSERT
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (1), (2), (3);")).isTrue();
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (4), (5);")).isTrue();

        FlushResult fr = m.flush();
        assertThat(fr.tupleCount()).isEqualTo(5);
        assertThat(fr.sql()).isEqualTo("INSERT INTO t VALUES (1),(2),(3),(4),(5);");
    }

    @Test
    void isFullWhenMaxReached() {
        InsertMerger m = new InsertMerger(3);
        m.tryAbsorb("INSERT INTO t VALUES (1);");
        assertThat(m.isFull()).isFalse();
        m.tryAbsorb("INSERT INTO t VALUES (2);");
        assertThat(m.isFull()).isFalse();
        m.tryAbsorb("INSERT INTO t VALUES (3);");
        assertThat(m.isFull()).isTrue();

        FlushResult fr = m.flush();
        assertThat(fr.tupleCount()).isEqualTo(3);
        assertThat(m.isEmpty()).isTrue();
    }

    @Test
    void flushEmptyReturnsNull() {
        InsertMerger m = new InsertMerger(100);
        assertThat(m.flush()).isNull();
        assertThat(m.isEmpty()).isTrue();
    }

    @Test
    void insertOrReplaceAbsorbed() {
        InsertMerger m = new InsertMerger(100);
        assertThat(m.tryAbsorb("INSERT OR REPLACE INTO t VALUES (1);")).isTrue();
        assertThat(m.tryAbsorb("INSERT OR REPLACE INTO t VALUES (2);")).isTrue();

        FlushResult fr = m.flush();
        assertThat(fr.tupleCount()).isEqualTo(2);
        assertThat(fr.sql()).contains("INSERT OR REPLACE INTO t VALUES");
    }

    @Test
    void insertOrIgnoreAbsorbed() {
        InsertMerger m = new InsertMerger(100);
        assertThat(m.tryAbsorb("INSERT OR IGNORE INTO t VALUES (1);")).isTrue();
        assertThat(m.tryAbsorb("INSERT OR IGNORE INTO t VALUES (2);")).isTrue();

        FlushResult fr = m.flush();
        assertThat(fr.tupleCount()).isEqualTo(2);
        assertThat(fr.sql()).contains("INSERT OR IGNORE INTO t VALUES");
    }

    @Test
    void caseInsensitiveKeywords() {
        InsertMerger m = new InsertMerger(100);
        assertThat(m.tryAbsorb("insert into t values (1);")).isTrue();
        assertThat(m.tryAbsorb("INSERT INTO t VALUES (2);")).isTrue();
        assertThat(m.tryAbsorb("Insert Into t Values (3);")).isTrue();

        FlushResult fr = m.flush();
        assertThat(fr.tupleCount()).isEqualTo(3);
    }

    @Test
    void multilineInsert() {
        InsertMerger m = new InsertMerger(100);
        String sql1 = "INSERT INTO t VALUES (\n  1,\n  'hello',\n  NULL\n);";
        String sql2 = "INSERT INTO t VALUES (\n  2,\n  'world',\n  42\n);";
        assertThat(m.tryAbsorb(sql1)).isTrue();
        assertThat(m.tryAbsorb(sql2)).isTrue();

        FlushResult fr = m.flush();
        assertThat(fr.tupleCount()).isEqualTo(2);
    }

    @Test
    void schemaPrefixedTable() {
        InsertMerger m = new InsertMerger(100);
        // 转换后的 SQL 可能还带 schema 前缀（转换器应已剥离，但 merger 不依赖此假设）
        assertThat(m.tryAbsorb("INSERT INTO edr.users VALUES (1);")).isTrue();
        assertThat(m.tryAbsorb("INSERT INTO edr.users VALUES (2);")).isTrue();
        // 不同 schema 不合并
        assertThat(m.tryAbsorb("INSERT INTO public.users VALUES (3);")).isFalse();
    }
}
