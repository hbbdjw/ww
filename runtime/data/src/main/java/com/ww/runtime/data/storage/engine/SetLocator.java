package com.ww.runtime.data.storage.engine;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Set 定位器：根据 (bucket, key) 选定对象所属的 Set 与 Set 内的 Drive。
 *
 * <p>定位逻辑基于 SHA-1 哈希做稳定散列，确保同一 (bucket,key) 始终映射到同一 Set/Drive。
 * 跨节点的归属路由（哪个节点是主）由 runtime placement 分区放置决定，二者互不耦合：
 * Set 负责"本地落到哪个磁盘"，placement 负责"哪个节点持有主副本"。
 */
public class SetLocator {

    private final DriveManager driveManager;
    private final ThreadLocal<MessageDigest> sha1;

    public SetLocator(DriveManager driveManager) {
        this.driveManager = driveManager;
        this.sha1 = ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                throw new StorageException("SHA-1 不可用", e);
            }
        });
    }

    /** 计算 (bucket,key) 的 SHA-1 十六进制字符串，用作对象目录名 */
    public String keySha1(String bucket, String key) {
        MessageDigest md = sha1.get();
        md.reset();
        byte[] digest = md.digest((bucket + "\0" + key).getBytes(StandardCharsets.UTF_8));
        return toHex(digest);
    }

    /** 选择对象所属 Set（含 Set 内 Drive） */
    public LocatedDrive locate(String bucket, String key) {
        String h = keySha1(bucket, key);
        // 取前 16 hex 位做 long hash，足够分散
        long hash = parseHexLong(h, 16);
        int setCount = Math.max(1, driveManager.getSetCount());
        int setIndex = (int) ((hash & Long.MAX_VALUE) % setCount);
        StorageSet set = driveManager.getSet(setIndex);
        Drive drive = set.selectDrive(hash >>> 32);
        return new LocatedDrive(setIndex, set, drive, h);
    }

    private static long parseHexLong(String hex, int len) {
        int n = Math.min(len, hex.length());
        long v = 0;
        for (int i = 0; i < n; i++) {
            v = (v << 4) | hexDigit(hex.charAt(i));
        }
        return v;
    }

    private static int hexDigit(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return 0;
    }

    private static String toHex(byte[] bytes) {
        char[] hex = new char[bytes.length * 2];
        String digits = "0123456789abcdef";
        for (int i = 0; i < bytes.length; i++) {
            hex[i * 2] = digits.charAt((bytes[i] >> 4) & 0xF);
            hex[i * 2 + 1] = digits.charAt(bytes[i] & 0xF);
        }
        return new String(hex);
    }

    /** 定位结果：Set 索引、Set、选中的 Drive、key 的 SHA-1 */
    public static class LocatedDrive {
        private final int setIndex;
        private final StorageSet set;
        private final Drive drive;
        private final String keySha1;

        LocatedDrive(int setIndex, StorageSet set, Drive drive, String keySha1) {
            this.setIndex = setIndex;
            this.set = set;
            this.drive = drive;
            this.keySha1 = keySha1;
        }

        public int getSetIndex() { return setIndex; }
        public StorageSet getSet() { return set; }
        public Drive getDrive() { return drive; }
        public String getKeySha1() { return keySha1; }
    }

    /** 给定一个已知 keySha1，定位到其所在的 Drive（用于读取已有对象） */
    public Drive locateBySha1(String keySha1) {
        long hash = parseHexLong(keySha1, 16);
        int setCount = Math.max(1, driveManager.getSetCount());
        int setIndex = (int) ((hash & Long.MAX_VALUE) % setCount);
        StorageSet set = driveManager.getSet(setIndex);
        return set.selectDrive(hash >>> 32);
    }

    /** 列出所有 Set（供扫描/list 使用） */
    public List<StorageSet> allSets() {
        return driveManager.getSets();
    }
}
