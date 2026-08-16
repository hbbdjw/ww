package com.ww.storage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * 对象读取句柄：统一本地读（Drive 文件）与远端转发读（HTTP 流）。
 *
 * <p>调用方通过 {@link #openStream()} 获取数据流，<b>使用后必须关闭</b>以释放底层资源
 * （本地为文件句柄，远端为 HTTP 连接）。元数据通过 {@link #metadata()} 一次性获取。
 *
 * <p>当 {@link #file()} 返回非 null（本地读）时，调用方可直接基于该 File 构造
 * {@code FileSystemResource}，触发 Servlet 容器（如 Tomcat）的 sendfile 零拷贝响应，
 * 跳过 InputStream → OutputStream 的 JVM 内字节拷贝。
 */
public interface ReadHandle {

    ObjectMetadata metadata();

    /**
     * 打开数据流。通常只能消费一次（远端）。
     * 调用方负责关闭返回的 InputStream。
     */
    InputStream openStream() throws IOException;

    /**
     * 本地数据文件（仅本地读返回非 null；远端转发读返回 null）。
     * 返回非 null 时，调用方可优先使用 sendfile 零拷贝路径。
     */
    default File file() { return null; }
}
