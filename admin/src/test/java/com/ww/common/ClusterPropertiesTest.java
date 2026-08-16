package com.ww.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ClusterProperties#findByHostHttpPort} 单元测试：
 * storage 节点（host:httpPort）与 cache 节点（host:cachePort）的同一物理节点对齐。
 */
@DisplayName("ClusterProperties storage/cache 节点对齐")
class ClusterPropertiesTest {

    @Test
    @DisplayName("显式配置 httpPort 时按 host+httpPort 精确匹配")
    void matchByExplicitHttpPort() {
        ClusterNode n1 = new ClusterNode("node1", 8701, 1.0);
        n1.setHttpPort(8081);
        ClusterNode n2 = new ClusterNode("node2", 8702, 1.0);
        n2.setHttpPort(8082);
        ClusterProperties props = new ClusterProperties();
        props.setNodes(java.util.List.of(n1, n2));

        assertThat(props.findByHostHttpPort("node1", 8081, 8701, 8080)).isSameAs(n1);
        assertThat(props.findByHostHttpPort("node2", 8082, 8701, 8080)).isSameAs(n2);
        assertThat(props.findByHostHttpPort("node1", 8082, 8701, 8080)).isNull();
        assertThat(props.findByHostHttpPort("node3", 8081, 8701, 8080)).isNull();
    }

    @Test
    @DisplayName("未配置 httpPort 时按对称部署偏移推导（port + serverPort - cachePort）")
    void matchByDerivedHttpPort() {
        ClusterNode n1 = new ClusterNode("node1", 8701, 1.0);
        ClusterNode n2 = new ClusterNode("node2", 8702, 1.0);
        ClusterProperties props = new ClusterProperties();
        props.setNodes(java.util.List.of(n1, n2));

        // offset = 8080 - 8701 = -621 → node1 httpPort = 8080，node2 = 8081
        assertThat(props.findByHostHttpPort("node1", 8080, 8701, 8080)).isSameAs(n1);
        assertThat(props.findByHostHttpPort("node2", 8081, 8701, 8080)).isSameAs(n2);
        assertThat(props.findByHostHttpPort("node2", 8082, 8701, 8080)).isNull();
    }

    @Test
    @DisplayName("同一 host 上多个节点（不同端口）互不混淆")
    void sameHostDistinctNodes() {
        ClusterNode n1 = new ClusterNode("127.0.0.1", 8701, 1.0);
        n1.setHttpPort(8081);
        ClusterNode n2 = new ClusterNode("127.0.0.1", 8702, 1.0);
        n2.setHttpPort(8082);
        ClusterProperties props = new ClusterProperties();
        props.setNodes(java.util.List.of(n1, n2));

        assertThat(props.findByHostHttpPort("127.0.0.1", 8081, 8701, 8080)).isSameAs(n1);
        assertThat(props.findByHostHttpPort("127.0.0.1", 8082, 8701, 8080)).isSameAs(n2);
    }

    @Test
    @DisplayName("非法入参与空节点列表返回 null")
    void invalidInputsReturnNull() {
        ClusterNode n1 = new ClusterNode("node1", 8701, 1.0);
        ClusterProperties props = new ClusterProperties();
        props.setNodes(java.util.List.of(n1));

        assertThat(props.findByHostHttpPort(null, 8081, 8701, 8080)).isNull();
        assertThat(props.findByHostHttpPort("node1", 0, 8701, 8080)).isNull();
        assertThat(props.findByHostHttpPort("node1", -1, 8701, 8080)).isNull();
        props.setNodes(null);
        assertThat(props.findByHostHttpPort("node1", 8081, 8701, 8080)).isNull();
    }
}
