#!/usr/bin/env python3
"""TCP -> unix-socket bridge: expose the WSL-internal dockerd on tcp://0.0.0.0:2375.

用途（Task 11 集成验证）：本机 Docker daemon 只在 WSL 发行版内监听
unix:///var/run/docker.sock，Windows 侧无 docker.exe / npipe。Testcontainers
（Java，运行在 Windows JVM）需要 DOCKER_HOST 可达，故在 WSL 内启动本脚本：
    wsl -e sh -c "nohup python3 /mnt/f/ww/ww/scripts/runtime-docker-bridge.py >/tmp/rt-docker-bridge.log 2>&1 &"
随后 Windows 侧设置:
    DOCKER_HOST=tcp://localhost:2375   （依赖 WSL2 localhost 端口转发）
仅 stdlib，双向字节泵 + 半关闭传递，满足 HTTP/1.1 keep-alive。
"""
import sys
import socket
import socketserver
import threading

DOCKER_SOCK = "/var/run/docker.sock"
# 默认绑 127.0.0.1：WSL mirrored 网络下仅回环绑定可被 Windows 侧 localhost 访问
LISTEN = (sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1", int(sys.argv[2]) if len(sys.argv) > 2 else 2375)


def pump(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        upstream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            upstream.connect(DOCKER_SOCK)
        except OSError:
            self.request.close()
            return
        t = threading.Thread(target=pump, args=(self.request, upstream), daemon=True)
        t.start()
        pump(upstream, self.request)
        t.join(timeout=5)
        upstream.close()


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    print("bridging %s:%s -> %s" % (LISTEN[0], LISTEN[1], DOCKER_SOCK), flush=True)
    Server(LISTEN, Handler).serve_forever()
