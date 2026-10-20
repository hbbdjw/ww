#!/usr/bin/env bash
# 对比 HTTP 提供的 JS 与本地文件
curl -s http://localhost:8091/assets/index-C9U_L-dW.js -o /tmp/served.js
md5sum /tmp/served.js /mnt/f/ww/ww/runtime/server/src/main/resources/static/assets/index-C9U_L-dW.js
echo "=== served.js line 12 (len + head) ==="
awk 'NR==12 {print "len=" length($0); print substr($0, 640, 80)}' /tmp/served.js
echo "=== diff with local (first 3 hunks) ==="
diff <(head -c 2000 /tmp/served.js) <(head -c 2000 /mnt/f/ww/ww/runtime/server/src/main/resources/static/assets/index-C9U_L-dW.js) && echo "first 2000 bytes identical"
