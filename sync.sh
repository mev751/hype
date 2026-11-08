#!/usr/bin/env bash
# 一键同步: 提交本目录所有变更并推送到 GitHub (mev751/hype)
# 用法: ./sync.sh [提交说明]    默认提交说明为当前时间
# 推送策略:
#   1) 直连推送 (~/.ssh/config 已为 github.com 配置 github-proxy.sh, 直连失败自动改用隧道);
#   2) 直连/隧道均失败时, 回退到 SSH 443 端口 (ssh.github.com) + 本机 HTTP 代理隧道。
set -uo pipefail
cd "$(dirname "$0")" || exit 1

if ! git remote get-url origin >/dev/null 2>&1; then
    echo "[同步] 未配置 origin 远端仓库，跳过。" >&2
    exit 1
fi

# 提交所有变更 (新增/修改/删除; 按 .gitignore 规则排除的不会包含)
git add -A
if git diff --cached --quiet; then
    echo "[同步] 无变更，无需提交/推送。"
    exit 0
fi

MSG="${1:-$(date '+%Y-%m-%d %H:%M:%S') 自动同步}"
if ! git commit -m "$MSG"; then
    echo "[同步] 提交失败。" >&2
    exit 1
fi

# 方式1: 直连 (已配置自动回退, 无需额外处理)
if git push origin main; then
    echo "[同步] 推送成功 🎉"
    exit 0
fi

# 方式2: 强制走本机 HTTP 代理隧道 (ssh.github.com:443)
echo "[同步] 直连推送失败，回退: 代理隧道 ssh.github.com:443 ..."
if GIT_SSH_COMMAND="ssh -p 443 -o ProxyCommand=\"python3 $HOME/.ssh/github_http_tunnel.py %h %p\"" \
   git push ssh://git@ssh.github.com:443/mev751/hype.git main; then
    echo "[同步] 代理隧道推送成功 🎉"
    exit 0
fi

echo "[同步] 所有推送方式均失败 ❌" >&2
exit 1