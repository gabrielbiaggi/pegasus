#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy.sh — Deploy do Pegasus para o servidor genesys-ubuntu via Tailscale
#
# Fluxo:
#   1. git add + commit + push  (código)
#   2. scp .env  →  server      (.env nunca vai no git, tem tokens)
#   3. git pull  no server
#   4. restart do bot           (opcional: --restart)
#
# Uso:
#   ./deploy.sh                          # commit automático "deploy: sync"
#   ./deploy.sh "mensagem do commit"     # commit com mensagem custom
#   ./deploy.sh --restart                # commit + deploy + reinicia o bot
#   ./deploy.sh "msg" --restart          # tudo junto
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SERVER="root@100.98.249.29"
SSH_KEY="$HOME/.ssh/id_ed25519"
REMOTE_DIR="/opt/pegasus"
SSH="ssh -i $SSH_KEY -o IdentitiesOnly=yes -o StrictHostKeyChecking=no"
SCP="scp -i $SSH_KEY -o IdentitiesOnly=yes -o StrictHostKeyChecking=no"

DO_RESTART=false
COMMIT_MSG="deploy: sync"

# Parse argumentos
for arg in "$@"; do
    if [[ "$arg" == "--restart" ]]; then
        DO_RESTART=true
    elif [[ "$arg" != --* ]]; then
        COMMIT_MSG="$arg"
    fi
done

echo "════════════════════════════════════════════"
echo "  🚀 PEGASUS DEPLOY  →  genesys-ubuntu"
echo "════════════════════════════════════════════"

# ── 1. Git commit + push ──────────────────────────────────────────────────────
echo ""
echo "▶ [1/4] Git: commit + push..."
cd "$(dirname "$0")"

if git diff --quiet && git diff --cached --quiet; then
    echo "  Nenhuma mudança de código para commitar."
else
    git add -u                          # arquivos já rastreados (nunca adiciona .env)
    git add -A -- '*.py' '*.sh' '*.md' '*.txt' '*.json' '*.html' '*.yml' '*.yaml' 2>/dev/null || true
    git reset HEAD .env 2>/dev/null || true  # garante que .env nunca vai no commit
    git commit -m "$COMMIT_MSG"
    echo "  Commit: $COMMIT_MSG"
fi
git push
echo "  ✅ Push OK"

# ── 2. Copiar .env para o servidor ───────────────────────────────────────────
echo ""
echo "▶ [2/4] Copiando .env → server..."
$SCP .env "${SERVER}:${REMOTE_DIR}/.env"
echo "  ✅ .env copiado"

# ── 3. Git pull no servidor ──────────────────────────────────────────────────
echo ""
echo "▶ [3/4] Git pull no servidor..."
$SSH "$SERVER" "cd $REMOTE_DIR && git pull"
echo "  ✅ Pull OK"

# ── 4. Restart do bot (opcional) ─────────────────────────────────────────────
if [ "$DO_RESTART" = true ]; then
    echo ""
    echo "▶ [4/4] Reiniciando bot..."
    $SSH "$SERVER" "
        screen -S pegasus -X quit 2>/dev/null || true
        sleep 2
        cd $REMOTE_DIR
        > logs/trades.csv
        > logs/signals.csv
        screen -dmS pegasus bash -c 'cd $REMOTE_DIR && .venv/bin/python bot.py 2>&1 | tee -a logs/trades.log'
        sleep 4
        if pgrep -f 'python.*bot.py' > /dev/null; then
            echo '  ✅ Bot reiniciado OK'
            pgrep -a -f 'python.*bot.py'
        else
            echo '  ❌ ERRO: bot não subiu — cheque os logs!'
            tail -20 $REMOTE_DIR/logs/trades.log
            exit 1
        fi
    "
else
    echo ""
    echo "  ℹ️  Bot NÃO reiniciado. Use --restart para reiniciar."
fi

echo ""
echo "════════════════════════════════════════════"
echo "  ✅ DEPLOY CONCLUÍDO"
echo "════════════════════════════════════════════"
