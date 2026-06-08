#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy.sh — Deploy do Pegasus para o servidor genesys-ubuntu via Tailscale
#
# Fluxo:
#   0. scp .env  server → local  (fonte atual antes de alterar)
#   1. git add + commit + push   (código versionado)
#   2. scp .env  local → server  (.env nunca vai no git, tem tokens)
#   3. git pull  no server       (deploy do código via Git)
#   4. restart do bot            (opcional: --restart)
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
DEPLOY_REMOTE="${DEPLOY_REMOTE:-deploy}"
DEPLOY_BRANCH="${DEPLOY_BRANCH:-main}"
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

# ── 0. Capturar .env remoto sem sobrescrever ajustes locais ───────────────────
echo ""
echo "▶ [0/4] Capturando .env remoto para backup local..."
if $SSH "$SERVER" "[ -f ${REMOTE_DIR}/.env ]"; then
    mkdir -p .env_backups
    BACKUP_ENV=".env_backups/server-$(date +%Y%m%d_%H%M%S).env"
    $SCP "${SERVER}:${REMOTE_DIR}/.env" "$BACKUP_ENV"
    echo "  ✅ .env remoto salvo em $BACKUP_ENV"
else
    echo "  ⚠️  .env não encontrado no servidor, mantendo versão local"
fi

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
if ! git remote get-url "$DEPLOY_REMOTE" >/dev/null 2>&1; then
    echo "  ❌ Remote Git '$DEPLOY_REMOTE' não existe. Configure antes de deploy."
    exit 1
fi
git push "$DEPLOY_REMOTE" "HEAD:${DEPLOY_BRANCH}"
echo "  ✅ Push concluído em ${DEPLOY_REMOTE}/${DEPLOY_BRANCH}"

# ── 2. Copiar .env para o servidor ───────────────────────────────────────────
echo ""
echo "▶ [2/4] Copiando .env → server..."
$SCP .env "${SERVER}:${REMOTE_DIR}/.env"
echo "  ✅ .env copiado"

# ── 3. Git pull no servidor ──────────────────────────────────────────────────
echo ""
echo "▶ [3/4] Git pull no servidor..."
$SSH "$SERVER" "
    set -euo pipefail
    cd '$REMOTE_DIR'
    systemctl stop pegasus-optimizer.service pegasus-dashboard.service 2>/dev/null || true
    if ! git remote get-url '$DEPLOY_REMOTE' >/dev/null 2>&1; then
        git remote add '$DEPLOY_REMOTE' /opt/pegasus-deploy.git
    fi
    git fetch '$DEPLOY_REMOTE' '$DEPLOY_BRANCH'
    if [ -f logs/results.db ] && ! git diff --quiet -- logs/results.db 2>/dev/null; then
        mkdir -p logs/.deploy-backups
        cp logs/results.db logs/.deploy-backups/results-$(date +%Y%m%d_%H%M%S).db
        git checkout -- logs/results.db
    fi
    if ! git pull --ff-only '$DEPLOY_REMOTE' '$DEPLOY_BRANCH'; then
        echo '  ℹ️  ff-only falhou, tentando rebase limpo sobre deploy/main'
        git rebase '$DEPLOY_REMOTE'/'$DEPLOY_BRANCH'
    fi
    systemctl start pegasus-optimizer.service pegasus-dashboard.service
"
echo "  ✅ Pull OK via ${DEPLOY_REMOTE}/${DEPLOY_BRANCH}"


# ── 4. Restart do bot (opcional) ─────────────────────────────────────────────
if [ "$DO_RESTART" = true ]; then
    echo ""
    echo "\u25b6 [4/4] Reiniciando bot..."
    $SSH "$SERVER" "
        screen -S pegasus -X quit 2>/dev/null || true
        sleep 2
        cd $REMOTE_DIR

        # FIX 1: Preserva start_of_day_balance se for o mesmo dia (sessao ativa).
        # Zera contadores operacionais mas mantém a referencia de P&L do dia.
        # Isso garante que stop loss/gain calculem sobre o inicio REAL do dia,
        # nao sobre o saldo do momento do restart.
        python3 - << 'PYEOF'
import json, datetime, pathlib, time
today = datetime.date.today().isoformat()
state_path = pathlib.Path('$REMOTE_DIR/logs/risk_state.json')

# Tenta preservar todo o estado de risco se for o mesmo dia (evita zerar P&L no restart)
state = None
try:
    if state_path.exists():
        old = json.loads(state_path.read_text())
        if old.get('day') == today:
            state = old
            print(f'  Mesmo dia: mantendo estado de risco integro (lucro_liquido={old.get(\"daily_net_profit\", 0.0):.2f})')
except Exception as e:
    print(f'  Erro ao tentar carregar estado antigo: {e}')

if state is None:
    state = {
        'day': today,
        'start_of_day_balance': 50.0,
        'daily_loss': 0.0, 'daily_net_profit': 0.0,
        'daily_peak_profit': 0.0,
        'daily_trailing_active': False, 'trades_today': 0, 'wins': 0, 'losses': 0,
        'consecutive_losses': 0, 'max_loss_streak_today': 0,
        'soros_step': 0, 'soros_profit': 0.0,
        'martingale_step': 0, 'martingale_accumulated_loss': 0.0,
        'martingale_base_stake': 0.0, 'loss_block_override': False,
        'session_start_ts': time.time(),
        'cooldown_until': 0.0
    }
    print(f'  Novo dia detectado ou sem estado antigo: inicializando risk_state para {today}')

state_path.write_text(json.dumps(state, indent=2))
PYEOF

        > logs/trades.csv
        > logs/signals.csv
        screen -dmS pegasus bash -c 'cd $REMOTE_DIR && PEGASUS_LIVE_BOT=true .venv/bin/python bot.py >> logs/trades.log 2>&1'
        sleep 4
        if pgrep -f 'python.*bot.py' > /dev/null; then
            echo '  \u2705 Bot reiniciado OK'
            pgrep -a -f 'python.*bot.py'
        else
            echo '  \u274c ERRO: bot nao subiu — cheque os logs!'
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
