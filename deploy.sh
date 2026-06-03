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

# ── 0. Sincronizar .env do servidor para o local ──────────────────────────────
echo ""
echo "▶ [0/4] Sincronizando .env remoto → local..."
if $SSH "$SERVER" "[ -f ${REMOTE_DIR}/.env ]"; then
    $SCP "${SERVER}:${REMOTE_DIR}/.env" .env
    echo "  ✅ .env local atualizado com a versão do servidor"
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
git push
echo "  ✅ Push OK"

# ── 2. Copiar .env e optimizer_state.json para o servidor ───────────────────
echo ""
echo "▶ [2/4] Copiando .env → server..."
$SCP .env "${SERVER}:${REMOTE_DIR}/.env"
echo "  ✅ .env copiado"
# Copia estado do optimizer (gitignored mas necessário para o dashboard)
if [ -f "logs/optimizer_state.json" ]; then
    $SCP logs/optimizer_state.json "${SERVER}:${REMOTE_DIR}/logs/optimizer_state.json" 2>/dev/null || true
    echo "  ✅ optimizer_state.json copiado"
fi

# ── 3. Git pull no servidor ──────────────────────────────────────────────────
echo ""
echo "▶ [3/4] Git pull no servidor..."
$SSH "$SERVER" "cd $REMOTE_DIR && git pull"
echo "  ✅ Pull OK"


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
        screen -dmS pegasus bash -c 'cd $REMOTE_DIR && PEGASUS_LIVE_BOT=true .venv/bin/python bot.py 2>&1 | tee -a logs/trades.log'
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
