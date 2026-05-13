# Pegasus

Bot educacional para operar Accumulators (`ACCU`) na Deriv usando ticks de 1 segundo. O Pegasus foi simplificado para este modelo e nao mantem a estrategia direcional antiga.

## Aviso De Risco

Accumulators, opcoes binarias, derivados e indices sinteticos envolvem risco alto de perda total do capital. O projeto vem em modo seguro por padrao (`DRY_RUN=true`) e deve ser validado em conta demo antes de qualquer uso real.

## Estrutura

```text
.
├── bot.py
├── config.py
├── strategy.py
├── risk_manager.py
├── journal.py
├── backtest.py
├── optimize.py
├── download_ticks.py
├── download_candles.py
├── requirements.txt
├── .env.example
└── tests/
```

`download_candles.py` e apenas um wrapper de compatibilidade para `download_ticks.py`.

## Instalar

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
chmod 600 .env
```

Edite `.env` e coloque um token demo da Deriv. O arquivo `.env` esta no `.gitignore`.

## Configuracao Principal

```env
BOT_NAME=Pegasus
ACCOUNT_MODE=demo
CONTRACT_MODE=accumulator
SYMBOL=1HZ100V
DRY_RUN=true
ALLOW_REAL_TRADING=false
```

Com `ACCOUNT_MODE=demo`, o Pegasus encerra se a API autorizar uma conta real. Para usar conta real no futuro, `DRY_RUN=false` e `ALLOW_REAL_TRADING=true` precisam ser definidos explicitamente.

## Estrategia Accumulators

O Pegasus busca compressao de volatilidade antes de enviar uma proposta `ACCU`:

```env
TICK_COUNT=300
ACCUMULATOR_GROWTH_RATE=0.03
ACCUMULATOR_TAKE_PROFIT_PERCENT=3.0
ACCUMULATOR_MAX_HOLD_TICKS=8
ACCUMULATOR_COOLDOWN_TICKS=3
ACCUMULATOR_BB_WINDOW=20
ACCUMULATOR_BB_STD_DEV=2.0
ACCUMULATOR_MAX_BB_WIDTH_PERCENT=0.08
ACCUMULATOR_ATR_WINDOW=20
ACCUMULATOR_MAX_TICK_ATR_PERCENT=0.015
ACCUMULATOR_RECENT_WINDOW=5
ACCUMULATOR_MAX_RECENT_MOVE_PERCENT=0.05
ACCUMULATOR_MIN_SCORE=7
```

O score usa tres filtros:

- `bb_width_percent`: largura das Bandas de Bollinger em percentual do preco.
- `tick_atr_percent`: media do movimento absoluto tick a tick.
- `recent_move_percent`: deslocamento absoluto nos ultimos ticks.

Quando o contrato esta aberto, o bot monitora `proposal_open_contract` e vende via API ao atingir o lucro alvo ou `ACCUMULATOR_MAX_HOLD_TICKS`. A ideia operacional e sair rapido; Accumulators punem permanencia longa em regioes de spike.

## Rodar

```bash
source venv/bin/activate
python bot.py
```

Logs:

```bash
tail -f logs/trades.log
```

CSVs gerados:

- `logs/signals.csv`: sinais aceitos com `entry_epoch`, score e metricas de compressao.
- `logs/trades.csv`: contratos executados, lucro, resultado, `exit_epoch` e `held_ticks`.
- `logs/risk_state.json`: estado diario de risco para sobreviver a restart.

## Regras De Risco

- Bloqueia novas entradas ao atingir `MAX_LOSS_PER_DAY`.
- Bloqueia novas entradas ao atingir `MAX_PROFIT_PER_DAY`, quando maior que zero.
- Bloqueia novas entradas ao atingir `MAX_TRADES_PER_DAY`.
- Bloqueia novas entradas ao atingir `MAX_CONSECUTIVE_LOSSES`.
- Ativa trailing diario com `DAILY_TRAILING_START` e protege `DAILY_TRAILING_LOCK`.
- Pode usar Soros conservador com `USE_SOROS=true`, `SOROS_MAX_STEPS` e `SOROS_PROFIT_FACTOR`.
- Calcula a stake como o menor valor entre `STAKE`, `balance * MAX_STAKE_PERCENT` e `MAX_STAKE`.
- Nao opera se a stake calculada ficar abaixo de `MIN_STAKE`.
- Respeita `ACCUMULATOR_COOLDOWN_TICKS` depois de cada entrada.

## Baixar Ticks

```bash
source venv/bin/activate
python download_ticks.py --symbol 1HZ100V --count 5000 --output data/ticks_1HZ100V.csv
```

O CSV precisa ter:

```csv
epoch,quote
1700000000,1234.56
```

## Backtest

```bash
python backtest.py \
  --ticks data/ticks_1HZ100V.csv \
  --initial-balance 1000 \
  --stake 1 \
  --growth-rate 0.03 \
  --take-profit-percent 3 \
  --barrier-percent 0.05 \
  --max-hold-ticks 8 \
  --cooldown-ticks 3 \
  --output logs/accumulator_backtest.csv
```

`--barrier-percent` e uma aproximacao local para simular perda por rompimento da barreira. A barreira real do Accumulator e controlada pela Deriv no contrato ao vivo, entao o backtest serve para eliminar configuracoes ruins e comparar filtros, nao para prometer resultado real.

## Otimizacao

```bash
python optimize.py \
  --ticks data/ticks_1HZ100V.csv \
  --bb-width-percents 0.04,0.06,0.08 \
  --tick-atr-percents 0.008,0.01,0.015 \
  --recent-move-percents 0.02,0.03,0.05 \
  --take-profit-percents 3,4,5 \
  --max-hold-ticks 3:8 \
  --cooldown-ticks 0:5 \
  --min-trades 20 \
  --output logs/accumulator_optimization.csv
```

O resultado ordena por lucro liquido, winrate, drawdown e sequencia de perdas. A configuracao vencedora ainda precisa passar por demo ao vivo.

## Testes

```bash
python -m unittest discover -s tests
```

## Documentacao Oficial

- Deriv Ticks History: https://developers.deriv.com/docs/data/ticks-history/
- Deriv Price Proposal: https://developers.deriv.com/docs/trading/proposal/
- Deriv Buy: https://developers.deriv.com/docs/trading/buy/
- Deriv Open Contract Status: https://developers.deriv.com/docs/trading/proposal-open-contract/
