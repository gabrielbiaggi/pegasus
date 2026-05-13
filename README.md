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
ACCUMULATOR_HAWKES_ALPHA=1.0
ACCUMULATOR_HAWKES_BETA=0.85
ACCUMULATOR_HAWKES_JUMP_ATR_MULTIPLIER=1.5
ACCUMULATOR_MAX_HAWKES_INTENSITY=0.2
ACCUMULATOR_IMBALANCE_WINDOW=10
ACCUMULATOR_MAX_ABS_TICK_IMBALANCE=2
ACCUMULATOR_HURST_WINDOW=30
ACCUMULATOR_MAX_HURST_EXPONENT=0.45
ACCUMULATOR_DERIVATIVE_WINDOW=20
ACCUMULATOR_MAX_VELOCITY_ZSCORE=2.0
ACCUMULATOR_MAX_ACCELERATION_ZSCORE=2.0
ACCUMULATOR_INTEGRAL_WINDOW=20
ACCUMULATOR_MAX_PMI_DISTANCE_PERCENT=0.005
ACCUMULATOR_MARKOV_WINDOW=50
ACCUMULATOR_MAX_MARKOV_CONTINUATION_PROB=0.45
ACCUMULATOR_SHANNON_ENTROPY_WINDOW=30
ACCUMULATOR_MIN_SHANNON_ENTROPY=0.80
ACCUMULATOR_KALMAN_Q=1e-5
ACCUMULATOR_KALMAN_R=1e-2
ACCUMULATOR_MAX_KALMAN_RESIDUAL_ZSCORE=2.0
```

O score usa tres filtros:

- `bb_width_percent`: largura das Bandas de Bollinger em percentual do preco.
- `tick_atr_percent`: media do movimento absoluto tick a tick.
- `recent_move_percent`: deslocamento absoluto nos ultimos ticks.

Depois do score minimo, o sinal passa por filtros quantitativos em cascata:

- `hurst_exponent < 0.45`: rejeita memoria/tendencia.
- `tick_imbalance` entre `-2` e `+2`: exige lateralizacao curta.
- `hawkes_intensity <= 0.2`: rejeita auto-excitacao depois de saltos.
- `velocity_zscore` e `acceleration_zscore <= 2.0`: bloqueia velocidade/aceleracao anormais.
- `pmi_distance_percent <= 0.005`: exige preco perto do centro de massa pela integral trapezoidal.
- `markov_p_up_given_up` e `markov_p_down_given_down < 0.45`: rejeita continuidade direcional.
- `shannon_entropy >= 0.80`: exige ruido distribuido, sem padrao direcional concentrado.
- `kalman_residual_zscore <= 2.0`: rejeita tick distante demais do estado estimado pelo filtro de Kalman.

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

```text
logs/signals.csv
logs/trades.csv
```

## Validacao demo

Smoke test de compra e fechamento real na conta demo:

```bash
source .venv/bin/activate
python demo_smoke_test.py --stake 1
```

O script bloqueia se o token autorizar uma conta que nao seja `VRTC`.

Coleta shadow para calibracao:

```bash
python shadow_collect.py --ticks 600 --output data/shadow_ticks.csv
```

Esse arquivo grava cada tick com score, filtros quantitativos e o resultado futuro simulado dentro de `ACCUMULATOR_MAX_HOLD_TICKS`. Ele nao compra contratos.

Otimizacao com os novos filtros:

```bash
python optimize.py \
  --ticks data/ticks_1HZ100V.csv \
  --max-hurst-exponents 0.45,0.50,0.55,0.60 \
  --max-pmi-distance-percents 0.005,0.01,0.02,0.05 \
  --max-hawkes-intensities 0.2,0.5,1.0 \
  --max-abs-tick-imbalances 2,3,4 \
  --max-markov-continuation-probs 0.45,0.50,0.60 \
  --min-shannon-entropies 0.70,0.80,0.90 \
  --max-kalman-residual-zscores 2.0,2.5,3.0
```

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
  --max-hurst-exponents 0.45,0.55,0.60 \
  --max-pmi-distance-percents 0.005,0.02,0.05 \
  --max-hawkes-intensities 0.2,1.0 \
  --max-abs-tick-imbalances 2,4 \
  --max-markov-continuation-probs 0.45,0.55 \
  --min-shannon-entropies 0.70,0.80 \
  --max-kalman-residual-zscores 2.0,3.0 \
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
