# Pegasus

Bot educacional para operar contratos Rise/Fall (`CALL`/`PUT`) na Deriv usando candles de 1 minuto, RSI, MACD, Bollinger Bands e cruzamento de EMAs.

## Aviso de risco

Opcoes binarias, derivados e indices sinteticos envolvem risco alto de perda total do capital. Este projeto vem em modo seguro por padrao (`DRY_RUN=true`) e deve ser validado em conta demo por pelo menos 30 dias antes de qualquer uso com dinheiro real.

## Estrutura

```text
.
├── bot.py
├── config.py
├── logger.py
├── risk_manager.py
├── strategy.py
├── requirements.txt
├── .env.example
└── logs/
```

## Instalar

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
chmod 600 .env
```

Edite `.env` e coloque um token demo da Deriv. O arquivo `.env` esta no `.gitignore`.

## Rodar em modo seguro

Com `DRY_RUN=true`, o bot conecta, carrega candles, calcula sinais e registra o que faria, mas nao envia compras.
Com `ACCOUNT_MODE=demo`, o Pegasus encerra se o token autorizado nao for de conta demo.

```bash
source venv/bin/activate
python bot.py
```

Logs:

```bash
tail -f logs/trades.log
```

O Pegasus tambem grava:

- `logs/signals.csv`: todos os sinais aceitos pela estrategia.
- `logs/trades.csv`: contratos executados e resultado final.
- `logs/risk_state.json`: estado diario de risco para sobreviver a restart.

## Habilitar compra em demo

Use token de conta demo e ajuste:

```env
DRY_RUN=false
ALLOW_REAL_TRADING=false
```

Se o token for de conta real, o bot bloqueia a compra enquanto `ALLOW_REAL_TRADING=false`.

## Habilitar conta real

Somente depois de validar a estrategia em demo:

```env
DRY_RUN=false
ALLOW_REAL_TRADING=true
MAX_LOSS_PER_DAY=10.00
MAX_STAKE_PERCENT=0.01
STAKE=1.00
```

O bot persiste o estado diario de risco em `logs/risk_state.json` para evitar que um restart zere a perda diaria.

## Regras de risco

- Bloqueia novas entradas ao atingir `MAX_LOSS_PER_DAY`.
- Bloqueia novas entradas ao atingir `MAX_PROFIT_PER_DAY`, quando maior que zero.
- Bloqueia novas entradas ao atingir `MAX_TRADES_PER_DAY`.
- Bloqueia novas entradas ao atingir `MAX_CONSECUTIVE_LOSSES`.
- Calcula a stake como o menor valor entre `STAKE`, `balance * MAX_STAKE_PERCENT` e `MAX_STAKE`.
- Nao opera se a stake calculada ficar abaixo de `MIN_STAKE`.
- Avalia somente candles fechados para evitar multiplas entradas no mesmo candle em formacao.
- Respeita `COOLDOWN_CANDLES` depois de cada entrada.

## Backtest

Baixe candles publicos da Deriv:

```bash
source venv/bin/activate
python download_candles.py --symbol R_100 --granularity 60 --count 5000 --output data/candles_R_100.csv
```

Rode a simulacao:

```bash
python backtest.py --candles data/candles_R_100.csv --initial-balance 1000 --duration-candles 5 --stake 1 --payout 0.85 --output logs/backtest_trades.csv
```

O backtest e aproximado: ele usa fechamento futuro do candle e payout fixo. Ele serve para filtrar configuracoes ruins antes de qualquer teste demo ao vivo, nao para prometer resultado real.

## Otimizar parametros

Depois de baixar candles, rode uma grade simples de parametros:

```bash
python optimize.py --candles data/candles_R_100.csv --min-scores 5:8 --durations 3:8 --cooldowns 0:3 --min-trades 20 --output logs/optimization.csv
```

O resultado ordena por lucro liquido, winrate, drawdown e sequencia de perdas. Use isso como filtro inicial; a configuracao vencedora ainda precisa passar por demo ao vivo.

## Testes

```bash
python -m unittest discover -s tests
```

## Documentacao oficial usada

- Deriv Ticks History: https://developers.deriv.com/docs/data/ticks-history/
- Deriv Price Proposal: https://developers.deriv.com/docs/trading/proposal/
- Deriv Buy: https://developers.deriv.com/docs/trading/buy/
- Deriv Open Contract Status: https://developers.deriv.com/docs/trading/proposal-open-contract/
