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

```bash
source venv/bin/activate
python bot.py
```

Logs:

```bash
tail -f logs/trades.log
```

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
- Bloqueia novas entradas ao atingir `MAX_CONSECUTIVE_LOSSES`.
- Calcula a stake como o menor valor entre `STAKE`, `balance * MAX_STAKE_PERCENT` e `MAX_STAKE`.
- Nao opera se a stake calculada ficar abaixo de `MIN_STAKE`.
- Avalia somente candles fechados para evitar multiplas entradas no mesmo candle em formacao.

## Documentacao oficial usada

- Deriv Ticks History: https://developers.deriv.com/docs/data/ticks-history/
- Deriv Price Proposal: https://developers.deriv.com/docs/trading/proposal/
- Deriv Buy: https://developers.deriv.com/docs/trading/buy/
- Deriv Open Contract Status: https://developers.deriv.com/docs/trading/proposal-open-contract/
