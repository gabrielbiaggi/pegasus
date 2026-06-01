#!/usr/bin/env python3
"""
Pegasus Auto-Optimizer v2 — Ciclo infinito sobre ticks reais de Maio.

OBJETIVO REAL: Maximizar o LUCRO DIÁRIO consistente com banca de $50.
Meta do usuário: dobrar a banca todo dia (100%/dia = $50 de lucro/dia).

MÉTRICAS CORRETAS:
  - avg_daily_profit: lucro médio por dia com operação ($ reais)
  - positive_days: dias com lucro > 0
  - consistency_score: % de dias operando que foram positivos
  - daily_score: pontuação composta que guia o optimizer

CRITÉRIO DE ACEITAÇÃO (em ordem de prioridade):
  1. Maior número de dias positivos
  2. Maior lucro médio diário
  3. Maior score composto

O backtest usa $50 fixo por dia (sem compounding) para evitar distorções.
Roda infinitamente sobre 01/05/2026 a 31/05/2026 (ticks reais Deriv).
"""

import os
import sys
import json
import time
import random
import subprocess
from pathlib import Path

# ── Configuração ──────────────────────────────────────────────────────────────
START_DATE    = "2026-05-01"
END_DATE      = "2026-05-31"
START_BALANCE = "50.0"       # banca base — NUNCA mude isto!
ENV_PATH      = Path(".env")
STATE_PATH    = Path("logs/optimizer_state.json")   # lido pelo dashboard
TEMP_OUT      = Path("logs/backtest_opt_temp.json")

# ── Espaço de busca de parâmetros ────────────────────────────────────────────
PARAM_SPACE = {
    # Filtro de score mínimo do ensemble (mais alto = mais seletivo)
    "CALM_ACCU_MIN_SCORE": {"type": "int",       "min": 16, "max": 32, "step": 1},
    # Probabilidade mínima do ensemble XGBoost para aceitar entrada
    "ENSEMBLE_MIN_PROB":   {"type": "float",     "min": 0.20, "max": 0.40, "step": 0.01},
    # Máximo CUSUM para aceitar entrada (< = zona mais calma = mais seguro)
    "CALM_ACCU_MAX_ENTRY_CUSUM": {"type": "float", "min": 2.5, "max": 8.0, "step": 0.5},
    # Hurst mínimo: > 0.5 = trending, < 0.5 = mean-reverting
    "ACCUMULATOR_MIN_HURST_EXPONENT": {"type": "float", "min": 0.38, "max": 0.55, "step": 0.01},
    # Limiar de volatilidade para ativar Calm (menor = mais restritivo)
    "CALM_ACCU_THRESHOLD": {"type": "float_sci", "min": 0.8e-6, "max": 3.5e-6},
    # Limite do XGBoost bypass (quando XGB pode ser ignorado)
    "PCS_XGB_BYPASS_LIMIT": {"type": "float",   "min": 0.10, "max": 0.25, "step": 0.01},
    # Take profit regime B+ (agressivo, ticks up)
    "PCS_REGIME_B_PLUS_TP": {"type": "float",   "min": 5.0, "max": 20.0, "step": 1.0},
    # Take profit regime B- (defensivo, ticks down)
    "PCS_REGIME_B_MINUS_TP": {"type": "float",  "min": 1.0, "max": 6.0, "step": 0.5},
    # Stop loss % diário (se temos SL configurado)
    "STOP_LOSS_PCT": {"type": "float",           "min": 0.0, "max": 0.0, "step": 0.0},  # fixo
    # Stake base
    "STAKE": {"type": "float",                   "min": 1.0, "max": 5.0, "step": 0.5},
}

# Parâmetros que NÃO perturbar (muito sensíveis ao modelo)
FROZEN_PARAMS = {"STOP_LOSS_PCT"}

# ── Funções utilitárias ───────────────────────────────────────────────────────

def load_current_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def save_env(env_vars: dict[str, str]) -> None:
    if not ENV_PATH.exists():
        ENV_PATH.write_text("\n".join(f"{k}={v}" for k, v in env_vars.items()))
        return
    lines = ENV_PATH.read_text(encoding="utf-8").splitlines()
    new_lines, updated = [], set()
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            k = stripped.split("=", 1)[0].strip()
            if k in env_vars:
                new_lines.append(f"{k}={env_vars[k]}")
                updated.add(k)
                continue
        new_lines.append(line)
    for k, v in env_vars.items():
        if k not in updated:
            new_lines.append(f"{k}={v}")
    ENV_PATH.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def perturb_params(current: dict[str, str]) -> dict[str, str]:
    """Perturba 1–3 parâmetros aleatoriamente ao redor da melhor config atual."""
    perturbed = current.copy()
    eligible = [k for k in PARAM_SPACE if k not in FROZEN_PARAMS]
    num = random.randint(1, min(3, len(eligible)))
    keys = random.sample(eligible, num)

    for key in keys:
        space = PARAM_SPACE[key]
        curr_str = current.get(key)

        if space["type"] == "int":
            step = space.get("step", 1)
            curr_v = int(curr_str) if curr_str else (space["min"] + space["max"]) // 2
            new_v = curr_v + random.choice([-step, step])
            new_v = max(space["min"], min(space["max"], new_v))
            perturbed[key] = str(int(new_v))

        elif space["type"] == "float":
            step = space.get("step", 0.01)
            if step == 0.0:
                continue  # parâmetro fixo
            curr_v = float(curr_str) if curr_str else (space["min"] + space["max"]) / 2
            # Às vezes pula 2 steps para explorar mais
            multiplier = random.choice([-2, -1, 1, 2])
            new_v = curr_v + multiplier * step
            new_v = round(max(space["min"], min(space["max"], new_v)), 4)
            perturbed[key] = str(new_v)

        elif space["type"] == "float_sci":
            curr_v = float(curr_str) if curr_str else (space["min"] + space["max"]) / 2
            factor = random.choice([0.75, 0.9, 1.1, 1.25])
            new_v = curr_v * factor
            new_v = max(space["min"], min(space["max"], new_v))
            perturbed[key] = f"{new_v:.2e}"

    return perturbed


def compute_daily_score(results: list[dict], strategy: str = "Super-Frankenstein") -> dict:
    """
    Calcula métricas REAIS de lucro diário para o optimizer.

    Retorna dict com:
      - avg_daily_profit : lucro médio $ nos dias com operação
      - positive_days    : dias com lucro > 0
      - negative_days    : dias com pnl < 0
      - active_days      : dias com pelo menos 1 trade
      - consistency_pct  : % dias ativos que foram positivos (0..100)
      - total_pnl        : soma total $ de todo maio
      - score            : pontuação composta (quanto maior = melhor)
      - best_day_pnl     : melhor dia em $
      - worst_day_pnl    : pior dia em $
    """
    days_pnl = []
    for r in results:
        s = r.get("strategies", {}).get(strategy, {})
        pnl = s.get("pnl", 0.0)
        trades = s.get("trades", 0)
        days_pnl.append({"pnl": pnl, "trades": trades, "date": r.get("date", "?")})

    active  = [d for d in days_pnl if d["trades"] > 0]
    pos     = [d for d in days_pnl if d["pnl"] > 0]
    neg     = [d for d in days_pnl if d["pnl"] < 0]
    total_pnl = sum(d["pnl"] for d in days_pnl)

    n_active = len(active)
    n_pos    = len(pos)
    n_neg    = len(neg)

    avg_daily = (sum(d["pnl"] for d in active) / n_active) if n_active > 0 else 0.0
    consistency = (n_pos / n_active * 100) if n_active > 0 else 0.0

    best_day  = max((d["pnl"] for d in days_pnl), default=0.0)
    worst_day = min((d["pnl"] for d in days_pnl), default=0.0)

    # ── Score composto ────────────────────────────────────────────────────────
    # Objetivo: LUCRO DIÁRIO REAL com consistência.
    # Fórmula:
    #   score = avg_daily_profit * 10           (lucro médio em $, peso alto)
    #         + positive_days * 3               (cada dia positivo vale 3 pts)
    #         + consistency_pct * 0.5           (% consistência)
    #         - negative_days * 8               (penalidade pesada por dia negativo)
    #         - bust_penalty                    (bust zera o dia)
    #
    # Exemplos:
    #   Dia médio $5 de lucro, 20 pos, 2 neg, 0 bust:
    #     score = 50 + 60 + consistencia - 16 = ~120+
    #   Dia médio $0.50, 18 pos, 0 neg:
    #     score = 5 + 54 + ... = ~80 (pior!)
    # ─────────────────────────────────────────────────────────────────────────
    bust_penalty = sum(10.0 for d in days_pnl if d["pnl"] < -20)  # perda catastrófica

    score = (
        avg_daily * 10.0          # lucro médio em $ — o mais importante
        + n_pos * 3.0             # dias positivos
        + consistency * 0.5       # consistência percentual
        - n_neg * 8.0             # penalidade por dia negativo
        - bust_penalty            # penalidade por bust
    )

    return {
        "avg_daily_profit": round(avg_daily, 4),
        "positive_days": n_pos,
        "negative_days": n_neg,
        "active_days": n_active,
        "total_days": len(days_pnl),
        "consistency_pct": round(consistency, 1),
        "total_pnl": round(total_pnl, 2),
        "score": round(score, 4),
        "best_day_pnl": round(best_day, 2),
        "worst_day_pnl": round(worst_day, 2),
        "roi_pct": round(total_pnl / 50.0 * 100, 1),  # sempre sobre $50 base
    }


def run_backtest(env_vars: dict[str, str]) -> dict | None:
    """Executa backtest em $50 fixo (sem compounding) e retorna métricas reais."""
    my_env = os.environ.copy()
    my_env.update(env_vars)
    my_env["BACKTEST_COMPOUNDING"] = "false"  # FIXO: sem compounding (métricas reais!)

    if TEMP_OUT.exists():
        TEMP_OUT.unlink()

    cmd = [
        ".venv/bin/python", "backtest_engine.py",
        START_DATE, END_DATE, START_BALANCE, str(TEMP_OUT),
    ]
    try:
        subprocess.run(
            cmd, env=my_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True, timeout=450,
        )
    except Exception as e:
        print(f"   ⚠️  Backtest falhou: {e}")
        return None

    if not TEMP_OUT.exists():
        return None

    try:
        data = json.loads(TEMP_OUT.read_text(encoding="utf-8"))
        results = data.get("results", [])
        if not results:
            return None
        return compute_daily_score(results)
    except Exception as e:
        print(f"   ⚠️  Erro ao ler resultado: {e}")
        return None


def write_state(
    iteration: int,
    baseline: dict,
    best: dict | None,
    history: list[dict],
    running: bool = True,
) -> None:
    """Grava estado para o dashboard ler em tempo real."""
    try:
        STATE_PATH.write_text(json.dumps({
            "running": running,
            "current_iteration": iteration,
            "baseline": baseline,
            "best": best,
            "iterations": history[-100:],
            "last_update": time.time(),
            "start_date": START_DATE,
            "end_date": END_DATE,
        }, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def deploy_to_server(msg: str = "optimize: new best") -> bool:
    """Faz push + scp .env + restart bot. Retorna True se OK."""
    try:
        subprocess.run(
            ["./deploy.sh", msg, "--restart"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            check=True, timeout=120,
        )
        return True
    except Exception as e:
        print(f"   ❌ Deploy falhou: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def fmt(m: dict) -> str:
    return (
        f"avg_day=${m['avg_daily_profit']:.2f}"
        f" | pos={m['positive_days']} neg={m['negative_days']}"
        f" | consist={m['consistency_pct']:.0f}%"
        f" | total=${m['total_pnl']:.2f}"
        f" | score={m['score']:.1f}"
    )


def main():
    sep = "=" * 68
    print(sep)
    print("  🦅 PEGASUS OPTIMIZER v2 — Foco em LUCRO DIÁRIO REAL")
    print("  Meta: Maximizar lucro/dia consistente com banca de $50")
    print(sep)

    best_env = load_current_env()

    # Preenche parâmetros faltando com valores médios
    for key, space in PARAM_SPACE.items():
        if key not in best_env and key not in FROZEN_PARAMS:
            if space["type"] == "int":
                best_env[key] = str((space["min"] + space["max"]) // 2)
            elif space["type"] == "float":
                best_env[key] = f"{(space['min'] + space['max']) / 2:.4f}"
            elif space["type"] == "float_sci":
                best_env[key] = f"{(space['min'] + space['max']) / 2:.2e}"

    print("\n📊 Calculando baseline (parâmetros atuais)...")
    t0 = time.time()
    baseline_metrics = run_backtest(best_env)
    if not baseline_metrics:
        print("❌ Falha ao calcular baseline. Abortando.")
        sys.exit(1)

    print(f"   ✅ Baseline [{time.time()-t0:.0f}s]: {fmt(baseline_metrics)}")
    print(f"\n   ⚠️  Contexto: ROI total de {baseline_metrics['roi_pct']}% = ${baseline_metrics['total_pnl']:.2f} em 31 dias")
    print(f"   ⚠️  Lucro médio/dia: ${baseline_metrics['avg_daily_profit']:.2f}  (meta: $50/dia = dobrar banca)")
    print(f"   ⚠️  Consistência: {baseline_metrics['positive_days']} dias positivos / {baseline_metrics['active_days']} ativos")

    best_score  = baseline_metrics["score"]
    best_pos    = baseline_metrics["positive_days"]
    best_avg    = baseline_metrics["avg_daily_profit"]
    best_data   = {**baseline_metrics, "iteration": 0}

    iteration = 1
    history: list[dict] = []

    # Salva estado inicial
    write_state(0, baseline_metrics, None, history)

    print(f"\n{'─'*68}")
    print(f"  🔄 Iniciando loop infinito de otimização...")
    print(f"  Critério: 1) + dias positivos  2) + lucro médio/dia  3) + score")
    print(f"{'─'*68}\n")

    while True:
        print(f"🔄 Iteração {iteration} — testando nova combinação...")
        candidate = perturb_params(best_env)

        t0 = time.time()
        m = run_backtest(candidate)
        elapsed = time.time() - t0

        if not m:
            print(f"   ⚠️  Backtest falhou [{elapsed:.0f}s]. Pulando.")
            iteration += 1
            continue

        # Regra de aceitação (em cascata):
        #   1. Mais dias positivos → aceita
        #   2. Mesmo # positivos e maior avg_daily → aceita
        #   3. Mesmo # positivos, mesma avg → maior score → aceita
        is_better = False
        reason = ""
        if m["positive_days"] > best_pos:
            is_better = True
            reason = f"+ dias positivos ({best_pos}→{m['positive_days']})"
        elif m["positive_days"] == best_pos:
            if m["avg_daily_profit"] > best_avg + 0.01:
                is_better = True
                reason = f"+ lucro/dia (${best_avg:.2f}→${m['avg_daily_profit']:.2f})"
            elif abs(m["avg_daily_profit"] - best_avg) <= 0.01 and m["score"] > best_score + 0.1:
                is_better = True
                reason = f"+ score ({best_score:.1f}→{m['score']:.1f})"

        status = "🔥 MELHOR!" if is_better else "❌ Pior/Igual"
        print(f"   [{elapsed:.0f}s] {fmt(m)} → {status}")

        entry = {
            "iteration": iteration,
            "roi": m["roi_pct"],
            "pnl": m["total_pnl"],
            "busted": m["negative_days"],
            "elapsed_s": round(elapsed, 1),
            "is_best": is_better,
            "avg_daily": m["avg_daily_profit"],
            "positive_days": m["positive_days"],
            "consistency_pct": m["consistency_pct"],
            "score": m["score"],
            "ts": time.time(),
        }
        history.append(entry)

        if is_better:
            best_score  = m["score"]
            best_pos    = m["positive_days"]
            best_avg    = m["avg_daily_profit"]
            best_env    = candidate.copy()
            best_data   = {**m, "iteration": iteration, "reason": reason}

            params_str = {k: best_env.get(k) for k in PARAM_SPACE if k not in FROZEN_PARAMS}
            print(f"\n🏆 NOVO RECORDE! — {reason}")
            print(f"   Lucro médio/dia: ${m['avg_daily_profit']:.2f}")
            print(f"   Dias positivos: {m['positive_days']}/{m['active_days']} ({m['consistency_pct']:.0f}%)")
            print(f"   PnL total maio: ${m['total_pnl']:.2f}")
            print(f"   Parâmetros: {params_str}\n")

            # Salva .env localmente
            save_env(best_env)

            # Deploy para o servidor
            print("   🚀 Fazendo deploy no servidor...")
            ok = deploy_to_server(f"optimize v2 it{iteration}: avg_day=${m['avg_daily_profit']:.2f} pos={m['positive_days']}")
            if ok:
                print("   ✅ Deploy OK — bot ao vivo com novos parâmetros!\n")
            else:
                print("   ⚠️  Deploy falhou — parâmetros salvos localmente apenas.\n")

        # Atualiza dashboard
        write_state(iteration, baseline_metrics, best_data if best_pos > 0 else None, history)

        iteration += 1
        time.sleep(0.5)


if __name__ == "__main__":
    main()
