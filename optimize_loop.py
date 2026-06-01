#!/usr/bin/env python3
"""
Pegasus Auto-Optimizer Loop — Ciclo infinito de auditoria, testes e auto-correção contínua.
Roda backtests rápidos, perturba hiperparâmetros, encontra recordes de ROI e faz deploy + restart no bot ao vivo.
"""

import os
import sys
import json
import time
import random
import subprocess
from pathlib import Path

# Configuração
START_DATE = "2026-05-01"
END_DATE = "2026-05-31"
START_BALANCE = "50.0"
OUTPUT_JSON = "logs/backtest_full_may.json"
ENV_PATH = Path(".env")
STATE_PATH = Path("logs/optimizer_state.json")  # lido pelo dashboard

# Hiperparâmetros a serem otimizados com seus tipos e limites
PARAM_SPACE = {
    "CALM_ACCU_MIN_SCORE": {"type": "int", "min": 18, "max": 30, "step": 1},
    "ENSEMBLE_MIN_PROB": {"type": "float", "min": 0.22, "max": 0.35, "step": 0.01},
    "CALM_ACCU_MAX_ENTRY_CUSUM": {"type": "float", "min": 3.0, "max": 7.0, "step": 0.5},
    "ACCUMULATOR_MIN_HURST_EXPONENT": {"type": "float", "min": 0.40, "max": 0.52, "step": 0.01},
    "CALM_ACCU_THRESHOLD": {"type": "float_sci", "min": 1.0e-6, "max": 2.5e-6},
    "PCS_XGB_BYPASS_LIMIT": {"type": "float", "min": 0.10, "max": 0.22, "step": 0.01},
    "PCS_REGIME_B_PLUS_TP": {"type": "float", "min": 6.0, "max": 15.0, "step": 1.0},
    "PCS_REGIME_B_MINUS_TP": {"type": "float", "min": 2.0, "max": 5.0, "step": 0.5},
}


def load_current_env() -> dict[str, str]:
    """Carrega o .env atual como dicionário."""
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                env[key.strip()] = val.strip()
    return env


def save_env(env_vars: dict[str, str]) -> None:
    """Atualiza e salva o .env preservando o layout original onde possível."""
    if not ENV_PATH.exists():
        content = "\n".join(f"{k}={v}" for k, v in env_vars.items())
        ENV_PATH.write_text(content, encoding="utf-8")
        return

    lines = ENV_PATH.read_text(encoding="utf-8").splitlines()
    new_lines = []
    updated_keys = set()

    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in env_vars:
                new_lines.append(f"{key}={env_vars[key]}")
                updated_keys.add(key)
                continue
        new_lines.append(line)

    # Adiciona novas chaves
    for key, val in env_vars.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={val}")

    ENV_PATH.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def perturb_params(current: dict[str, str]) -> dict[str, str]:
    """Aplica perturbações aleatórias em torno da melhor configuração atual."""
    perturbed = current.copy()
    
    # Decide quantas variáveis perturbar (entre 1 e 3)
    num_to_perturb = random.randint(1, 3)
    keys_to_perturb = random.sample(list(PARAM_SPACE.keys()), num_to_perturb)
    
    for key in keys_to_perturb:
        space = PARAM_SPACE[key]
        curr_val_str = current.get(key)
        
        if space["type"] == "int":
            curr_val = int(curr_val_str) if curr_val_str else (space["min"] + space["max"]) // 2
            # Perturbação de -step ou +step
            step = space.get("step", 1)
            new_val = curr_val + random.choice([-step, step])
            new_val = max(space["min"], min(space["max"], new_val))
            perturbed[key] = str(new_val)
            
        elif space["type"] == "float":
            curr_val = float(curr_val_str) if curr_val_str else (space["min"] + space["max"]) / 2.0
            step = space.get("step", 0.01)
            new_val = curr_val + random.choice([-step, step])
            new_val = round(max(space["min"], min(space["max"], new_val)), 4)
            perturbed[key] = str(new_val)
            
        elif space["type"] == "float_sci":
            curr_val = float(curr_val_str) if curr_val_str else (space["min"] + space["max"]) / 2.0
            # Multiplica por um fator levemente acima/abaixo de 1
            factor = random.choice([0.9, 1.1])
            new_val = curr_val * factor
            new_val = max(space["min"], min(space["max"], new_val))
            perturbed[key] = f"{new_val:.2e}"
            
    return perturbed


def run_backtest_local(env_vars: dict[str, str]) -> dict | None:
    """Roda o backtest local com o ambiente modificado e extrai estatísticas de Super-Frankenstein."""
    my_env = os.environ.copy()
    my_env.update(env_vars)
    # Habilita compounding para otimização fiel
    my_env["BACKTEST_COMPOUNDING"] = "true"

    output_path = Path("logs/backtest_opt_temp.json")
    if output_path.exists():
        output_path.unlink()

    cmd = [
        ".venv/bin/python",
        "backtest_engine.py",
        START_DATE,
        END_DATE,
        START_BALANCE,
        str(output_path)
    ]

    try:
        # Silencia a maior parte dos prints no backtest local
        subprocess.run(
            cmd,
            env=my_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=400
        )
    except Exception as e:
        print(f"❌ Falha ao rodar backtest: {e}")
        return None

    if not output_path.exists():
        return None

    try:
        data = json.loads(output_path.read_text(encoding="utf-8"))
        summary = data.get("summary", {})
        sf_summary = summary.get("strategies", {}).get("Super-Frankenstein", {})
        return sf_summary
    except Exception as e:
        print(f"❌ Erro ao ler resultado do backtest: {e}")
        return None


def main():
    print("=====================================================================")
    print("  🌀 PEGASUS AUTO-OPTIMIZER LOOP STARTED")
    print("=====================================================================")

    # Carrega env inicial
    best_env = load_current_env()
    
    # Preenche valores ausentes com padrões do PARAM_SPACE
    for key, space in PARAM_SPACE.items():
        if key not in best_env:
            if space["type"] == "int":
                best_env[key] = str((space["min"] + space["max"]) // 2)
            elif space["type"] == "float":
                best_env[key] = f"{(space['min'] + space['max']) / 2.0:.4f}"
            elif space["type"] == "float_sci":
                best_env[key] = f"{(space['min'] + space['max']) / 2.0:.2e}"

    print("Calculando baseline inicial...")
    baseline = run_backtest_local(best_env)
    
    if not baseline:
        print("❌ Erro ao calcular baseline inicial. Abortando.")
        sys.exit(1)

    best_roi = baseline.get("roi_pct", -9999.0)
    best_busted = baseline.get("busted_days", 99)
    best_pnl = baseline.get("total_pnl", -9999.0)

    print(f" Baseline Inicial:")
    print(f"   ROI: {best_roi}% | PnL: ${best_pnl:.2f} | Dias Quebrados: {best_busted}")
    print(f"   Hiperparâmetros: { {k: best_env.get(k) for k in PARAM_SPACE} }")

    iteration = 1
    _history: list[dict] = []  # histórico de iterações para o dashboard
    _baseline_data = {"roi": best_roi, "pnl": best_pnl, "busted": best_busted}
    _best_data: dict | None = None
    
    while True:
        print(f"\n🔄 Iteração {iteration} — Perturbando parâmetros...")
        candidate_env = perturb_params(best_env)
        
        t0 = time.time()
        res = run_backtest_local(candidate_env)
        elapsed = time.time() - t0

        if not res:
            print(f"   ⚠️ Backtest falhou ou timeout. Pulando...")
            continue

        roi = res.get("roi_pct", -9999.0)
        busted = res.get("busted_days", 99)
        pnl = res.get("total_pnl", -9999.0)

        # Regra de Aceitação:
        # 1. Menor número de dias faliu (busted_days)
        # 2. Se empatar em busted_days, maior ROI/PnL
        is_better = False
        if busted < best_busted:
            is_better = True
        elif busted == best_busted:
            if roi > best_roi:
                is_better = True

        status_str = "❌ Pior ou Igual"
        if is_better:
            status_str = "🔥 MELHOR!"
            
        print(f"   Resultado [{elapsed:.1f}s]: ROI: {roi}% | PnL: ${pnl:.2f} | Quebras: {busted} → {status_str}")

        # Registra no histórico para o dashboard
        entry = {
            "iteration": iteration,
            "roi": roi,
            "pnl": pnl,
            "busted": busted,
            "elapsed_s": round(elapsed, 1),
            "is_best": is_better,
            "ts": time.time(),
        }
        _history.append(entry)

        if is_better:
            best_roi = roi
            best_busted = busted
            best_pnl = pnl
            best_env = candidate_env.copy()
            _best_data = {"iteration": iteration, "roi": roi, "pnl": pnl, "busted": busted}
            
            print(f"🎉 NOVO RECORDE ENCONTRADO!")
            print(f"   Hiperparâmetros: { {k: best_env.get(k) for k in PARAM_SPACE} }")
            
            # Salva o recorde localmente no .env
            print("   Gravando nova configuração no .env local...")
            save_env(best_env)
            
            # Deploy e restart no bot ao vivo!
            print("   Sincronizando com o servidor remoto e reiniciando o bot ao vivo...")
            try:
                subprocess.run(
                    ["./deploy.sh", "optimize: new best params found", "--restart"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=True
                )
                print("   ✅ Deploy e Restart do Bot concluído com sucesso!")
            except Exception as e:
                print(f"   ❌ Falha ao sincronizar com servidor: {e}")

        # Grava estado para o dashboard ler
        try:
            STATE_PATH.write_text(json.dumps({
                "running": True,
                "current_iteration": iteration,
                "baseline": _baseline_data,
                "best": _best_data,
                "iterations": _history[-100:],  # últimas 100
                "last_update": time.time(),
                "start_date": START_DATE,
                "end_date": END_DATE,
            }, indent=2), encoding="utf-8")
        except Exception:
            pass

        iteration += 1
        # Evita consumir 100% de CPU constante, dorme 1s entre iterações
        time.sleep(1.0)


if __name__ == "__main__":
    main()
