#!/usr/bin/env python3
"""
token_watchdog.py — Watchdog de expiração do PAT Deriv.

Monitora o estado do PAT e:
  - Envia alertas quando faltam ≤14 dias para expirar
  - Registra avisos no log do sistema
  - Escreve arquivo de status para o dashboard

Pode ser executado via cron (diariamente) ou como serviço.

Uso:
  python3 token_watchdog.py         # verifica e alerta
  python3 token_watchdog.py daemon  # loop contínuo (verifica a cada 24h)
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

# Adiciona o diretório do projeto ao path
project_dir = Path(__file__).parent
sys.path.insert(0, str(project_dir))

# Carrega .env antes de importar módulos do projeto
try:
    from dotenv import load_dotenv
    load_dotenv(project_dir / ".env")
except ImportError:
    pass

from deriv_auth import check_token_expiry, register_pat_first_use

# ─── Configurações ─────────────────────────────────────────────────────────────
LOG_DIR = project_dir / "logs"
STATUS_FILE = LOG_DIR / "token_watchdog_status.json"
LOG_FILE = LOG_DIR / "token_watchdog.log"
CHECK_INTERVAL_HOURS = 24
WARNING_DAYS = 14   # Aviso com N dias de antecedência
CRITICAL_DAYS = 7   # Aviso crítico com N dias de antecedência


def _log(msg: str, level: str = "INFO") -> None:
    """Registra mensagem no log do watchdog."""
    ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] [{level}] {msg}"
    print(line)
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def write_status(status: dict) -> None:
    """Salva status em JSON para leitura pelo dashboard."""
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        status["checked_at"] = datetime.now(UTC).isoformat()
        with open(STATUS_FILE, "w") as f:
            json.dump(status, f, indent=2)
    except Exception as e:
        _log(f"Erro ao salvar status: {e}", "ERROR")


def send_alert(status: dict) -> None:
    """
    Envia alerta de expiração.
    
    Métodos disponíveis (configuráveis via .env):
      - LOG: sempre registra no log
      - FILE: cria arquivo de alerta
    
    Futuramente pode incluir: email, telegram, webhook, etc.
    """
    days = status.get("days_remaining", -1)
    msg = status.get("message", "")
    stat = status.get("status", "unknown")

    # Sempre loga
    level = "CRITICAL" if stat == "expired" else ("WARNING" if stat == "warning" else "INFO")
    _log(f"ALERTA DE TOKEN: {msg}", level)

    # Cria arquivo de alerta que pode ser detectado por scripts externos
    alert_file = LOG_DIR / "token_alert.json"
    try:
        with open(alert_file, "w") as f:
            json.dump({
                "alert": True,
                "status": stat,
                "days_remaining": days,
                "message": msg,
                "timestamp": datetime.now(UTC).isoformat(),
                "action_required": (
                    "Acesse app.deriv.com → Configurações → Tokens de API → Gere um novo PAT"
                    if stat in {"warning", "expired"} else ""
                ),
            }, f, indent=2)
    except Exception:
        pass


def check_and_alert() -> int:
    """
    Verifica o status do token e emite alertas se necessário.
    Retorna: 0=ok, 1=warning, 2=expired, 3=unknown
    """
    _log("Verificando status do PAT Deriv...")

    # Garante que o PAT está registrado se existir no .env
    pat = os.getenv("DERIV_PAT", "").strip()
    if pat:
        register_pat_first_use(pat)

    status = check_token_expiry()
    write_status(status)

    stat = status.get("status", "unknown")
    days = status.get("days_remaining", -1)

    if stat == "expired":
        _log(f"🚨 PAT EXPIRADO! {status.get('message', '')}", "CRITICAL")
        send_alert(status)
        return 2
    elif stat == "warning":
        _log(f"⚠️  PAT expirando em breve: {days} dia(s). {status.get('message', '')}", "WARNING")
        send_alert(status)
        return 1
    elif stat == "ok":
        _log(f"✅ PAT OK: {status.get('message', '')}")
        return 0
    else:
        _log(f"❓ Status desconhecido: {status.get('message', '')}", "WARNING")
        return 3


def run_daemon() -> None:
    """Executa o watchdog em modo daemon (loop contínuo)."""
    _log("Iniciando token watchdog em modo daemon...")
    _log(f"Intervalo de verificação: {CHECK_INTERVAL_HOURS}h")

    while True:
        try:
            exit_code = check_and_alert()
            if exit_code == 2:
                _log("Token expirado! Verificando a cada hora até renovação...", "CRITICAL")
                wait_seconds = 3600  # 1 hora
            else:
                wait_seconds = CHECK_INTERVAL_HOURS * 3600

            next_check = datetime.fromtimestamp(
                time.time() + wait_seconds, UTC
            ).strftime("%Y-%m-%d %H:%M UTC")
            _log(f"Próxima verificação: {next_check}")
            time.sleep(wait_seconds)

        except KeyboardInterrupt:
            _log("Watchdog encerrado pelo usuário.")
            break
        except Exception as e:
            _log(f"Erro no watchdog: {e}", "ERROR")
            time.sleep(3600)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "daemon":
        run_daemon()
    else:
        exit_code = check_and_alert()
        sys.exit(exit_code)
