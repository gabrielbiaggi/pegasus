"""
deriv_auth.py — Módulo de autenticação Deriv unificada.

Suporta:
  1. PAT (Personal Access Token) — novo sistema Deriv (REST → OTP → WS)
  2. Token legado — sistema antigo WebSocket direto

Fluxo para PAT:
  1. GET https://api.derivws.com/trading/v1/options/accounts  (lista contas demo/real)
  2. POST /trading/v1/options/accounts/{account_id}/otp        (gera OTP + URL WS)
  3. Conecta via WebSocket no URL retornado

Fluxo legado (fallback):
  1. Conecta em wss://ws.derivws.com/websockets/v3?app_id=APP_ID
  2. Envia {"authorize": TOKEN}
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Optional

from logger import logger

# ─── Constantes ────────────────────────────────────────────────────────────────
DERIV_REST_BASE = "https://api.derivws.com"
DERIV_WS_LEGACY = "wss://ws.derivws.com/websockets/v3"
DERIV_WS_NEW_PUBLIC = "wss://api.derivws.com/trading/v1/options/ws/public"
DERIV_WS_NEW_DEMO = "wss://api.derivws.com/trading/v1/options/ws/demo"
DERIV_WS_NEW_REAL = "wss://api.derivws.com/trading/v1/options/ws/real"

# Arquivo de estado para monitorar expiração do PAT
TOKEN_STATE_FILE = os.path.join(
    os.path.dirname(__file__), "logs", "token_state.json"
)

# PAT expira em 90 dias por padrão
PAT_EXPIRY_DAYS = 90


# ─── Data classes ──────────────────────────────────────────────────────────────
@dataclass
class DerivAccount:
    account_id: str
    account_type: str  # "demo" | "real"
    currency: str
    balance: float
    status: str


@dataclass
class AuthResult:
    """Resultado de autenticação com dados da conta ativa."""
    ws_url: str            # URL WebSocket para conectar
    account_id: str        # ID da conta ativa
    account_type: str      # "demo" | "real"
    balance: float
    is_new_api: bool       # True = PAT + novo endpoint; False = token legado
    pat_token: str = ""    # PAT original (para renovação)
    legacy_token: str = "" # Token legado (se usando fallback)


# ─── Funções REST ──────────────────────────────────────────────────────────────
def _rest_request(
    method: str,
    path: str,
    token: str,
    app_id: str,
    body: dict | None = None,
    timeout: int = 15,
) -> dict:
    """Faz uma requisição HTTP para a nova REST API da Deriv."""
    url = f"{DERIV_REST_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    if app_id:
        req.add_header("Deriv-App-ID", app_id)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode(errors="replace")
        raise ValueError(f"REST {method} {path} → HTTP {e.code}: {body_text}") from e


def list_accounts(pat: str, app_id: str) -> list[DerivAccount]:
    """Lista contas disponíveis via novo sistema REST."""
    response = _rest_request("GET", "/trading/v1/options/accounts", pat, app_id)
    accounts = []
    for acc in response.get("data", []):
        accounts.append(
            DerivAccount(
                account_id=acc.get("account_id", ""),
                account_type=acc.get("account_type", "unknown"),
                currency=acc.get("currency", "USD"),
                balance=float(acc.get("balance", 0)),
                status=acc.get("status", "unknown"),
            )
        )
    return accounts


def get_otp_ws_url(pat: str, app_id: str, account_id: str) -> str:
    """Obtém URL WebSocket autenticada via OTP para a conta especificada."""
    path = f"/trading/v1/options/accounts/{account_id}/otp"
    response = _rest_request("POST", path, pat, app_id)
    url = response.get("data", {}).get("url", "")
    if not url:
        raise ValueError(f"OTP response sem URL: {response}")
    return url


# ─── Autenticação principal ────────────────────────────────────────────────────
def authenticate_pat(
    pat: str,
    app_id: str,
    account_mode: str = "demo",  # "demo" | "real" | "any"
) -> Optional[AuthResult]:
    """
    Autenticação via novo sistema PAT.
    Retorna AuthResult ou None se o PAT não funcionar neste sistema.
    """
    try:
        accounts = list_accounts(pat, app_id)
        if not accounts:
            logger.warning("PAT: nenhuma conta encontrada na nova API.")
            return None

        # Selecionar conta conforme account_mode
        active = None
        for acc in accounts:
            if acc.status != "active":
                continue
            if account_mode == "any":
                active = acc
                break
            if acc.account_type == account_mode:
                active = acc
                break

        if not active:
            # Fallback: primeiro ativo disponível
            actives = [a for a in accounts if a.status == "active"]
            if actives:
                active = actives[0]
                logger.warning(
                    "PAT: conta '%s' não encontrada. Usando %s (%s) como fallback.",
                    account_mode,
                    active.account_id,
                    active.account_type,
                )
            else:
                logger.error("PAT: nenhuma conta ativa disponível.")
                return None

        # Obter OTP e URL WS
        ws_url = get_otp_ws_url(pat, app_id, active.account_id)

        logger.info(
            "PAT autenticado: conta=%s tipo=%s balance=%.2f %s",
            active.account_id,
            active.account_type,
            active.balance,
            active.currency,
        )
        return AuthResult(
            ws_url=ws_url,
            account_id=active.account_id,
            account_type=active.account_type,
            balance=active.balance,
            is_new_api=True,
            pat_token=pat,
        )
    except Exception as e:
        logger.warning("PAT autenticação falhou: %s", e)
        return None


def authenticate_legacy(
    token: str,
    app_id: str,
    account_mode: str = "demo",
) -> AuthResult:
    """
    Autenticação via token legado (retorna URL WebSocket legada).
    A autorização real acontece via WebSocket após a conexão.
    """
    ws_url = f"{DERIV_WS_LEGACY}?app_id={app_id}"
    return AuthResult(
        ws_url=ws_url,
        account_id="",
        account_type=account_mode,
        balance=0.0,
        is_new_api=False,
        legacy_token=token,
    )


def get_auth(
    app_id: str,
    account_mode: str = "demo",
) -> AuthResult:
    """
    Ponto de entrada principal de autenticação.
    Prioridade:
      1. DERIV_PAT (novo sistema)
      2. DERIV_TOKEN + token legado (fallback)
    """
    # Tenta PAT primeiro
    pat = os.getenv("DERIV_PAT", "").strip()
    if pat:
        logger.info("Tentando autenticação via PAT (novo sistema Deriv)...")
        result = authenticate_pat(pat, app_id, account_mode)
        if result:
            _update_token_state(pat)
            return result
        logger.warning("PAT falhou. Tentando token legado como fallback...")

    # Fallback: token legado por modo
    if account_mode == "real":
        token = os.getenv("DERIV_REAL_TOKEN", os.getenv("DERIV_TOKEN", "")).strip()
    else:
        token = os.getenv("DERIV_DEMO_TOKEN", os.getenv("DERIV_TOKEN", "")).strip()

    if not token:
        raise ValueError(
            "Nenhum token disponível. Defina DERIV_PAT ou DERIV_TOKEN no .env"
        )

    logger.info("Usando autenticação via token legado (%s).", account_mode)
    return authenticate_legacy(token, app_id, account_mode)


# ─── Watchdog de expiração do token ───────────────────────────────────────────
def _update_token_state(pat: str) -> None:
    """Salva timestamp de quando o PAT foi usado pela última vez."""
    try:
        os.makedirs(os.path.dirname(TOKEN_STATE_FILE), exist_ok=True)
        state = {
            "pat_first_use": _get_existing_first_use(),
            "pat_last_auth": datetime.now(UTC).isoformat(),
            "pat_prefix": pat[:12] + "...",  # Não salva o token completo por segurança
            "expiry_days": PAT_EXPIRY_DAYS,
        }
        with open(TOKEN_STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.debug("Não foi possível salvar token_state.json: %s", e)


def _get_existing_first_use() -> str:
    """Retorna o timestamp de primeiro uso do PAT (para calcular expiração)."""
    try:
        if os.path.exists(TOKEN_STATE_FILE):
            with open(TOKEN_STATE_FILE) as f:
                state = json.load(f)
                if "pat_first_use" in state:
                    return state["pat_first_use"]
    except Exception:
        pass
    return datetime.now(UTC).isoformat()


def check_token_expiry() -> dict:
    """
    Verifica o estado de expiração do PAT.
    Retorna dict com:
      - status: "ok" | "warning" | "expired" | "unknown"
      - days_remaining: int
      - first_use: str (ISO)
      - message: str
    """
    if not os.path.exists(TOKEN_STATE_FILE):
        return {
            "status": "unknown",
            "days_remaining": -1,
            "first_use": "",
            "message": "Estado do token não encontrado. Token nunca usado via PAT.",
        }

    try:
        with open(TOKEN_STATE_FILE) as f:
            state = json.load(f)

        first_use_str = state.get("pat_first_use", "")
        if not first_use_str:
            return {"status": "unknown", "days_remaining": -1, "first_use": "", "message": "Data de primeiro uso não registrada."}

        first_use = datetime.fromisoformat(first_use_str.replace("Z", "+00:00"))
        expiry_date = first_use + timedelta(days=PAT_EXPIRY_DAYS)
        now = datetime.now(UTC)
        days_remaining = (expiry_date - now).days

        if days_remaining <= 0:
            status = "expired"
            message = f"⚠️ PAT EXPIRADO há {abs(days_remaining)} dia(s)! Gere um novo token em app.deriv.com."
        elif days_remaining <= 14:
            status = "warning"
            message = f"⚠️ PAT expira em {days_remaining} dia(s)! Renove em breve."
        else:
            status = "ok"
            message = f"PAT válido por mais {days_remaining} dia(s)."

        return {
            "status": status,
            "days_remaining": days_remaining,
            "first_use": first_use_str,
            "expiry_date": expiry_date.isoformat(),
            "message": message,
        }
    except Exception as e:
        return {"status": "unknown", "days_remaining": -1, "first_use": "", "message": f"Erro ao verificar token: {e}"}


def register_pat_first_use(pat: str) -> None:
    """
    Chama isto quando o PAT é configurado pela primeira vez para
    iniciar a contagem de expiração de 90 dias corretamente.
    """
    try:
        os.makedirs(os.path.dirname(TOKEN_STATE_FILE), exist_ok=True)
        existing = {}
        if os.path.exists(TOKEN_STATE_FILE):
            with open(TOKEN_STATE_FILE) as f:
                existing = json.load(f)

        # Só registra first_use se ainda não existe
        if "pat_first_use" not in existing:
            existing["pat_first_use"] = datetime.now(UTC).isoformat()

        existing["pat_prefix"] = pat[:12] + "..."
        existing["pat_last_registered"] = datetime.now(UTC).isoformat()
        existing["expiry_days"] = PAT_EXPIRY_DAYS

        with open(TOKEN_STATE_FILE, "w") as f:
            json.dump(existing, f, indent=2)
        print(f"PAT registrado. Expira em {PAT_EXPIRY_DAYS} dias.")
    except Exception as e:
        print(f"Erro ao registrar PAT: {e}")


if __name__ == "__main__":
    """Modo CLI: verifica status do token."""
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "register":
        pat = os.getenv("DERIV_PAT", "").strip()
        if not pat:
            print("DERIV_PAT não definido no .env")
            sys.exit(1)
        register_pat_first_use(pat)
    else:
        status = check_token_expiry()
        print(f"\n{'='*50}")
        print(f"Status do PAT Deriv:")
        print(f"  Status: {status['status'].upper()}")
        print(f"  Mensagem: {status['message']}")
        if status['days_remaining'] >= 0:
            print(f"  Dias restantes: {status['days_remaining']}")
        if status.get('expiry_date'):
            print(f"  Expira em: {status['expiry_date']}")
        print(f"{'='*50}\n")
        if status['status'] == 'expired':
            sys.exit(2)
        elif status['status'] == 'warning':
            sys.exit(1)
