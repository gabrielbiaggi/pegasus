#!/usr/bin/env python3
"""
setup_deriv_mcp.py — Configura o MCP da Deriv no ambiente local.

O MCP (Model Context Protocol) da Deriv permite que agentes de IA
interajam com a API da Deriv diretamente.

URL do MCP Server: https://mcp-api.deriv.com/mcp

Uso:
  python3 setup_deriv_mcp.py check    — verifica se MCP está acessível
  python3 setup_deriv_mcp.py config   — mostra configuração para Claude/Cursor
  python3 setup_deriv_mcp.py test     — testa acesso autenticado via PAT
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

project_dir = Path(__file__).parent
sys.path.insert(0, str(project_dir))

try:
    from dotenv import load_dotenv
    load_dotenv(project_dir / ".env")
except ImportError:
    pass

DERIV_MCP_URL = "https://mcp-api.deriv.com/mcp"
DERIV_REST_BASE = "https://api.derivws.com"


def check_mcp_endpoint() -> dict:
    """Verifica se o endpoint MCP da Deriv está acessível."""
    try:
        # MCP usa protocolo SSE/JSON-RPC - tenta GET inicial
        req = urllib.request.Request(DERIV_MCP_URL, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json, text/event-stream")
        
        # Requisição de initialize do MCP
        body = json.dumps({
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "pegasus-bot", "version": "1.0"}
            },
            "id": 1
        }).encode()
        req.data = body
        
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read().decode()
            return {"status": "ok", "response": data[:500]}
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        return {"status": "http_error", "code": e.code, "body": body[:300]}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def check_rest_api(pat: str, app_id: str) -> dict:
    """Verifica autenticação na nova REST API."""
    try:
        req = urllib.request.Request(f"{DERIV_REST_BASE}/trading/v1/options/accounts")
        req.add_header("Authorization", f"Bearer {pat}")
        if app_id:
            req.add_header("Deriv-App-ID", app_id)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            accounts = data.get("data", [])
            return {
                "status": "ok",
                "accounts": len(accounts),
                "types": [a.get("account_type") for a in accounts]
            }
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        return {"status": "error", "code": e.code, "message": body[:200]}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def print_mcp_config():
    """Mostra como configurar o MCP da Deriv em diferentes ferramentas."""
    config = {
        "mcpServers": {
            "deriv-api": {
                "url": DERIV_MCP_URL,
                "description": "Deriv API MCP Server — trading, market data, account management"
            }
        }
    }
    
    print("\n" + "="*60)
    print("CONFIGURAÇÃO DO MCP DERIV")
    print("="*60)
    print("\nPara Claude Desktop (claude_desktop_config.json):")
    print(json.dumps(config, indent=2))
    
    print("\nPara Cursor (settings.json):")
    cursor_config = {
        "mcpServers": {
            "deriv-api": {
                "url": DERIV_MCP_URL
            }
        }
    }
    print(json.dumps(cursor_config, indent=2))
    
    print("\nURL MCP Server:", DERIV_MCP_URL)
    print("Documentação:", "https://developers.deriv.com/llms.txt")
    print("AI Hub:", "https://developers.deriv.com/ai-hub/")
    print("="*60)


def run_check():
    """Executa verificação completa do sistema."""
    pat = os.getenv("DERIV_PAT", "").strip()
    app_id = os.getenv("DERIV_APP_ID", "1089").strip()
    
    print("\n" + "="*60)
    print("DIAGNÓSTICO DERIV API")
    print("="*60)
    
    # 1. Verifica MCP
    print("\n1. Verificando MCP Server...")
    mcp_status = check_mcp_endpoint()
    print(f"   Status: {mcp_status.get('status')}")
    if mcp_status.get("code"):
        print(f"   HTTP: {mcp_status['code']}")
    print(f"   Detalhes: {mcp_status.get('body') or mcp_status.get('response') or mcp_status.get('error', '')[:100]}")
    
    # 2. Verifica REST API com PAT
    print("\n2. Verificando REST API com PAT...")
    if not pat:
        print("   ⚠️  DERIV_PAT não configurado no .env")
    else:
        rest_status = check_rest_api(pat, app_id)
        if rest_status["status"] == "ok":
            print(f"   ✅ Autenticado! Contas: {rest_status['accounts']} ({rest_status['types']})")
        else:
            print(f"   ❌ Falha: HTTP {rest_status.get('code', '?')} — {rest_status.get('message', '')}")
            if "App-ID" in str(rest_status.get("message", "")):
                print("   ⚠️  PROBLEMA: Precisa de App ID registrado em developers.deriv.com")
                print("   SOLUÇÃO:")
                print("     1. Acesse https://developers.deriv.com")
                print("     2. Crie um aplicativo para obter seu App ID")
                print("     3. Atualize DERIV_APP_ID no .env com o novo ID")
            elif "expired" in str(rest_status.get("message", "")).lower():
                print("   ⚠️  PROBLEMA: PAT inválido ou expirado")
                print("   SOLUÇÃO: Gere novo PAT em app.deriv.com → Configurações → API Token")
    
    # 3. Status do token
    print("\n3. Status do PAT...")
    try:
        from deriv_auth import check_token_expiry
        status = check_token_expiry()
        print(f"   {status.get('message', 'Desconhecido')}")
    except Exception as e:
        print(f"   Não foi possível verificar: {e}")
    
    # 4. Ação recomendada
    print("\n" + "="*60)
    print("AÇÃO RECOMENDADA:")
    print("-"*60)
    print("A Deriv migrou para um novo sistema de API em 2026.")
    print("Para o bot funcionar com o novo sistema:")
    print()
    print("  1. Acesse: https://developers.deriv.com")
    print("  2. Faça login com sua conta Deriv")
    print("  3. Crie um aplicativo para obter o App ID")
    print("  4. Atualize DERIV_APP_ID no .env com o novo ID")
    print()
    print("Enquanto isso, o sistema usa tokens legados como fallback.")
    print("Para reativar tokens legados: acesse app.deriv.com")
    print("="*60 + "\n")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    
    if cmd == "config":
        print_mcp_config()
    elif cmd == "check":
        run_check()
    else:
        print(f"Comando desconhecido: {cmd}")
        print("Uso: python3 setup_deriv_mcp.py [check|config|test]")
