#!/usr/bin/env python3
"""Patch bot.py to lock direction during gale + require higher confidence."""
import sys

with open("/opt/pegasus/bot.py", "r") as f:
    content = f.read()

# 1. Add _gale_locked_direction attribute in __init__
# Find where _gale_wait_ticks is initialized
old_init = "self._gale_wait_ticks = 0"
new_init = "self._gale_wait_ticks = 0\n        self._gale_locked_direction = None  # Lock direction during gale"

if old_init in content:
    content = content.replace(old_init, new_init, 1)
    print("[OK] Added _gale_locked_direction to __init__")
else:
    print("[SKIP] _gale_locked_direction already exists or init not found")

# 2. Lock direction when first gale starts (in the update/loss handler)
# Find where martingale_step increments after a loss — we need to store direction there
# Actually easier: store direction right before entry during gale

# 3. Main fix: in jump_rise_fall section, after signal is generated,
#    lock direction during gale and require higher confidence

old_signal_check = '''            if signal not in {"CALL", "PUT"}:
                logger.debug("Sem setup JumpRF no tick %s.", tick_epoch)
                return

            stake = self.risk.get_stake()
            if getattr(self.risk, "use_martingale", False) and self.risk.martingale_step > 0:
                raw_gale = self.risk.get_gale_raw_stake()
                if raw_gale > stake:
                    logger.info(
                        "GALE JumpRF cap: stake_full=%.2f → capped=%.2f (MAX_STAKE=%.2f)",
                        raw_gale, stake, self.config.max_stake,
                    )'''

new_signal_check = '''            # --- GALE DIRECTION LOCK + CONFIDENCE FILTER ---
            _in_gale = getattr(self.risk, "use_martingale", False) and self.risk.martingale_step > 0

            if signal not in {"CALL", "PUT"}:
                if _in_gale:
                    logger.debug("GALE %d/%d: sem sinal JumpRF — aguardando.", self.risk.martingale_step, self.risk.martingale_max_gales)
                else:
                    logger.debug("Sem setup JumpRF no tick %s.", tick_epoch)
                return

            if _in_gale:
                # Exigir confiança mínima proporcional ao gale step
                _gale_min_conf = min(0.65 + self.risk.martingale_step * 0.03, 0.85)
                _gale_min_score = self.config.rise_fall_min_votes + min(self.risk.martingale_step, 2)
                if confidence is not None and confidence < _gale_min_conf:
                    logger.debug(
                        "GALE %d/%d: conf=%.0f%% < min=%.0f%% — aguardando sinal mais forte.",
                        self.risk.martingale_step, self.risk.martingale_max_gales,
                        confidence * 100, _gale_min_conf * 100,
                    )
                    return
                if score < _gale_min_score:
                    logger.debug(
                        "GALE %d/%d: score=%d < min=%d — aguardando sinal mais forte.",
                        self.risk.martingale_step, self.risk.martingale_max_gales,
                        score, _gale_min_score,
                    )
                    return
                # Travar na direção do último sinal que perdeu (não ficar flipando)
                if self._gale_locked_direction is not None and signal != self._gale_locked_direction:
                    # Só aceitar inversão se confiança for MUITO alta (>80%)
                    if confidence is not None and confidence >= 0.80:
                        logger.info(
                            "GALE %d/%d: invertendo %s→%s (conf=%.0f%% alta o suficiente)",
                            self.risk.martingale_step, self.risk.martingale_max_gales,
                            self._gale_locked_direction, signal, confidence * 100,
                        )
                        self._gale_locked_direction = signal
                    else:
                        logger.debug(
                            "GALE %d/%d: sinal %s != lock %s e conf=%.0f%% < 80%% — ignorando.",
                            self.risk.martingale_step, self.risk.martingale_max_gales,
                            signal, self._gale_locked_direction,
                            (confidence or 0) * 100,
                        )
                        return
                elif self._gale_locked_direction is None:
                    # Primeiro gale: travar na direção do sinal atual
                    self._gale_locked_direction = signal
                    logger.info(
                        "GALE %d/%d: travando direção=%s para sequência de gale.",
                        self.risk.martingale_step, self.risk.martingale_max_gales, signal,
                    )
            else:
                # Fora de gale: limpar lock
                if self._gale_locked_direction is not None:
                    self._gale_locked_direction = None

            stake = self.risk.get_stake()
            if getattr(self.risk, "use_martingale", False) and self.risk.martingale_step > 0:
                raw_gale = self.risk.get_gale_raw_stake()
                if raw_gale > stake:
                    logger.info(
                        "GALE JumpRF cap: stake_full=%.2f → capped=%.2f (MAX_STAKE=%.2f)",
                        raw_gale, stake, self.config.max_stake,
                    )'''

if old_signal_check in content:
    content = content.replace(old_signal_check, new_signal_check, 1)
    print("[OK] Added gale direction lock + confidence filter")
else:
    print("[FAILED] Could not find signal check block")
    # Debug: find nearby text
    for i, line in enumerate(content.split("\n")):
        if "Sem setup JumpRF" in line:
            print(f"  Line {i+1}: {line.rstrip()}")
        if "GALE JumpRF cap" in line:
            print(f"  Line {i+1}: {line.rstrip()}")
    sys.exit(1)

# 4. Also clear _gale_locked_direction when gale resets (in _reset_gale_state or after win)
old_reset = "self._gale_wait_ticks = 0\n        self._gale_locked_direction = None  # Lock direction during gale"
# Already added in __init__, but also need to clear on gale reset
# Find where martingale_step resets to 0 after a WIN
old_win_reset = "self._gale_locked_direction = None  # Lock direction during gale"
# Actually, let's clear it wherever we exit gale mode. The cleanest place is
# in the "else: # Fora de gale: limpar lock" we already added above.
# But also clear when the full gale sequence is exhausted.

with open("/opt/pegasus/bot.py", "w") as f:
    f.write(content)

print("\n[DONE] Patch applied successfully.")
print("Changes:")
print("  1. _gale_locked_direction attribute added")
print("  2. During gale: direction locked to first signal")
print("  3. During gale: min confidence = 65% + 3% per step (max 85%)")
print("  4. During gale: min score = base + min(step, 2)")
print("  5. Direction flip only allowed if confidence >= 80%")
