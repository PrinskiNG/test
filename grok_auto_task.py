# -*- coding: utf-8 -*-
"""
grok_auto_task.py  v2.0
Architecture: Grok (pure search, per-account) + Kimi (analyse & summarise)

Phase 1 – Tiered scan:
  All 100 accounts searched individually (from:account, limit=10, mode=Latest).
  Collect 3 newest posts + 1 metadata row per account.
  Auto-classify accounts into S / A / B / inactive.

Phase 2 – Differential collection + report:
  S-tier (~5-8):  10 posts + x_thread_fetch for likes >5000
  A-tier (~20-25): 5 posts, qt field for retweets
  B-tier (rest):   reuse Phase 1 data (3 posts)
  Kimi (moonshot-v1-32k) generates the daily report.
  Push to Feishu + WeChat.
"""

import os
import re
import json
import time
import base64
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright

# ── Environment variables ────────────────────────────────────────────────────
JIJYUN_WEBHOOK_URL  = os.getenv("JIJYUN_WEBHOOK_URL", "")
SF_API_KEY          = os.getenv("SF_API_KEY", "")
KIMI_API_KEY        = os.getenv("KIMI_API_KEY", "")
OPENROUTER_API_KEY  = os.getenv("OPENROUTER_API_KEY", "")
GROK_COOKIES_JSON  = os.getenv("SUPER_GROK_COOKIES", "")   # unified all-caps
PAT_FOR_SECRETS    = os.getenv("PAT_FOR_SECRETS", "")
GITHUB_REPOSITORY  = os.getenv("GITHUB_REPOSITORY", "")

# ── Global timeout tracking ──────────────────────────────────────────────────
_START_TIME      = time.time()
PHASE1_DEADLINE  = 20 * 60   # 20 min → trigger degradation (skip remaining batches)
GLOBAL_DEADLINE  = 45 * 60   # 45 min → stop Grok, hand off to Kimi

# ── 100 accounts – ordered high-value first so degradation truncates B-tier ──
ALL_ACCOUNTS = [
    # ── Tier-1 giants (likely S / A after classification) ──────────────────
    "elonmusk", "sama", "karpathy", "demishassabis", "darioamodei",
    "OpenAI", "AnthropicAI", "GoogleDeepMind", "xAI", "AIatMeta",
    "GoogleAI", "MSFTResearch", "IlyaSutskever", "gregbrockman",
    "GaryMarcus", "rowancheung", "clmcleod", "bindureddy",
    # ── Chinese KOL / VC / Media (likely A / B) ────────────────────────────
    "dotey", "oran_ge", "vista8", "imxiaohu", "Sxsyer",
    "K_O_D_A_D_A", "tualatrix", "linyunqiu", "garywong", "web3buidl",
    "AI_Era", "AIGC_News", "jiangjiang", "hw_star", "mranti", "nishuang",
    "a16z", "ycombinator", "lightspeedvp", "sequoia", "foundersfund",
    "eladgil", "pmarca", "bchesky", "chamath", "paulg",
    "TheInformation", "TechCrunch", "verge", "WIRED", "Scobleizer", "bentossell",
    # ── Open source + infrastructure ──────────────────────────────────────
    "HuggingFace", "MistralAI", "Perplexity_AI", "GroqInc", "Cohere",
    "TogetherCompute", "runwayml", "Midjourney", "StabilityAI", "Scale_AI",
    "CerebrasSystems", "tenstorrent", "weights_biases", "langchainai", "llama_index",
    "supabase", "vllm_project", "huggingface_hub",
    # ── Hardware / spatial computing ──────────────────────────────────────
    "nvidia", "AMD", "Intel", "SKhynix", "tsmc",
    "magicleap", "NathieVR", "PalmerLuckey", "ID_AA_Carmack", "boz",
    "rabovitz", "htcvive", "XREAL_Global", "RayBan", "MetaQuestVR", "PatrickMoorhead",
    # ── Researchers / niche – placed last for graceful degradation ─────────
    "jeffdean", "chrmanning", "hardmaru", "goodfellow_ian", "feifeili",
    "_akhaliq", "promptengineer", "AI_News_Tech", "siliconvalley", "aithread",
    "aibreakdown", "aiexplained", "aipubcast", "lexfridman", "hubermanlab", "swyx",
]


# ════════════════════════════════════════════════════════════════════════════
# Feishu multi-webhook
# ════════════════════════════════════════════════════════════════════════════
def get_feishu_webhooks() -> list:
    urls = []
    for suffix in ["", "_1", "_2", "_3"]:
        url = os.getenv(f"FEISHU_WEBHOOK_URL{suffix}", "")
        if url:
            urls.append(url)
    return urls


# ════════════════════════════════════════════════════════════════════════════
# Date utilities
# ════════════════════════════════════════════════════════════════════════════
def get_dates() -> tuple:
    tz = timezone(timedelta(hours=8))
    today = datetime.now(tz)
    yesterday = today - timedelta(days=1)
    return today.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")


# ════════════════════════════════════════════════════════════════════════════
# Session management: load + auto-renew
# ════════════════════════════════════════════════════════════════════════════
def prepare_session_file() -> bool:
    """
    Write SUPER_GROK_COOKIES to local session_state.json.
    Returns True  = Playwright storage-state format (post-renewal)
    Returns False = raw Cookie-Editor array (first-time manual import)
    """
    if not GROK_COOKIES_JSON:
        print("[Session] ⚠️ SUPER_GROK_COOKIES not configured", flush=True)
        return False
    try:
        data = json.loads(GROK_COOKIES_JSON)
        if isinstance(data, dict) and "cookies" in data:
            with open("session_state.json", "w", encoding="utf-8") as f:
                json.dump(data, f)
            print("[Session] ✅ Playwright storage-state format (renewed)", flush=True)
            return True
        else:
            print(f"[Session] ✅ Cookie-Editor array format ({len(data)} entries)", flush=True)
            return False
    except Exception as e:
        print(f"[Session] ❌ Parse failed: {e}", flush=True)
        return False


def load_raw_cookies(context):
    """Cookie-Editor array → inject into Playwright context (first-time use)."""
    try:
        cookies = json.loads(GROK_COOKIES_JSON)
        formatted = []
        for c in cookies:
            cookie = {
                "name":   c.get("name", ""),
                "value":  c.get("value", ""),
                "domain": c.get("domain", ".grok.com"),
                "path":   c.get("path", "/"),
            }
            if "httpOnly" in c: cookie["httpOnly"] = c["httpOnly"]
            if "secure"   in c: cookie["secure"]   = c["secure"]
            ss = c.get("sameSite", "")
            if ss in ("Strict", "Lax", "None"):
                cookie["sameSite"] = ss
            formatted.append(cookie)
        context.add_cookies(formatted)
        print(f"[Session] ✅ Injected {len(formatted)} cookies", flush=True)
    except Exception as e:
        print(f"[Session] ❌ Cookie injection failed: {e}", flush=True)


def save_and_renew_session(context):
    """
    Save current Playwright storage-state locally, then write back to
    the SUPER_GROK_COOKIES GitHub secret via API (session renewal).
    """
    try:
        context.storage_state(path="session_state.json")
        print("[Session] ✅ Storage state saved locally", flush=True)
    except Exception as e:
        print(f"[Session] ❌ Save storage state failed: {e}", flush=True)
        return

    if not PAT_FOR_SECRETS or not GITHUB_REPOSITORY:
        print("[Session] ⚠️ PAT_FOR_SECRETS or GITHUB_REPOSITORY not configured, skip renewal",
              flush=True)
        return

    try:
        from nacl import encoding, public as nacl_public

        with open("session_state.json", "r", encoding="utf-8") as f:
            state_str = f.read()

        headers = {
            "Authorization": f"token {PAT_FOR_SECRETS}",
            "Accept": "application/vnd.github.v3+json",
        }

        key_resp = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPOSITORY}/actions/secrets/public-key",
            headers=headers, timeout=30,
        )
        key_resp.raise_for_status()
        key_data = key_resp.json()

        pub_key = nacl_public.PublicKey(key_data["key"].encode(), encoding.Base64Encoder())
        sealed  = nacl_public.SealedBox(pub_key).encrypt(state_str.encode())
        enc_b64 = base64.b64encode(sealed).decode()

        put_resp = requests.put(
            f"https://api.github.com/repos/{GITHUB_REPOSITORY}/actions/secrets/SUPER_GROK_COOKIES",
            headers=headers,
            json={"encrypted_value": enc_b64, "key_id": key_data["key_id"]},
            timeout=30,
        )
        put_resp.raise_for_status()
        print("[Session] ✅ GitHub Secret SUPER_GROK_COOKIES auto-renewed", flush=True)

    except ImportError:
        print("[Session] ⚠️ PyNaCl not installed, skip renewal", flush=True)
    except Exception as e:
        print(f"[Session] ❌ Secret renewal failed: {e}", flush=True)


def check_cookie_expiry():
    if not GROK_COOKIES_JSON:
        return
    try:
        data = json.loads(GROK_COOKIES_JSON)
        if not isinstance(data, list):
            return
        for c in data:
            if c.get("name") == "sso" and c.get("expirationDate"):
                exp = datetime.fromtimestamp(c["expirationDate"], tz=timezone.utc)
                days_left = (exp - datetime.now(timezone.utc)).days
                if days_left <= 5:
                    msg = (f"⚠️ Grok Cookie expires in {days_left} days, "
                           f"please update SUPER_GROK_COOKIES!")
                    print(f"[Cookie] {msg}", flush=True)
                    for url in get_feishu_webhooks():
                        try:
                            requests.post(url,
                                          json={"msg_type": "text", "content": {"text": msg}},
                                          timeout=15)
                        except Exception:
                            pass
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════
# Model selection: enable Grok 4.20 Beta Toggle
# ════════════════════════════════════════════════════════════════════════════
def enable_grok4_beta(page):
    print("\n[Model] Enabling Grok 4.20 Beta Toggle...", flush=True)
    try:
        model_btn = page.wait_for_selector(
            "button:has-text('快速模式'), button:has-text('Fast'), "
            "button:has-text('自动模式'), button:has-text('Auto')",
            timeout=15000,
        )
        model_btn.click()
        time.sleep(1)

        toggle = page.wait_for_selector(
            "button[role='switch'], input[type='checkbox']", timeout=8000,
        )
        is_on = page.evaluate("""() => {
            const sw = document.querySelector("button[role='switch']");
            if (sw) return sw.getAttribute('aria-checked') === 'true'
                        || sw.getAttribute('data-state') === 'checked';
            const cb = document.querySelector("input[type='checkbox']");
            return cb ? cb.checked : false;
        }""")
        if not is_on:
            toggle.click()
            print("[Model] ✅ Toggle enabled", flush=True)
            time.sleep(1)
        else:
            print("[Model] ✅ Already enabled", flush=True)
        page.keyboard.press("Escape")
        time.sleep(0.5)
    except Exception as e:
        print(f"[Model] ⚠️ Failed, using current model: {e}", flush=True)


# ════════════════════════════════════════════════════════════════════════════
# Send prompt
# ════════════════════════════════════════════════════════════════════════════
def send_prompt(page, prompt_text, label, screenshot_prefix):
    print(f"\n[{label}] Filling prompt ({len(prompt_text)} chars)...", flush=True)
    page.wait_for_selector("div[contenteditable='true'], textarea", timeout=30000)

    ok = page.evaluate("""(text) => {
        const el = document.querySelector("div[contenteditable='true']")
                || document.querySelector("textarea");
        if (!el) return false;
        el.focus();
        document.execCommand('selectAll', false, null);
        document.execCommand('delete', false, null);
        document.execCommand('insertText', false, text);
        return true;
    }""", prompt_text)

    if not ok:
        inp = page.query_selector("div[contenteditable='true'], textarea")
        if inp:
            inp.click()
            page.keyboard.press("Control+a")
            page.keyboard.press("Backspace")
            for i in range(0, len(prompt_text), 500):
                page.keyboard.type(prompt_text[i:i+500])
                time.sleep(0.2)

    time.sleep(1.5)

    try:
        inp = page.query_selector("div[contenteditable='true'], textarea")
        if inp:
            inp.click()
            time.sleep(0.5)
    except Exception:
        pass

    clicked = False
    try:
        send_btn = page.wait_for_selector(
            "button[aria-label='Submit']:not([disabled]), "
            "button[aria-label='Send message']:not([disabled]), "
            "button[type='submit']:not([disabled])",
            timeout=30000, state="visible",
        )
        send_btn.click()
        clicked = True
    except Exception as e:
        print(f"[{label}] ⚠️ Normal click failed ({e}), trying JS...", flush=True)

    if not clicked:
        result = page.evaluate("""() => {
            const btn = document.querySelector("button[type='submit']")
                     || document.querySelector("button[aria-label='Submit']")
                     || document.querySelector("button[aria-label='Send message']");
            if (btn) { btn.click(); return true; }
            return false;
        }""")
        if result:
            print(f"[{label}] ✅ JS fallback click succeeded", flush=True)
        else:
            raise RuntimeError(f"[{label}] ❌ Submit button not found, aborting")

    print(f"[{label}] ✅ Sent", flush=True)
    time.sleep(5)


# ════════════════════════════════════════════════════════════════════════════
# Wait for Grok to finish generating
# ════════════════════════════════════════════════════════════════════════════
def _get_last_msg(page):
    return page.evaluate("""() => {
        const msgs = document.querySelectorAll(
            '[data-testid="message"], .message-bubble, .response-content'
        );
        return msgs.length ? msgs[msgs.length - 1].innerText : "";
    }""")


def wait_and_extract(page, label, screenshot_prefix,
                     interval=3, stable_rounds=4, max_wait=120,
                     extend_if_growing=False, min_len=80):
    print(f"[{label}] Waiting for reply (max {max_wait}s, min len {min_len})...", flush=True)
    last_len  = -1
    stable    = 0
    elapsed   = 0
    last_text = ""

    while elapsed < max_wait:
        time.sleep(interval)
        elapsed += interval
        try:
            text = _get_last_msg(page)
        except Exception as e:
            print(f"[{label}] ⚠️ Page error: {e}", flush=True)
            return last_text.strip()
        last_text = text
        cur_len = len(text.strip())
        print(f"  {elapsed}s | chars: {cur_len}", flush=True)

        if cur_len == last_len and cur_len >= min_len:
            stable += 1
            if stable >= stable_rounds:
                print(f"[{label}] ✅ Done ({cur_len} chars)", flush=True)
                return text.strip()
        else:
            stable   = 0
            last_len = cur_len

    if extend_if_growing:
        print(f"[{label}] ⏳ Extending wait (up to 300s)...", flush=True)
        prev_len = last_len; prev_text = last_text; ext = 0
        while ext < 300:
            time.sleep(5); ext += 5
            try:
                text = _get_last_msg(page)
            except Exception:
                return prev_text.strip()
            cur_len = len(text.strip())
            print(f"  +{ext}s | chars: {cur_len}", flush=True)
            if cur_len == prev_len:
                return text.strip()
            prev_len = cur_len; prev_text = text
        try:
            return _get_last_msg(page).strip()
        except Exception:
            return prev_text.strip()
    else:
        try:
            return _get_last_msg(page).strip()
        except Exception:
            return last_text.strip()


# ════════════════════════════════════════════════════════════════════════════
# JSON Lines parser (tolerates non-JSON lines in Grok output)
# ════════════════════════════════════════════════════════════════════════════
def parse_jsonlines(text: str) -> list:
    """Return list of dicts parsed from valid JSON Lines in text."""
    results = []
    for line in text.splitlines():
        line = line.strip()
        if not line or not line.startswith('{') or not line.endswith('}'):
            continue
        try:
            results.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return results


# ════════════════════════════════════════════════════════════════════════════
# Phase 1 prompt: metadata scan (all accounts, B-level strategy)
# ════════════════════════════════════════════════════════════════════════════
def build_phase1_prompt(accounts: list) -> str:
    """
    Build a Phase-1 prompt for up to 24 accounts (8 rounds × 3 parallel).
    Search query: from:account (no keywords), limit=10, mode=Latest.
    Output: newest 3 posts + 1 metadata row per account.
    """
    rounds = [accounts[i:i+3] for i in range(0, len(accounts), 3)]
    rounds_text = "\n".join(
        f"第{i+1}轮：{' | '.join(r)}"
        for i, r in enumerate(rounds)
    )
    return (
        "你是X平台数据采集工具。执行以下账号搜索，输出纯JSON Lines格式。\n\n"
        "【搜索规则】\n"
        "1. 每个账号单独调用 x_keyword_search：query=from:账号名，mode=Latest，limit=10\n"
        "2. 按轮次并行执行（每轮同时搜索3个账号）\n"
        "3. 不加任何关键词，不加 since 时间参数\n"
        "4. 每个账号输出：最新3条帖子 + 1行元数据\n\n"
        f"【账号列表（共{len(accounts)}个，按轮次执行）】\n"
        f"{rounds_text}\n\n"
        "【输出格式（只输出JSON Lines，严禁输出任何其他文字）】\n"
        '帖子行：{"a":"账号名","l":点赞数,"t":"MMDD","s":"英文原文摘要50词内","tag":"raw"}\n'
        '元数据行：{"a":"账号名","type":"meta","total":返回总条数,"max_l":最高点赞数,"latest":"MMDD"}\n'
        '不活跃账号：{"a":"账号名","type":"meta","total":0,"max_l":0,"latest":"NA"}\n\n'
        "【严格限制】\n"
        "- 账号名不带@符号，与from:查询中的账号名完全一致\n"
        "- t字段格式MMDD（如0309=3月9日）\n"
        "- 每个账号先输出帖子行（最多3行），再输出1行元数据行\n"
        "- 不翻译、不解释、不总结、不过滤\n"
        "- 第一行到最后一行全部是JSON"
    )


# ════════════════════════════════════════════════════════════════════════════
# Phase 2 prompts: tier-specific collection
# ════════════════════════════════════════════════════════════════════════════
def build_phase2_s_prompt(accounts: list) -> str:
    """S-tier: 10 posts + x_thread_fetch for likes >5000, qt field for quotes."""
    rounds = [accounts[i:i+3] for i in range(0, len(accounts), 3)]
    rounds_text = "\n".join(
        f"第{i+1}轮：{' | '.join(r)}"
        for i, r in enumerate(rounds)
    )
    return (
        "你是X平台数据采集工具。执行以下S级账号深度采集，输出纯JSON Lines格式。\n\n"
        "【S级规则】\n"
        "1. 每个账号调用 x_keyword_search：query=from:账号名，mode=Latest，limit=10\n"
        "2. 输出全部10条帖子（不截断）\n"
        "3. 对点赞>5000的帖子，额外调用 x_thread_fetch 获取完整线程（每账号最多5次）\n"
        "4. 转发/引用帖（RT或QT）：在qt字段记录被引用原帖的作者和内容摘要\n"
        "5. 每轮并行3个账号\n\n"
        f"【S级账号（共{len(accounts)}个）】\n"
        f"{rounds_text}\n\n"
        "【输出格式（只输出JSON Lines）】\n"
        '普通帖：{"a":"账号名","l":点赞数,"t":"MMDD","s":"英文摘要50词内","tag":"raw"}\n'
        '引用帖：{"a":"账号名","l":点赞数,"t":"MMDD","s":"评论内容摘要","qt":"@原作者: 原帖摘要","tag":"raw"}\n'
        '线程帖：{"a":"账号名","l":点赞数,"t":"MMDD","s":"原文摘要","tag":"raw",'
        '"replies":[{"from":"回复者账号","text":"回复内容","l":点赞数}]}\n\n'
        "【严格限制】\n"
        "- 账号名不带@，与from:查询完全一致\n"
        "- 不翻译、不解释，只输出JSON\n"
        "- s字段用英文"
    )


def build_phase2_a_prompt(accounts: list) -> str:
    """A-tier: 5 newest posts, qt field for retweets/quotes."""
    rounds = [accounts[i:i+3] for i in range(0, len(accounts), 3)]
    rounds_text = "\n".join(
        f"第{i+1}轮：{' | '.join(r)}"
        for i, r in enumerate(rounds)
    )
    return (
        "你是X平台数据采集工具。执行以下A级账号采集，输出纯JSON Lines格式。\n\n"
        "【A级规则】\n"
        "1. 每个账号调用 x_keyword_search：query=from:账号名，mode=Latest，limit=10\n"
        "2. 只输出最新5条帖子\n"
        "3. 转发/引用帖（RT或QT）：在qt字段记录被引用原帖的作者和内容摘要\n"
        "4. 每轮并行3个账号\n\n"
        f"【A级账号（共{len(accounts)}个）】\n"
        f"{rounds_text}\n\n"
        "【输出格式（只输出JSON Lines）】\n"
        '普通帖：{"a":"账号名","l":点赞数,"t":"MMDD","s":"英文摘要50词内","tag":"raw"}\n'
        '引用帖：{"a":"账号名","l":点赞数,"t":"MMDD","s":"评论内容摘要","qt":"@原作者: 原帖摘要","tag":"raw"}\n\n'
        "【严格限制】\n"
        "- 账号名不带@，与from:查询完全一致\n"
        "- 不翻译、不解释，只输出JSON\n"
        "- s字段用英文"
    )


# ════════════════════════════════════════════════════════════════════════════
# Account classification
# ════════════════════════════════════════════════════════════════════════════
def classify_accounts(meta_results: dict) -> dict:
    """
    Classify accounts based on Phase-1 metadata.
    Returns {account: tier} where tier ∈ {S, A, B, inactive}.

    S:        max_likes > 10000 AND posted within last 7 days
    A:        max_likes > 1000  AND posted within last 14 days
    B:        other active accounts
    inactive: 0 posts or no posts in last 30 days
    """
    tz = timezone(timedelta(hours=8))
    today = datetime.now(tz)
    classification = {}

    for account, meta in meta_results.items():
        total  = meta.get("total", 0)
        max_l  = meta.get("max_l", 0)
        latest = meta.get("latest", "NA")

        if total == 0 or latest == "NA":
            classification[account] = "inactive"
            continue

        # Parse MMDD → days since latest post
        try:
            mm = int(latest[:2])
            dd = int(latest[2:])
            latest_date = today.replace(month=mm, day=dd)
            # If the parsed date is in the future, it belongs to the previous year
            if latest_date > today:
                latest_date = latest_date.replace(year=today.year - 1)
            days_since = (today - latest_date).days
        except (ValueError, TypeError):
            days_since = 999

        if days_since > 30:
            classification[account] = "inactive"
        elif max_l > 10000 and days_since <= 7:
            classification[account] = "S"
        elif max_l > 1000 and days_since <= 14:
            classification[account] = "A"
        else:
            classification[account] = "B"

    return classification


# ════════════════════════════════════════════════════════════════════════════
# Open a new Grok conversation page
# ════════════════════════════════════════════════════════════════════════════
def open_grok_page(context):
    """Open a new tab, navigate to grok.com, verify login, enable beta."""
    page = context.new_page()
    try:
        page.goto("https://grok.com", wait_until="domcontentloaded", timeout=60000)
        time.sleep(3)
        if "sign" in page.url.lower() or "login" in page.url.lower():
            print("❌ Not logged in – session expired", flush=True)
            page.close()
            return None
        enable_grok4_beta(page)
        return page
    except Exception as e:
        print(f"❌ Failed to open Grok page: {e}", flush=True)
        try:
            page.close()
        except Exception:
            pass
        return None


# ════════════════════════════════════════════════════════════════════════════
# Run one Grok batch conversation
# ════════════════════════════════════════════════════════════════════════════
def run_grok_batch(context, accounts: list, prompt_builder, label: str,
                   initial_wait: int = 60) -> list:
    """
    Open a fresh Grok page, send the prompt, wait, parse and return JSON objects.
    Each call ≈ 8 rounds × 3 parallel = 24 accounts (within 25-call safety limit).
    """
    if not accounts:
        return []

    elapsed = time.time() - _START_TIME
    print(f"\n[{label}] Starting batch ({len(accounts)} accounts, "
          f"elapsed: {elapsed:.0f}s)...", flush=True)

    page = open_grok_page(context)
    if page is None:
        return []

    try:
        prompt = prompt_builder(accounts)
        send_prompt(page, prompt, label, label.lower().replace(" ", "_"))

        print(f"[{label}] ⏳ Waiting {initial_wait}s for Grok to start...", flush=True)
        time.sleep(initial_wait)

        raw_text = wait_and_extract(
            page, label, label.lower().replace(" ", "_"),
            interval=5, stable_rounds=5, max_wait=360,
            extend_if_growing=True, min_len=50,
        )
        results = parse_jsonlines(raw_text)
        print(f"[{label}] ✅ Parsed {len(results)} JSON objects", flush=True)
        return results

    except Exception as e:
        print(f"[{label}] ❌ Error: {e}", flush=True)
        return []
    finally:
        try:
            page.close()
        except Exception:
            pass


# ════════════════════════════════════════════════════════════════════════════
# LLM summarisation (Claude Sonnet 4.6 via OpenRouter, fallback to Kimi)
# ════════════════════════════════════════════════════════════════════════════
def llm_summarize(combined_jsonl: str, today_str: str):
    """
    Send combined JSON Lines to Claude via OpenRouter for full analysis and
    daily-report generation.  Falls back to Kimi moonshot-v1-32k if OpenRouter
    is unavailable.
    Returns (report_text, cover_title, cover_prompt, cover_insight).
    """
    if not OPENROUTER_API_KEY and not KIMI_API_KEY:
        print("[LLM] ⚠️ No API key configured (OPENROUTER_API_KEY / KIMI_API_KEY)", flush=True)
        return "", "", "", ""

    # Claude has 1M token context – no need to truncate even large payloads.
    # Keep a generous safety cap just in case.
    max_data_chars = 200000
    if len(combined_jsonl) > max_data_chars:
        print(f"[LLM] ⚠️ Data truncated from {len(combined_jsonl)} "
              f"to {max_data_chars} chars", flush=True)
        combined_jsonl = combined_jsonl[:max_data_chars]

    prompt = f"""你是硅谷AI圈资深分析师，精通Twitter/X内容分析和吃瓜日报写作。以下是今天从X平台采集的原始JSON Lines数据：

{combined_jsonl}

今天日期：{today_str}

【重要规则】
- 所有推文内容必须翻译成中文，严禁保留英文原文或任何URL链接
- 翻译风格：准确、简洁、口语化
- type="meta"的元数据行（含 total=0 的 inactive 账号）忽略，不用分析

请完成以下任务：
1. **时间过滤**：只保留最近48小时内的帖子（t字段MMDD与今天比较）；超过48小时但点赞>10000的帖子也保留
2. **价值识别**：筛选10-15条最有价值的帖子，按类别分组
3. **转发链分析**：有qt字段的帖子分析引用者态度；有replies字段的帖子还原完整对话链
4. **输出结构化JSON**（严格遵守以下格式，在@@@START@@@和@@@END@@@之间输出纯JSON，不加任何其他文字）：

@@@START@@@
{{
  "date": "{today_str}",
  "topics": [
    {{
      "category": "巨头宫斗",
      "title": "话题标题（中文，15-25字，极度抓眼球）",
      "account": "原推账号（不含@符号）",
      "real_name": "真实姓名（中英文均可）",
      "likes": "点赞数（格式：≥10000用如1.2w，≥1000用如3.2k，<1000直接用数字）",
      "comments": "评论数（同上格式）",
      "translation": "推文的中文译文，不含任何URL，长文可分段换行",
      "publish_time": "发布时间（格式：YYYY-MM-DD HH:MM PT）",
      "facts": "📌 增量事实：客观中立补充2-3点，每点以\\n- 开头",
      "strategy": "🧠 隐性博弈：行业暗战剖析2-3点，每点以\\n- 开头",
      "capital": "🎯 资本风向标：商业趋势研判2-3点，每点以\\n- 开头"
    }}
  ],
  "cover_title": "中文封面标题（15-30字，极度抓眼球）",
  "cover_prompt": "English image generation prompt (American comic book style, two forces confronting, <=150 words)",
  "cover_insight": "深度解读（100字以内，行业影响或投资启发或日常生活启发）"
}}
@@@END@@@

category字段只能是以下5种之一：巨头宫斗 / 开源生态 / 芯片硬件 / 资本市场 / 学术前沿
topics数组不少于10条，按category分组排列"""

    # ── Try OpenRouter + Claude sonnet-4.6 first ────────────────────────────
    if OPENROUTER_API_KEY:
        for attempt in range(1, 4):
            try:
                print(f"[LLM] Calling Claude Sonnet 4.6 via OpenRouter "
                      f"(data: {len(combined_jsonl)} chars, attempt {attempt}/3)...", flush=True)
                resp = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                        "Content-Type": "application/json",
                        "HTTP-Referer": "https://github.com/Prinsk1NG/X_AI_Github",
                        "X-Title": "AI吃瓜日报",
                    },
                    json={
                        "model": "anthropic/claude-sonnet-4-6",
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.7,
                        "max_tokens": 16000,
                    },
                    timeout=180,
                )
                resp.raise_for_status()
                result = resp.json()["choices"][0]["message"]["content"].strip()
                print(f"[LLM] ✅ Claude response received ({len(result)} chars)", flush=True)
                return _parse_llm_result(result)
            except Exception as e:
                print(f"[LLM] ❌ OpenRouter attempt {attempt}/3: {e}", flush=True)
                if attempt < 3:
                    wait = 2 ** attempt
                    print(f"[LLM] Retrying in {wait}s...", flush=True)
                    time.sleep(wait)

    # ── Fallback to Kimi moonshot-v1-8k ─────────────────────────────────────
    if KIMI_API_KEY:
        try:
            print(f"[LLM] Falling back to Kimi moonshot-v1-8k "
                  f"(data: {len(combined_jsonl)} chars)...", flush=True)
            resp = requests.post(
                "https://api.moonshot.cn/v1/chat/completions",
                headers={"Authorization": f"Bearer {KIMI_API_KEY}",
                         "Content-Type": "application/json"},
                json={
                    "model": "moonshot-v1-32k",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.7,
                    "max_tokens": 8000,
                },
                timeout=180,
            )
            resp.raise_for_status()
            result = resp.json()["choices"][0]["message"]["content"].strip()
            print(f"[LLM] ✅ Kimi fallback response received ({len(result)} chars)", flush=True)
            return _parse_llm_result(result)
        except Exception as e:
            print(f"[LLM] ❌ Kimi fallback error: {e}", flush=True)

    return "", "", "", ""


def _parse_llm_result(result: str):
    """Extract report text and cover metadata from raw LLM output."""
    report_text = extract_markdown_block(result) or result

    # Try JSON format first (new structured output)
    try:
        data = json.loads(report_text)
        return (
            report_text,
            data.get("cover_title", ""),
            data.get("cover_prompt", ""),
            data.get("cover_insight", ""),
        )
    except (json.JSONDecodeError, TypeError, ValueError):
        pass

    # Fallback: legacy freeform markdown with TITLE/PROMPT/INSIGHT after @@@END@@@
    after_end = result[result.find("@@@END@@@") + 9:] if "@@@END@@@" in result else result
    title_m   = re.search(r"TITLE[:：]\s*(.+)", after_end)
    prompt_m  = re.search(r"PROMPT[:：]\s*([\s\S]+?)(?=INSIGHT[:：]|$)", after_end)
    insight_m = re.search(r"INSIGHT[:：]\s*([\s\S]+)", after_end)

    cover_title   = title_m.group(1).strip()   if title_m   else ""
    cover_prompt  = prompt_m.group(1).strip()  if prompt_m  else ""
    cover_insight = insight_m.group(1).strip() if insight_m else ""

    return report_text, cover_title, cover_prompt, cover_insight


# ════════════════════════════════════════════════════════════════════════════
# LLM fallback (TITLE / PROMPT / INSIGHT only)
# Tries OpenRouter + Claude Sonnet 4.6, then Kimi moonshot-v1-8k as last resort.
# ════════════════════════════════════════════════════════════════════════════
def llm_fallback(raw_b_text):
    if not raw_b_text or len(raw_b_text) < 100:
        return "", "", ""

    fallback_prompt = (
        "根据以下内容生成三行结果：\n" + raw_b_text[:6000] +
        "\nTITLE: <标题>\nPROMPT: <英文提示词>\nINSIGHT: <100字以内解读>"
    )

    def _extract(text):
        title_m   = re.search(r"TITLE[:：]\s*(.+)", text)
        prompt_m  = re.search(r"PROMPT[:：]\s*([\s\S]+?)(?=INSIGHT[:：]|$)", text)
        insight_m = re.search(r"INSIGHT[:：]\s*([\s\S]+)", text)
        return (
            title_m.group(1).strip()   if title_m   else "",
            prompt_m.group(1).strip()  if prompt_m  else "",
            insight_m.group(1).strip() if insight_m else "",
        )

    # Try OpenRouter first
    if OPENROUTER_API_KEY:
        for attempt in range(1, 4):
            try:
                resp = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                        "Content-Type": "application/json",
                        "HTTP-Referer": "https://github.com/Prinsk1NG/X_AI_Github",
                        "X-Title": "AI吃瓜日报",
                    },
                    json={
                        "model": "anthropic/claude-sonnet-4-6",
                        "messages": [{"role": "user", "content": fallback_prompt}],
                        "temperature": 0.7,
                        "max_tokens": 2000,
                    },
                    timeout=60,
                )
                resp.raise_for_status()
                return _extract(resp.json()["choices"][0]["message"]["content"].strip())
            except Exception as e:
                print(f"[LLM] ❌ OpenRouter fallback attempt {attempt}/3: {e}", flush=True)
                if attempt < 3:
                    wait = 2 ** attempt
                    time.sleep(wait)

    # Last resort: Kimi moonshot-v1-8k
    if KIMI_API_KEY:
        try:
            resp = requests.post(
                "https://api.moonshot.cn/v1/chat/completions",
                headers={"Authorization": f"Bearer {KIMI_API_KEY}",
                         "Content-Type": "application/json"},
                json={
                    "model": "moonshot-v1-8k",
                    "messages": [{"role": "user", "content": fallback_prompt}],
                    "temperature": 0.7, "max_tokens": 1000,
                },
                timeout=60,
            )
            resp.raise_for_status()
            return _extract(resp.json()["choices"][0]["message"]["content"].strip())
        except Exception:
            pass

    return "", "", ""


# ════════════════════════════════════════════════════════════════════════════
# Format cleanup
# ════════════════════════════════════════════════════════════════════════════
def clean_format(text: str) -> str:
    text = re.sub(r'(@\S[^\n]*)\n\n+(> )', r'\1\n\2', text)
    text = re.sub(r'(> "[^\n]*"[^\n]*)\n\n+(\*\*📝)', r'\1\n\2', text)
    text = re.sub(r'(• [^\n]+)\n\n+(• )', r'\1\n\2', text)
    text = re.sub(r'(• 📌 )涨姿势：\s*', r'\1', text)
    text = re.sub(r'(• 🧠 )猜博弈：\s*', r'\1', text)
    text = re.sub(r'(• 🎯 )识风向：\s*', r'\1', text)
    return text


def generate_cover_image(prompt):
    if not SF_API_KEY or not prompt:
        return ""
    try:
        resp = requests.post(
            "https://api.siliconflow.cn/v1/images/generations",
            headers={"Authorization": f"Bearer {SF_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "black-forest-labs/FLUX.1-schnell",
                  "prompt": prompt, "n": 1, "image_size": "1280x720"},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()["data"][0]["url"]
    except Exception:
        return ""


def upload_to_imgbb(image_path):
    imgbb_key = os.getenv("IMGBB_API_KEY", "")
    if not imgbb_key or not os.path.exists(image_path):
        return ""
    try:
        with open(image_path, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode("utf-8")
        resp = requests.post(
            "https://api.imgbb.com/1/upload",
            params={"key": imgbb_key}, data={"image": img_b64}, timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("success"):
            return data["data"]["url"]
        return ""
    except Exception:
        return ""


# ════════════════════════════════════════════════════════════════════════════
# 🌟 Feishu multi-card builder – one dashboard card + one card per topic
# ════════════════════════════════════════════════════════════════════════════

# Category keyword → Feishu header template colour
_CATEGORY_COLORS = {
    "巨头宫斗": "indigo", "宫斗": "indigo",
    "中文圈":   "orange",
    "开源基建": "green",  "开源": "green", "基建": "green",
    # 硬件 and 空间计算 are treated as one visual dimension, both mapped to purple
    "硬件":     "purple", "空间计算": "purple",
    "投资":     "blue",
    "研究员":   "grey",   "研究": "grey",
}


def _category_color(text: str):
    """Return Feishu template color if text contains a known category keyword, else None."""
    for kw, color in _CATEGORY_COLORS.items():
        if kw in text:
            return color
    return None


def build_feishu_cards(text: str, title: str, insight: str = "") -> list:
    """
    Build Feishu interactive card payload(s) from the LLM report.

    New format: accepts structured JSON from the updated llm_summarize() prompt and
    produces a single interactive card matching the 吃瓜日报 template.
    Legacy format: falls back to the original multi-card markdown parser when the
    input is not valid JSON.
    """
    # ── Attempt JSON parsing (new structured format) ─────────────────────────
    try:
        data = json.loads(text)
        return _build_feishu_cards_json(data)
    except (json.JSONDecodeError, TypeError, ValueError):
        pass

    # ── Legacy markdown fallback ──────────────────────────────────────────────
    return _build_feishu_cards_legacy(text, title, insight)


# Category → section icon mapping
_CATEGORY_SECTION_ICONS = {
    "巨头宫斗": "🏰",
    "开源生态": "🌳",
    "芯片硬件": "💾",
    "资本市场": "💰",
    "学术前沿": "🔬",
}


def _build_feishu_cards_json(data: dict) -> list:
    """Build the new-format Feishu card from parsed JSON topic data."""
    date_str = data.get("date", "")
    topics = data.get("topics", [])

    elements = []

    # ── Banner ────────────────────────────────────────────────────────────────
    elements.append({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": " **⚠️ 每日早8点准时更新 | 全网一手信源 | 深度行业解码 | 无广告无引流** ",
        },
        "icon": {"tag": "standard_icon", "token": "time_outlined", "color": "blue"},
    })
    elements.append({"tag": "hr"})

    # ── Topics grouped by category ────────────────────────────────────────────
    # Preserve insertion order (Python 3.7+)
    seen_categories: list = []
    category_groups: dict = {}
    for t in topics:
        cat = t.get("category", "其他")
        if cat not in category_groups:
            seen_categories.append(cat)
            category_groups[cat] = []
        category_groups[cat].append(t)

    topic_num = 0
    for cat in seen_categories:
        icon = _CATEGORY_SECTION_ICONS.get(cat, "📌")

        # Section header
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"# {icon} {cat}板块"},
        })
        elements.append({"tag": "hr"})

        for t in category_groups[cat]:
            topic_num += 1
            topic_title = t.get("title", "话题")
            account     = t.get("account", "")
            real_name   = t.get("real_name", "")
            likes       = t.get("likes", "-")
            comments    = t.get("comments", "-")
            translation = t.get("translation", "")
            pub_time    = t.get("publish_time", "")
            facts       = t.get("facts", "-")
            strategy    = t.get("strategy", "-")
            capital     = t.get("capital", "-")

            # Topic header
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"## 🍉 {topic_num}号事件 | {topic_title}",
                },
            })

            # Quote note (blue background)
            note_content = (
                f" **🗣️ 极客原声态 | 一手信源** \n"
                f"> **@{account} | {real_name}** (❤️ {likes}赞 | 💬 {comments}评)\n"
                f"> \"{translation}\"\n"
                f"> *原文发布于 {pub_time}*"
            )
            elements.append({
                "tag": "note",
                "elements": [{"tag": "lark_md", "content": note_content}],
                "background_color": "blue",
            })

            # 深度解码 header
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": " **📝 深度解码** "},
            })

            # Three-column analysis
            elements.append({
                "tag": "column_set",
                "flex_mode": "bisect",
                "background_style": "default",
                "columns": [
                    {
                        "tag": "column",
                        "width": "weighted",
                        "weight": 1,
                        "vertical_align": "top",
                        "elements": [{
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": f" **📌 增量事实 | 客观中立补充** \n{facts}",
                            },
                        }],
                    },
                    {
                        "tag": "column",
                        "width": "weighted",
                        "weight": 1,
                        "vertical_align": "top",
                        "elements": [{
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": f" **🧠 隐性博弈 | 行业暗战剖析** \n{strategy}",
                            },
                        }],
                    },
                    {
                        "tag": "column",
                        "width": "weighted",
                        "weight": 1,
                        "vertical_align": "top",
                        "elements": [{
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": f" **🎯 资本风向标 | 商业趋势研判** \n{capital}",
                            },
                        }],
                    },
                ],
            })

    # ── Footer ────────────────────────────────────────────────────────────────
    elements.append({"tag": "hr"})
    elements.append({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": (
                "*📅 本日报每日早8点更新，内容均来自X平台公开信源，"
                "解读仅代表行业观察，不构成任何投资建议*"
            ),
        },
    })
    elements.append({
        "tag": "action",
        "actions": [
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "查看往期日报"},
                "type": "default",
                "complex_interaction": True,
                "width": "default",
                "size": "medium",
            },
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "话题投稿"},
                "type": "primary",
                "complex_interaction": True,
                "width": "default",
                "size": "medium",
            },
        ],
    })

    return [{
        "msg_type": "interactive",
        "card": {
            "config": {
                "wide_screen_mode": True,
                "enable_forward": True,
                "update_multi": False,
            },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": "🌍 昨晚，X上硅谷AI圈在聊啥",
                },
                "subtitle": {
                    "tag": "plain_text",
                    "content": f"📡 AI圈极客吃瓜日报 | {date_str}",
                },
                "template": "blue",
                "ud_icon": {
                    "tag": "standard_icon",
                    "token": "chat-forbidden_outlined",
                },
            },
            "elements": elements,
        },
    }]


def _build_feishu_cards_legacy(text: str, title: str, insight: str = "") -> list:
    """Legacy markdown-based card builder (fallback for non-JSON LLM output)."""
    text = clean_format(text)
    cards = []

    # ── Card 0: Dashboard + Summary ─────────────────────────────────────────
    elements = []

    if insight:
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md",
                     "content": f"<font color='orange'>**💡 Insight**</font>\n{insight}"}
        })
        elements.append({"tag": "hr"})

    # Data panel
    remaining = text
    data_panel_match = re.search(r"【数据看板】\s*([\s\S]*?)(?=【执行摘要】)", text)
    if data_panel_match:
        data_str = data_panel_match.group(1).replace('\n', '')
        parts = [p.strip() for p in data_str.split('|')]
        fields = []
        for p in parts:
            if ':' in p or '：' in p:
                k, v = re.split(r'[:：]', p, 1)
                fields.append({
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**{k.strip()}**\n<font color='grey'>{v.strip()}</font>"
                    }
                })
        if fields:
            elements.append({"tag": "div", "fields": fields})
            elements.append({"tag": "hr"})
        remaining = remaining.replace(data_panel_match.group(0), "")

    # Executive summary
    summary_match = re.search(r"【执行摘要】\s*([\s\S]*?)(?=【动态详情】|\*\*.)", remaining)
    if summary_match:
        summary_text = summary_match.group(1).strip()
        summary_text = summary_text.replace(
            "**🟢 重大利好/突破**", "<font color='green'>**🟢 重大利好/突破**</font>")
        summary_text = summary_text.replace(
            "**🔴 重大风险/争议**", "<font color='red'>**🔴 重大风险/争议**</font>")
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md",
                     "content": f"**📋 EXECUTIVE SUMMARY**\n{summary_text}"}
        })
        remaining = remaining.replace(summary_match.group(0), "")

    cards.append({
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": f"📊 {title}"},
                "template": "indigo",
            },
            "elements": elements,
        },
    })

    # ── Topic cards ──────────────────────────────────────────────────────────
    # Strip boilerplate markers
    remaining = remaining.replace("【动态详情】", "").strip()
    remaining = re.sub(r"^📡.*?\n+", "", remaining).strip()

    # First pass: determine category color per topic number by scanning lines
    topic_colors: dict = {}
    current_color = "indigo"
    topic_idx = 0
    for line in remaining.splitlines():
        stripped = line.strip()
        # Only update color for lines that actually match a known category keyword
        if stripped.startswith("**") and "🍉" not in stripped:
            matched = _category_color(stripped)
            if matched is not None:
                current_color = matched
        elif "**🍉" in stripped:
            topic_idx += 1
            topic_colors[topic_idx] = current_color

    # Second pass: build individual topic cards
    current_color = "indigo"
    topic_idx = 0
    for part in re.split(r"(?=\*\*🍉)", remaining):
        part = part.strip()
        if not part:
            continue

        # Check whether this chunk is a pure section header block (no 🍉 topics inside)
        if "**🍉" not in part:
            # Update color if any category keyword is present
            matched = _category_color(part)
            if matched is not None:
                current_color = matched
            continue

        topic_idx += 1
        color = topic_colors.get(topic_idx, current_color)

        # Extract short title for card header
        title_match = re.match(r"\*\*🍉\s*\d*[.、]?\s*([^\n*]+?)\*{0,2}\s*$",
                               part.splitlines()[0])
        topic_title = title_match.group(1).strip() if title_match else "话题"

        cards.append({
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"tag": "plain_text", "content": f"🍉 {topic_title}"},
                    "template": color,
                },
                "elements": [{
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": part[:4000]},
                }],
            },
        })

    return cards


def push_to_feishu(cards):
    """Push one or more Feishu card payloads to all configured webhooks."""
    webhooks = get_feishu_webhooks()
    if not webhooks:
        return
    if isinstance(cards, dict):
        cards = [cards]
    for idx, card in enumerate(cards, 1):
        for i, url in enumerate(webhooks, 1):
            try:
                resp = requests.post(url, json=card, timeout=30)
                print(f"Feishu card {idx} → webhook #{i}: "
                      f"{resp.status_code} | {resp.text[:80]}", flush=True)
            except Exception as e:
                print(f"Feishu card {idx} → webhook #{i} error: {e}", flush=True)
        if idx < len(cards):
            time.sleep(0.5)  # brief pause to avoid webhook rate-limits


# ════════════════════════════════════════════════════════════════════════════
# WeChat HTML push
# ════════════════════════════════════════════════════════════════════════════
def _md_to_html(text):
    text = re.sub(r"\*\*([^*]+?)\*\*", r"\1", text)
    return text.replace("\n", "<br/>")


def build_wechat_html(text, cover_url="", insight=""):
    cover_block = (
        f'<p style="text-align:center;margin:0 0 16px 0;">'
        f'<img src="{cover_url}" style="max-width:100%;border-radius:8px;" /></p>'
        if cover_url else ""
    )
    insight_block = (
        f'<div style="border-radius:8px;background:#FFF7E6;padding:12px 14px;'
        f'margin:0 0 16px 0;"><div style="font-weight:bold;margin-bottom:6px;">'
        f'🔍 深度解读</div><div>{insight.replace(chr(10), "<br/>")}</div></div>'
        if insight else ""
    )

    # Handle JSON format (new structured output)
    try:
        data = json.loads(text)
        html_body = _json_topics_to_html(data)
        return cover_block + insight_block + html_body
    except (json.JSONDecodeError, TypeError, ValueError):
        pass

    # Legacy markdown format
    text = clean_format(text)
    return cover_block + insight_block + _md_to_html(text)


def _json_topics_to_html(data: dict) -> str:
    """Convert structured JSON topics to simple HTML for WeChat."""
    topics = data.get("topics", [])
    parts = []
    for i, t in enumerate(topics, 1):
        title    = t.get("title", "")
        account  = t.get("account", "")
        rname    = t.get("real_name", "")
        likes    = t.get("likes", "-")
        comments = t.get("comments", "-")
        trans    = t.get("translation", "").replace("\n", "<br/>")
        pub_time = t.get("publish_time", "")
        facts    = t.get("facts", "").replace("\n", "<br/>")
        strategy = t.get("strategy", "").replace("\n", "<br/>")
        capital  = t.get("capital", "").replace("\n", "<br/>")

        parts.append(
            f'<h3>🍉 {i}号事件 | {title}</h3>'
            f'<p><b>@{account} | {rname}</b>'
            f'&nbsp;&nbsp;❤️ {likes} | 💬 {comments}</p>'
            f'<blockquote>{trans}</blockquote>'
            f'<p><i>发布于 {pub_time}</i></p>'
            f'<p><b>📌 增量事实：</b>{facts}</p>'
            f'<p><b>🧠 隐性博弈：</b>{strategy}</p>'
            f'<p><b>🎯 资本风向标：</b>{capital}</p>'
            f'<hr/>'
        )
    return "\n".join(parts)


def push_to_jijyun(html_content, title, cover_url=""):
    if not JIJYUN_WEBHOOK_URL:
        return
    try:
        resp = requests.post(
            JIJYUN_WEBHOOK_URL,
            json={"title": title, "author": "大尉Prinski",
                  "html_content": html_content, "cover_jpg": cover_url},
            timeout=30,
        )
        print(f"WeChat push: {resp.status_code} | {resp.text[:120]}", flush=True)
    except Exception as e:
        print(f"WeChat push error: {e}", flush=True)


# ════════════════════════════════════════════════════════════════════════════
# Utility helpers
# ════════════════════════════════════════════════════════════════════════════
def extract_markdown_block(text):
    start = text.find("@@@START@@@")
    end   = text.find("@@@END@@@")
    if start == -1:
        return ""
    cs = start + len("@@@START@@@")
    return text[cs:end].strip() if (end != -1 and end > start) else text[cs:].strip()


def is_valid_content(text):
    if not text or len(text) < 200:
        return False
    # JSON format (new): must have a non-empty topics array
    try:
        data = json.loads(text)
        return bool(data.get("topics"))
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    # Legacy markdown format
    return len(text) >= 300 and "【数据看板】" in text and "🍉" in text


def _is_placeholder(text):
    return not text or (text.startswith("<") and text.endswith(">"))


# ════════════════════════════════════════════════════════════════════════════
# Save daily data to data/ directory
# ════════════════════════════════════════════════════════════════════════════
def save_daily_data(today_str: str, post_objects: list, meta_results: dict,
                    report_text: str, classification: dict):
    """Persist all collected data under data/YYYY-MM-DD/."""
    data_dir = Path(f"data/{today_str}")
    data_dir.mkdir(parents=True, exist_ok=True)

    # combined.txt – post lines only (no meta rows)
    combined_txt = "\n".join(
        json.dumps(obj, ensure_ascii=False)
        for obj in post_objects
        if obj.get("type") != "meta"
    )
    (data_dir / "combined.txt").write_text(combined_txt, encoding="utf-8")

    # meta.json – per-account metadata
    (data_dir / "meta.json").write_text(
        json.dumps(meta_results, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # daily_report.txt – Kimi report (may be empty on first save)
    if report_text:
        (data_dir / "daily_report.txt").write_text(report_text, encoding="utf-8")

    # data/classification.json – latest classification (updated every run)
    cls_path = Path("data/classification.json")
    cls_path.write_text(
        json.dumps({"date": today_str, "classification": classification},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        f"[Data] ✅ Saved to {data_dir} "
        f"({sum(1 for o in post_objects if o.get('type') != 'meta')} posts, "
        f"{len(meta_results)} accounts)",
        flush=True,
    )


# ════════════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════════════
def main():
    print("=" * 60, flush=True)
    print("🚀 AI吃瓜日报 v2.0（Grok搜索 + Kimi总结）", flush=True)
    print("=" * 60, flush=True)

    check_cookie_expiry()
    is_storage_state = prepare_session_file()
    today_str, _ = get_dates()

    # Ensure data/ root exists for classification.json
    Path("data").mkdir(exist_ok=True)

    # ── Collected data ──────────────────────────────────────────────────────
    meta_results  = {}    # account → {total, max_l, latest}
    phase1_posts  = {}    # account → [post_obj, ...]  (Phase-1 data, 3 posts)
    phase2_posts  = {}    # account → [post_obj, ...]  (Phase-2 data, S/A only)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage", "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1280,800",
            ],
        )

        ctx_opts = {
            "viewport":   {"width": 1280, "height": 800},
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "locale": "zh-CN",
        }
        if is_storage_state:
            ctx_opts["storage_state"] = "session_state.json"

        context = browser.new_context(**ctx_opts)
        if not is_storage_state:
            load_raw_cookies(context)

        # ── Login verification ──────────────────────────────────────────────
        verify_page = context.new_page()
        verify_page.goto("https://grok.com", wait_until="domcontentloaded", timeout=60000)
        time.sleep(3)
        if "sign" in verify_page.url.lower() or "login" in verify_page.url.lower():
            print("❌ Not logged in – Cookie/Session expired. "
                  "Please update SUPER_GROK_COOKIES.", flush=True)
            browser.close()
            raise SystemExit(1)
        print("✅ Logged in to Grok", flush=True)
        verify_page.close()

        # ════════════════════════════════════════════════════════════════════
        # Phase 1 – Scan all accounts (batches of 24, B-level strategy)
        # ════════════════════════════════════════════════════════════════════
        print("\n" + "=" * 50, flush=True)
        print("📊 Phase 1: Tiered scan – all accounts", flush=True)
        print("=" * 50, flush=True)

        BATCH_SIZE = 24   # 8 rounds × 3 parallel = 24 accounts per conversation

        for batch_num, batch_start in enumerate(
                range(0, len(ALL_ACCOUNTS), BATCH_SIZE), start=1):

            elapsed = time.time() - _START_TIME
            if elapsed > PHASE1_DEADLINE:
                remaining = ALL_ACCOUNTS[batch_start:]
                print(
                    f"\n[Phase 1] ⚠️ Timeout ({elapsed:.0f}s > {PHASE1_DEADLINE}s), "
                    f"skipping {len(remaining)} remaining accounts (B-tier degradation).",
                    flush=True,
                )
                break

            batch   = ALL_ACCOUNTS[batch_start:batch_start + BATCH_SIZE]
            label   = f"Phase1-Batch{batch_num}"
            results = run_grok_batch(context, batch, build_phase1_prompt, label)

            for obj in results:
                account = obj.get("a", "").lstrip("@")
                if not account:
                    continue
                if obj.get("type") == "meta":
                    meta_results[account] = {
                        "total":  obj.get("total", 0),
                        "max_l":  obj.get("max_l", 0),
                        "latest": obj.get("latest", "NA"),
                    }
                else:
                    phase1_posts.setdefault(account, []).append(obj)

        print(
            f"\n[Phase 1] Done: {len(meta_results)} metadata rows, "
            f"{len(phase1_posts)} accounts with posts.",
            flush=True,
        )

        # ════════════════════════════════════════════════════════════════════
        # Classification
        # ════════════════════════════════════════════════════════════════════
        classification = classify_accounts(meta_results)
        s_accounts = [a for a, t in classification.items() if t == "S"]
        a_accounts = [a for a, t in classification.items() if t == "A"]
        b_accounts = [a for a, t in classification.items() if t == "B"]
        inactive   = [a for a, t in classification.items() if t == "inactive"]

        print(
            f"\n[Classification] S: {len(s_accounts)} | A: {len(a_accounts)} | "
            f"B: {len(b_accounts)} | inactive: {len(inactive)}",
            flush=True,
        )
        if s_accounts:
            print(f"  S-tier: {s_accounts}", flush=True)
        if a_accounts:
            print(f"  A-tier (first 10): {a_accounts[:10]}", flush=True)

        # ════════════════════════════════════════════════════════════════════
        # Phase 2 – Differential re-collection for S and A tiers
        # ════════════════════════════════════════════════════════════════════
        if (time.time() - _START_TIME) < GLOBAL_DEADLINE and (s_accounts or a_accounts):
            print("\n" + "=" * 50, flush=True)
            print("📊 Phase 2: Differential collection (S + A tiers)", flush=True)
            print("=" * 50, flush=True)

            # S-tier accounts (10 posts + thread fetches)
            for batch_start in range(0, len(s_accounts), BATCH_SIZE):
                if (time.time() - _START_TIME) >= GLOBAL_DEADLINE:
                    break
                batch   = s_accounts[batch_start:batch_start + BATCH_SIZE]
                label   = f"Phase2-S-Batch{batch_start // BATCH_SIZE + 1}"
                results = run_grok_batch(context, batch, build_phase2_s_prompt, label,
                                         initial_wait=90)
                for obj in results:
                    account = obj.get("a", "").lstrip("@")
                    if account and obj.get("type") != "meta":
                        phase2_posts.setdefault(account, []).append(obj)

            # A-tier accounts (5 posts)
            for batch_start in range(0, len(a_accounts), BATCH_SIZE):
                if (time.time() - _START_TIME) >= GLOBAL_DEADLINE:
                    break
                batch   = a_accounts[batch_start:batch_start + BATCH_SIZE]
                label   = f"Phase2-A-Batch{batch_start // BATCH_SIZE + 1}"
                results = run_grok_batch(context, batch, build_phase2_a_prompt, label)
                for obj in results:
                    account = obj.get("a", "").lstrip("@")
                    if account and obj.get("type") != "meta":
                        phase2_posts.setdefault(account, []).append(obj)

        # ── Session renewal ─────────────────────────────────────────────────
        save_and_renew_session(context)
        browser.close()

    # ════════════════════════════════════════════════════════════════════════
    # Merge data: Phase-2 data overrides Phase-1 for S/A accounts.
    # B accounts retain their Phase-1 posts (3 per account).
    # ════════════════════════════════════════════════════════════════════════
    print("\n[Merge] Combining Phase 1 + Phase 2 data...", flush=True)
    combined_posts: dict = {}
    combined_posts.update(phase1_posts)     # B-tier baseline
    combined_posts.update(phase2_posts)     # S/A override

    all_post_objects = [obj for posts in combined_posts.values() for obj in posts]
    print(
        f"[Merge] Total: {len(all_post_objects)} posts "
        f"from {len(combined_posts)} accounts",
        flush=True,
    )

    # Build JSON Lines string for LLM
    combined_jsonl = "\n".join(
        json.dumps(obj, ensure_ascii=False) for obj in all_post_objects
    )

    # Persist Phase-1/merge data (report will be updated after LLM)
    save_daily_data(today_str, all_post_objects, meta_results, "", classification)

    # ════════════════════════════════════════════════════════════════════════
    # LLM summarisation
    # ════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 50, flush=True)
    print("🤖 LLM: Generating daily report...", flush=True)
    print("=" * 50, flush=True)

    report_text, cover_title, cover_prompt, cover_insight = llm_summarize(
        combined_jsonl, today_str
    )

    # Fallback if LLM report is insufficient
    if not is_valid_content(report_text):
        print("[LLM] ⚠️ Report quality check failed, trying fallback...", flush=True)
        if not cover_title and not cover_prompt:
            cover_title, cover_prompt, cover_insight = llm_fallback(combined_jsonl[:6000])
        if not report_text:
            report_text = (
                f"@@@START@@@\n"
                f"📡 硅谷AI圈大事扫描 | {today_str}\n\n"
                f"【数据看板】\n"
                f"跟踪大V总数: 100 | 有动态的大V: {len(combined_posts)} | "
                f"重点高价值动态: - | 热点趋势: -\n\n"
                f"【执行摘要】\n"
                f"**{today_str}**\n\n"
                f"【动态详情】\n\n"
                f"🍉 数据采集完成，共 {len(all_post_objects)} 条帖子。\n"
                f"@@@END@@@"
            )

    # Persist final report
    if report_text:
        report_path = Path(f"data/{today_str}/daily_report.txt")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report_text, encoding="utf-8")

    # ════════════════════════════════════════════════════════════════════════
    # Cover image generation
    # ════════════════════════════════════════════════════════════════════════
    cover_url = generate_cover_image(cover_prompt)
    if cover_url:
        import urllib.request
        try:
            urllib.request.urlretrieve(cover_url, "cover.png")
        except Exception:
            pass

    # Determine display title
    if cover_title and not _is_placeholder(cover_title):
        title = cover_title
    else:
        m = re.search(r"📡.*?[|\n]", report_text or "")
        title = m.group(0).strip("📡| \n") if m else "AI圈极客大事扫描"
    print(f"\nFinal title: {title}", flush=True)

    imgbb_url      = upload_to_imgbb("cover.png")
    final_cover_url = imgbb_url if imgbb_url else cover_url

    final_markdown = extract_markdown_block(report_text) or report_text or ""
    final_markdown = clean_format(final_markdown)

    # ════════════════════════════════════════════════════════════════════════
    # Push to Feishu + WeChat
    # ════════════════════════════════════════════════════════════════════════
    print("\n[Push] Sending to Feishu (multi-card layout)...", flush=True)
    push_to_feishu(
        build_feishu_cards(final_markdown, title, cover_insight)
    )

    push_to_jijyun(
        build_wechat_html(final_markdown, final_cover_url, cover_insight),
        title, final_cover_url,
    )

    print("\n🎉 All tasks completed!", flush=True)


if __name__ == "__main__":
    main()
