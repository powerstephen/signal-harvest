"""
Signal Harvest — extraction engine.
Two modes:
  - domain: given a list of domains, extract contacts directly
  - search: given an ICP description, find companies via SERP then extract
"""

import asyncio
import re
from typing import AsyncIterator
from urllib.parse import urlparse, urlencode, unquote

import httpx

from llm.groq_client import (
    extract_contacts_enhanced,
    classify_industry,
    score_company,
    expand_search_queries,
)

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
NON_EMAIL_TLDS = {"jpg", "jpeg", "png", "gif", "webp", "svg", "ico", "css", "js", "pdf", "zip"}
GENERIC_EMAIL_PREFIX = {
    "noreply", "no-reply", "donotreply", "postmaster", "example", "support",
    "info", "admin", "hello", "contact", "enquiries", "hr", "jobs", "careers",
    "billing", "accounts", "sales", "marketing", "pr", "press"
}
SKIP_DOMAINS = {
    "facebook.com", "instagram.com", "twitter.com", "x.com", "linkedin.com",
    "youtube.com", "tiktok.com", "wikipedia.org", "amazon.com", "yelp.com",
    "google.com", "bing.com", "duckduckgo.com", "github.com", "medium.com",
    "g2.com", "capterra.com", "trustpilot.com", "crunchbase.com", "bloomberg.com",
}
LOW_VALUE_PATTERNS = re.compile(
    r"/(blog|news|press|posts?|articles?|insights?|guides?|tutorials?|"
    r"categor(y|ies)|tag|wiki|compare|vs|alternatives?|directory|listings?|"
    r"search|results?)(/|$)",
    re.IGNORECASE,
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _root_domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


def _normalise_domain(raw: str) -> str:
    raw = raw.strip().lower()
    if not raw:
        return ""
    if not raw.startswith("http"):
        raw = "https://" + raw
    return _root_domain(raw)


def _is_skippable(url: str) -> bool:
    host = _root_domain(url)
    if not host:
        return True
    if any(host == s or host.endswith("." + s) for s in SKIP_DOMAINS):
        return True
    try:
        path = urlparse(url).path or ""
    except Exception:
        path = ""
    return bool(LOW_VALUE_PATTERNS.search(path))


def _valid_email(e: str) -> bool:
    if "@" not in e:
        return False
    local, _, dom = e.partition("@")
    if not local or not dom or "." not in dom:
        return False
    tld = dom.rsplit(".", 1)[-1].lower()
    if tld in NON_EMAIL_TLDS:
        return False
    if len(e) > 100 or len(local) < 2:
        return False
    return True


def _is_generic_email(e: str) -> bool:
    prefix = e.split("@")[0].lower()
    return prefix in GENERIC_EMAIL_PREFIX


async def _fetch_text(client: httpx.AsyncClient, url: str) -> str:
    try:
        r = await client.get(url, timeout=15, follow_redirects=True, headers=HEADERS)
        return r.text[:15000]
    except Exception:
        return ""


async def _fetch_site_text(base_url: str) -> str:
    chunks = []
    async with httpx.AsyncClient() as c:
        for path in ["", "/about", "/about-us", "/team", "/contact"]:
            target = base_url.rstrip("/") + path if path else base_url
            text = await _fetch_text(c, target)
            if text:
                chunks.append(text[:4000])
            if len("\n".join(chunks)) > 10000:
                break
    return "\n".join(chunks)[:10000]


def _regex_emails(text: str, host: str) -> list[str]:
    emails, seen = [], set()
    for m in EMAIL_RE.finditer(text):
        e = m.group(0).lower().rstrip(".,;:")
        if _valid_email(e) and e not in seen:
            seen.add(e)
            emails.append(e)
    if host:
        host_root = host.split(".")[0]
        same = [e for e in emails if host_root in e.split("@", 1)[1]]
        named = [e for e in same if not _is_generic_email(e)]
        others = [e for e in emails if e not in same]
        emails = (named + others)[:5]
    return emails


async def _ddg_search(query: str) -> list[dict]:
    url = "https://html.duckduckgo.com/html/?" + urlencode({"q": query})
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(url, timeout=20, headers=HEADERS, follow_redirects=True)
            text = r.text
    except Exception:
        return []
    results = []
    for m in re.finditer(r'class="result__a"[^>]*href="([^"]+)"[^>]*>([^<]+)', text):
        href, title = m.group(1), m.group(2)
        m2 = re.search(r"uddg=([^&]+)", href)
        real = unquote(m2.group(1)) if m2 else href
        results.append({"url": real, "title": title.strip(), "snippet": ""})
    return results[:15]


async def _process_domain(domain: str, signal: str, country: str, notes: str, log_cb) -> dict | None:
    base_url = f"https://{domain}"
    await log_cb(f"Fetching: {domain}")
    try:
        text = await _fetch_site_text(base_url)
    except Exception as e:
        await log_cb(f"  failed: {e}")
        return None

    if not text:
        await log_cb(f"  no content: {domain}")
        return None

    emails = _regex_emails(text, domain)
    llm_data = await extract_contacts_enhanced(domain, text, domain_hint=domain)
    industry = await classify_industry(domain, llm_data.get("description", ""), base_url)

    llm_emails = [e for e in llm_data.get("emails", []) if _valid_email(e)]
    all_emails = list(dict.fromkeys(llm_emails + emails))
    named = [e for e in all_emails if not _is_generic_email(e)]
    final_emails = named[:3] if named else all_emails[:3]

    first = llm_data.get("first_name", "")
    last = llm_data.get("last_name", "")

    await log_cb(f"  done — {first} {last} | {len(final_emails)} emails | {industry}")

    return {
        "company": domain.split(".")[0].replace("-", " ").title(),
        "website": base_url,
        "industry": industry,
        "employee_count": llm_data.get("employee_count", ""),
        "description": llm_data.get("description", ""),
        "country": country,
        "first_name": first,
        "last_name": last,
        "email": final_emails[0] if final_emails else "",
        "phone": llm_data.get("phones", [""])[0] if llm_data.get("phones") else "",
        "job_title": llm_data.get("job_title", ""),
        "linkedin_url": llm_data.get("linkedin_url", ""),
        "signal": signal,
        "relevance_score": 0.0,
        "relevance_reason": "",
        "source_url": base_url,
    }


async def run_domain_mode(domains: list[tuple[str, str]], country: str, notes: str, log_cb=None) -> AsyncIterator[dict]:
    async def log(msg): 
        if log_cb: await log_cb(msg)

    await log(f"Domain mode: {len(domains)} domains")
    for raw_domain, signal in domains:
        domain = _normalise_domain(raw_domain)
        if not domain:
            continue
        result = await _process_domain(domain, signal, country, notes, log)
        if result:
            yield result
        await asyncio.sleep(0.5)
    await log("Done.")


SCORE_THRESHOLD = 35
MAX_ROUNDS = 4


async def run_search_mode(icp: str, country: str, notes: str, limit: int, log_cb=None) -> AsyncIterator[dict]:
    async def log(msg):
        if log_cb: await log_cb(msg)

    target = max(1, min(limit, 25))
    yielded = 0
    seen: set[str] = set()
    used_queries: list[str] = []

    await log(f"Search mode: {target} leads for '{icp}'")

    for round_num in range(1, MAX_ROUNDS + 1):
        if yielded >= target:
            break
        await log(f"── Round {round_num}/{MAX_ROUNDS} · need {target - yielded} more ──")

        try:
            queries = await expand_search_queries(icp, country, notes, previous=used_queries or None)
        except Exception as e:
            await log(f"Query expansion failed: {e}")
            queries = [f"{icp} {country}".strip()]

        new_queries = [q for q in queries if q not in used_queries]
        if not new_queries:
            await log("No fresh queries — stopping.")
            break
        used_queries.extend(new_queries)

        candidates = []
        for q in new_queries:
            await log(f"SERP: {q}")
            try:
                results = await _ddg_search(q)
            except Exception as e:
                await log(f"  SERP error: {e}")
                results = []
            for r in results:
                host = _root_domain(r["url"])
                if not host or host in seen or _is_skippable(r["url"]):
                    continue
                seen.add(host)
                candidates.append(r)
            await asyncio.sleep(1)

        if not candidates:
            await log("No new candidates.")
            continue

        await log(f"{len(candidates)} candidates — scoring...")
        scored = []
        for c in candidates:
            try:
                s = await score_company(c["title"], c["url"], c["snippet"], icp, notes)
            except Exception:
                s = {"score": 0.0, "reason": ""}
            if s["score"] >= SCORE_THRESHOLD:
                c["score"] = s["score"]
                c["reason"] = s["reason"]
                scored.append(c)

        scored.sort(key=lambda x: x["score"], reverse=True)
        await log(f"{len(scored)} passed threshold")

        for c in scored:
            if yielded >= target:
                break
            host = _root_domain(c["url"])
            result = await _process_domain(host, "", country, notes, log)
            if result:
                result["relevance_score"] = c["score"]
                result["relevance_reason"] = c["reason"]
                result["source_url"] = c["url"]
                yielded += 1
                yield result
            await asyncio.sleep(0.5)

    await log(f"✓ Done. {yielded} prospects found.")
