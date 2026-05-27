"""
Google Email Hunter — finds business emails via Google Search snippets.
Uses SerpApi to search for businesses with emails indexed by Google.
No scraping, no Cloudflare issues — Google already did the crawling.
"""

import asyncio
import re
from urllib.parse import urlencode

import httpx

from config import SERPAPI_KEY

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Email domain patterns that indicate real business owners
# Old-school domains = no Cloudflare = our ideal targets
BUSINESS_EMAIL_DOMAINS = [
    "@gmail.com", "@yahoo.com", "@hotmail.com", "@bellsouth.net",
    "@aol.com", "@outlook.com", "@live.com", "@comcast.net",
    "@att.net", "@verizon.net", "@icloud.com", "@me.com",
    "@msn.com", "@earthlink.net", "@cox.net", "@charter.net",
    "@sbcglobal.net", "@roadrunner.com", "@optonline.net",
]

# Search query templates
QUERY_TEMPLATES = [
    '"{industry}" "{location}" "@gmail.com"',
    '"{industry}" "{location}" "@yahoo.com"',
    '"{industry}" "{location}" "@bellsouth.net" OR "@aol.com" OR "@comcast.net"',
    '"{industry}" "{location}" "@hotmail.com" OR "@outlook.com" OR "@live.com"',
    '"{industry}" "{location}" "@att.net" OR "@verizon.net" OR "@cox.net"',
    '"{industry}" "{location}" "@sbcglobal.net" OR "@earthlink.net" OR "@charter.net"',
    '"{industry}" "{location}" "@icloud.com" OR "@me.com" OR "@msn.com"',
    'site:yellowpages.com "{industry}" "{location}" email',
    'site:yelp.com "{industry}" "{location}" email',
    'site:bbb.org "{industry}" "{location}" email',
    'site:nextdoor.com "{industry}" "{location}" email',
    'site:angieslist.com "{industry}" "{location}" email',
    'site:thumbtack.com "{industry}" "{location}" email',
    '"{industry} contractor" "{location}" email contact',
    '"{industry} company" "{location}" "@gmail.com" OR "@yahoo.com"',
    '"{industry} services" "{location}" email phone',
    '"{industry}" "{location}" "email us" OR "email:" site:*.com',
    'inurl:contact "{industry}" "{location}" "@gmail.com" OR "@yahoo.com"',
    '"{industry}" "{location}" FL "@bellsouth.net" OR "@aol.com"',
    '"roofing" "{location}" "email" "@"',
]

SKIP_DOMAINS = {
    "linkedin.com", "facebook.com", "twitter.com", "instagram.com",
    "google.com", "yelp.com", "yellowpages.com", "bbb.org",
    "indeed.com", "glassdoor.com", "angi.com", "homeadvisor.com",
}

GENERIC_PREFIXES = {
    "noreply", "no-reply", "donotreply", "support", "admin",
    "info", "hello", "contact", "enquiries", "billing", "postmaster"
}


def _valid_business_email(email: str) -> bool:
    email = email.lower()
    if "@" not in email:
        return False
    local, _, domain = email.partition("@")
    if len(local) < 2 or len(email) > 80:
        return False
    if any(bad in email for bad in ["example.", "test@", "png", "jpg", "css", "js", "schema"]):
        return False
    return True


def _is_personal_or_business_email(email: str) -> bool:
    """Prefer emails that look like real owner emails vs generic."""
    local = email.split("@")[0].lower()
    return local not in GENERIC_PREFIXES


def _extract_emails_from_snippet(text: str) -> list[str]:
    emails = EMAIL_RE.findall(text)
    return [e for e in emails if _valid_business_email(e)]


async def _serp_search(query: str) -> list[dict]:
    """Run a Google search via SerpApi and return organic results."""
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERPAPI_KEY,
        "num": 10,
        "gl": "us",
        "hl": "en",
    }
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get("https://serpapi.com/search", params=params)
            data = r.json()
            return data.get("organic_results", [])
    except Exception:
        return []


def _extract_business_from_result(result: dict) -> dict | None:
    """Extract business info from a Google search result."""
    title = result.get("title", "")
    snippet = result.get("snippet", "")
    link = result.get("link", "")

    # Skip directory/social sites
    from urllib.parse import urlparse
    host = urlparse(link).netloc.lower().replace("www.", "")
    if any(host == s or host.endswith("." + s) for s in SKIP_DOMAINS):
        return None

    # Find emails in snippet and title
    all_text = f"{title} {snippet}"
    emails = _extract_emails_from_snippet(all_text)

    if not emails:
        return None

    # Prefer personal/business emails over generic
    personal = [e for e in emails if _is_personal_or_business_email(e)]
    best_email = personal[0] if personal else emails[0]

    # Extract phone from snippet
    phone_m = re.search(r'\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}', snippet)
    phone = phone_m.group(0) if phone_m else ""

    return {
        "company": title.split("|")[0].split("-")[0].strip(),
        "website": link,
        "email": best_email,
        "phone": phone,
        "description": snippet[:200],
        "source_url": link,
    }


async def search_google_emails(
    industry: str,
    location: str,
    limit: int = 20,
    log_cb=None
) -> list[dict]:
    """
    Find businesses with publicly indexed emails via Google Search.
    Returns list of businesses with name, email, phone, website.
    """
    async def log(msg):
        if log_cb: await log_cb(msg)

    await log(f"Google email search: {industry} in {location}")

    results = []
    seen_emails = set()
    seen_companies = set()

    for i, template in enumerate(QUERY_TEMPLATES):
        if len(results) >= limit:
            break

        query = template.replace("{industry}", industry).replace("{location}", location)
        await log(f"Query {i+1}/{len(QUERY_TEMPLATES)}: {query[:60]}...")

        serp_results = await _serp_search(query)
        await log(f"  Got {len(serp_results)} results")

        for r in serp_results:
            if len(results) >= limit:
                break

            biz = _extract_business_from_result(r)
            if not biz:
                continue

            # Deduplicate by email
            if biz["email"] in seen_emails:
                continue
            company_key = biz["company"].lower()[:20]
            if company_key in seen_companies:
                continue

            seen_emails.add(biz["email"])
            seen_companies.add(company_key)

            await log(f"  ✅ {biz['company']} — {biz['email']} | {biz['phone']}")
            results.append({
                **biz,
                "industry": industry,
                "employee_count": "",
                "country": "US",
                "first_name": "",
                "last_name": "",
                "job_title": "",
                "linkedin_url": "",
                "signal": "Google indexed email",
                "relevance_score": 80.0,
                "relevance_reason": "Email publicly indexed by Google",
            })

        await asyncio.sleep(0.5)  # be nice to SerpApi

    await log(f"✓ Found {len(results)} businesses with emails")
    return results
