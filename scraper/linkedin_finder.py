"""
LinkedIn Contact Finder — finds multiple contacts per company via Google SERP.

Searches for:
- Owner / Founder / President (signals: small-mid business)
- Marketing Manager / Head of Marketing / CMO (signals: budget, growth focus)
- Head of Sales / VP Sales / Sales Director (signals: mid-market, revenue focus)

Returns up to 3 verified contacts per company.
"""

import asyncio
import re
from urllib.parse import urlencode
import httpx
from config import SERPAPI_KEY

# Target job titles grouped by seniority/signal value
CONTACT_TIERS = [
    {
        "label": "Owner",
        "titles": ["owner", "founder", "co-founder", "president", "principal", "proprietor", "director"],
        "signal": "Decision maker",
    },
    {
        "label": "Marketing",
        "titles": ["marketing manager", "head of marketing", "marketing director", "cmo", "chief marketing officer", "digital marketing", "marketing coordinator"],
        "signal": "Budget holder",
    },
    {
        "label": "Sales",
        "titles": ["head of sales", "sales director", "vp sales", "vp of sales", "sales manager", "business development", "chief revenue officer", "cro"],
        "signal": "Revenue focused",
    },
]


async def _serp(query: str, num: int = 5) -> list[dict]:
    params = {"engine": "google", "q": query, "api_key": SERPAPI_KEY, "num": num, "gl": "us", "hl": "en"}
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get("https://serpapi.com/search", params=params)
            return r.json().get("organic_results", [])
    except Exception:
        return []


def _extract_linkedin_url(results: list[dict]) -> str:
    for r in results:
        link = r.get("link", "")
        if "linkedin.com/in/" in link:
            # Clean URL - remove query params
            return link.split("?")[0]
    return ""


def _extract_name_from_linkedin(result: dict) -> tuple[str, str]:
    """Extract first and last name from LinkedIn search result title."""
    title = result.get("title", "")
    # LinkedIn titles are usually "First Last - Title - Company | LinkedIn"
    parts = title.split(" - ")[0].strip()
    name_parts = parts.split()
    if len(name_parts) >= 2:
        return name_parts[0], " ".join(name_parts[1:])
    elif len(name_parts) == 1:
        return name_parts[0], ""
    return "", ""


def _extract_title_from_snippet(snippet: str, tier: dict) -> str:
    """Extract the actual job title from snippet."""
    snippet_lower = snippet.lower()
    for title in tier["titles"]:
        if title in snippet_lower:
            # Find the actual cased version
            idx = snippet_lower.find(title)
            return snippet[idx:idx+len(title)].title()
    return tier["label"]


async def find_contacts_for_company(
    company: str,
    website: str,
    location: str = "",
    max_contacts: int = 3,
    log_cb=None
) -> list[dict]:
    """
    Find up to max_contacts people at a company via LinkedIn Google search.
    Returns list of contact dicts with name, title, linkedin_url, email permutations.
    """
    async def log(msg):
        if log_cb: await log_cb(msg)

    contacts = []
    domain = website.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0]

    await log(f"  Finding contacts at {company}...")

    for tier in CONTACT_TIERS:
        if len(contacts) >= max_contacts:
            break

        # Build LinkedIn search query
        title_query = " OR ".join(f'"{t}"' for t in tier["titles"][:3])
        query = f'site:linkedin.com/in "{company}" ({title_query})'
        if location:
            query += f' "{location}"'

        results = await _serp(query, num=3)

        for r in results:
            if len(contacts) >= max_contacts:
                break

            link = r.get("link", "")
            if "linkedin.com/in/" not in link:
                continue

            linkedin_url = link.split("?")[0]

            # Skip if already found this person
            if any(c["linkedin_url"] == linkedin_url for c in contacts):
                continue

            first, last = _extract_name_from_linkedin(r)
            if not first:
                continue

            snippet = r.get("snippet", "")
            title = _extract_title_from_snippet(snippet, tier)

            await log(f"  Found: {first} {last} — {title}")

            contacts.append({
                "first_name": first,
                "last_name": last,
                "job_title": title,
                "linkedin_url": linkedin_url,
                "tier_label": tier["label"],
                "tier_signal": tier["signal"],
                "email": "",
                "email_verified": False,
            })

        await asyncio.sleep(0.3)

    # Now try to find emails for each contact
    from scraper.owner_finder import _generate_permutations, verify_email_google, verify_email_smtp

    for contact in contacts:
        if not domain:
            continue

        perms = _generate_permutations(contact["first_name"], contact["last_name"], domain)
        await log(f"  Checking emails for {contact['first_name']} {contact['last_name']}...")

        for perm in perms[:3]:
            if await verify_email_google(perm):
                contact["email"] = perm
                contact["email_verified"] = True
                await log(f"  ✅ Verified: {perm}")
                break
            await asyncio.sleep(0.15)

        if not contact["email_verified"] and perms:
            # Try SMTP
            for perm in perms[:1]:
                try:
                    if await verify_email_smtp(perm):
                        contact["email"] = perm
                        contact["email_verified"] = True
                        await log(f"  ✅ SMTP verified: {perm}")
                        break
                except Exception:
                    pass

            if not contact["email_verified"] and perms:
                contact["email"] = perms[0]  # Best guess
                await log(f"  Best guess: {perms[0]}")

    return contacts
