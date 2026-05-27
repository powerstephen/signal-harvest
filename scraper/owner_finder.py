"""
Owner Finder — finds business owner names and verifies their email.

Flow:
1. Google search for owner name via BBB, Yelp, local directories
2. Extract first + last name from results
3. Generate email permutations from name + domain
4. Verify permutations via Google search (is it indexed anywhere?)
5. SMTP verify the best candidate
6. Return verified owner email
"""

import asyncio
import re
import smtplib
import socket
import dns.resolver
from urllib.parse import urlparse, urlencode

import httpx

from config import SERPAPI_KEY


# Domains that are directories - email verified there doesn't mean it's the owner's email
DIRECTORY_DOMAINS = {
    "nextdoor.com", "yelp.com", "yellowpages.com", "bbb.org", "angi.com",
    "homeadvisor.com", "thumbtack.com", "houzz.com", "porch.com",
    "angieslist.com", "facebook.com", "linkedin.com", "google.com",
    "issuu.com", "achhd.org", "putnamcountyny.gov", "diamondcertified.org",
    "roofingdirect.com", "pacepdh.com",
}

# Words that indicate it's a company name not a person name
NOT_PERSON_WORDS = {
    "roofing", "contractor", "construction", "company", "corp", "inc",
    "llc", "solutions", "services", "systems", "group", "standard",
    "elite", "premier", "pro", "master", "expert", "owner", "restaurant",
    "world", "one", "all", "new", "central", "florida", "orlando",
    "miami", "ads", "below", "health",
}

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Search templates for finding business owners
OWNER_SEARCH_TEMPLATES = [
    'owner of "{company}" {location}',
    '"{company}" {location} owner president',
    'site:bbb.org "{company}" {location} owner',
    'site:linkedin.com "{company}" {location} owner founder',
    '"{company}" {location} "owned by" OR "founded by" OR "president" OR "owner"',
    '"{company}" contact owner principal',
]


def _get_domain(website: str) -> str:
    try:
        parsed = urlparse(website if website.startswith("http") else f"https://{website}")
        host = parsed.netloc.lower().replace("www.", "")
        return host
    except Exception:
        return website.lower().replace("www.", "").split("/")[0]


def _generate_permutations(first: str, last: str, domain: str) -> list[str]:
    """Generate common business email permutations."""
    f = first.lower().strip()
    l = last.lower().strip()
    if not f or not domain:
        return []

    perms = []
    if f:
        perms.append(f"{f}@{domain}")
    if f and l:
        perms += [
            f"{f}.{l}@{domain}",
            f"{f}{l}@{domain}",
            f"{f[0]}{l}@{domain}",
            f"{f[0]}.{l}@{domain}",
            f"{l}.{f}@{domain}",
            f"{l}{f[0]}@{domain}",
            f"{f}-{l}@{domain}",
        ]
    if l:
        perms.append(f"{l}@{domain}")

    # Remove duplicates while preserving order
    seen = set()
    result = []
    for p in perms:
        if p not in seen and len(p) < 80:
            seen.add(p)
            result.append(p)
    return result


async def _serp_search(query: str, num: int = 5) -> list[dict]:
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERPAPI_KEY,
        "num": num,
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


def _extract_person_names(text: str) -> list[tuple[str, str]]:
    """Extract likely person names from text."""
    # Common name patterns in business contexts
    patterns = [
        r'\b([A-Z][a-z]{1,15})\s+([A-Z][a-z]{1,20})\b(?:\s+(?:is|has|owns|founded|started|runs|operates|president|owner|principal|CEO|founder|director))',
        r'(?:owner|president|founder|principal|CEO|director|operator)[\s:,]+([A-Z][a-z]{1,15})\s+([A-Z][a-z]{1,20})',
        r'(?:owned by|founded by|operated by|run by)\s+([A-Z][a-z]{1,15})\s+([A-Z][a-z]{1,20})',
        r'([A-Z][a-z]{1,15})\s+([A-Z][a-z]{1,20})[\s,]+(?:owner|president|founder|CEO|principal)',
        r'Contact\s+([A-Z][a-z]{1,15})\s+([A-Z][a-z]{1,20})',
    ]

    names = []
    seen = set()
    for pattern in patterns:
        for m in re.finditer(pattern, text):
            first, last = m.group(1), m.group(2)
            key = f"{first}{last}".lower()
            if key not in seen and first not in {"The", "Our", "This", "Your", "All", "For"}:
                seen.add(key)
                names.append((first, last))

    # Filter out names where first or last name is a company word
    real_names = []
    for first, last in names:
        if first.lower() in NOT_PERSON_WORDS or last.lower() in NOT_PERSON_WORDS:
            continue
        if len(first) < 2 or len(last) < 2:
            continue
        real_names.append((first, last))

    return real_names


async def find_owner_name(company: str, location: str, website: str, log_cb=None) -> tuple[str, str]:
    """Search Google to find the business owner's name."""
    async def log(msg):
        if log_cb: await log_cb(msg)

    all_names = []

    for template in OWNER_SEARCH_TEMPLATES[:4]:  # Limit queries
        query = template.replace("{company}", company).replace("{location}", location)
        results = await _serp_search(query, num=5)

        for r in results:
            text = f"{r.get('title', '')} {r.get('snippet', '')}"
            names = _extract_person_names(text)
            all_names.extend(names)

        if all_names:
            break

        await asyncio.sleep(0.3)

    if all_names:
        # Return most common name
        from collections import Counter
        counts = Counter([f"{f} {l}" for f, l in all_names])
        best = counts.most_common(1)[0][0]
        first, last = best.split(" ", 1)
        await log(f"  Found owner: {first} {last}")
        return first, last

    return "", ""


async def verify_email_google(email: str) -> bool:
    """Check if an email appears anywhere in Google's index."""
    # Skip if the email domain is a directory site
    email_domain = email.split("@")[1].lower() if "@" in email else ""
    if any(email_domain == d or email_domain.endswith("." + d) for d in DIRECTORY_DOMAINS):
        return False
    results = await _serp_search(f'"{email}"', num=3)
    return len(results) > 0


async def verify_email_smtp(email: str) -> bool:
    """
    Verify email exists via SMTP without sending.
    Checks MX records then attempts RCPT TO handshake.
    """
    domain = email.split("@")[1]

    try:
        # Get MX records
        mx_records = dns.resolver.resolve(domain, "MX")
        mx_host = str(sorted(mx_records, key=lambda r: r.preference)[0].exchange)

        # SMTP handshake
        with smtplib.SMTP(timeout=10) as smtp:
            smtp.connect(mx_host, 25)
            smtp.helo("verify.com")
            smtp.mail("verify@verify.com")
            code, _ = smtp.rcpt(email)
            return code == 250
    except Exception:
        return False


async def find_and_verify_owner_email(
    company: str,
    website: str,
    location: str = "",
    existing_email: str = "",
    log_cb=None
) -> dict:
    """
    Full pipeline: find owner → generate permutations → verify.
    Returns dict with first_name, last_name, email, verified.
    """
    async def log(msg):
        if log_cb: await log_cb(msg)

    domain = _get_domain(website)
    result = {
        "first_name": "",
        "last_name": "",
        "email": existing_email,
        "email_verified": False,
        "job_title": "Owner",
    }

    if not domain:
        return result

    # Step 1: Find owner name
    await log(f"  Finding owner of {company}...")
    first, last = await find_owner_name(company, location, website, log_cb)

    if first:
        result["first_name"] = first
        result["last_name"] = last

    # Step 2: Generate email permutations
    if first and domain:
        permutations = _generate_permutations(first, last, domain)
        await log(f"  Generated {len(permutations)} email permutations for {first} {last}")

        # Step 3: Verify via Google (fast, no SMTP needed)
        for perm in permutations[:6]:  # Check top 6
            await log(f"  Checking {perm}...")
            if await verify_email_google(perm):
                await log(f"  ✅ Verified via Google: {perm}")
                result["email"] = perm
                result["email_verified"] = True
                return result
            await asyncio.sleep(0.2)

        # Step 4: SMTP verify if no Google hit
        for perm in permutations[:3]:
            try:
                if await verify_email_smtp(perm):
                    await log(f"  ✅ Verified via SMTP: {perm}")
                    result["email"] = perm
                    result["email_verified"] = True
                    return result
            except Exception:
                pass

        # Return best guess even if unverified
        if permutations:
            result["email"] = result["email"] or permutations[0]
            await log(f"  Best guess: {permutations[0]} (unverified)")

    return result
