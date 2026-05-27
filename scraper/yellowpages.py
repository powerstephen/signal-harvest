"""
Yellow Pages scraper — finds businesses by industry + location,
extracts contact details from each listing page.
"""

import asyncio
import re
from urllib.parse import urlencode, quote_plus

import httpx

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


async def _get(url: str) -> str:
    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15, verify=False) as c:
            r = await c.get(url)
            return r.text if r.status_code == 200 else ""
    except Exception:
        return ""


def _clean(text: str) -> str:
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', text)).strip()


async def search_yellowpages(industry: str, location: str, limit: int = 20, log_cb=None) -> list[dict]:
    """Search Yellow Pages and return enriched business listings."""
    
    async def log(msg):
        if log_cb: await log_cb(msg)

    results = []
    page = 1

    while len(results) < limit:
        params = {"search_terms": industry, "geo_location_terms": location, "page": page}
        url = "https://www.yellowpages.com/search?" + urlencode(params)
        await log(f"Searching YP: {industry} in {location} (page {page})...")

        html = await _get(url)
        if not html:
            await log("No response from Yellow Pages")
            break

        # Extract listing blocks
        listings = re.findall(
            r'<div class="info">(.*?)</div>\s*</div>\s*</div>',
            html, re.DOTALL
        )

        if not listings:
            # Try alternative pattern
            listings = re.findall(
                r'class="[^"]*result[^"]*"[^>]*>(.*?)</div>\s*</div>',
                html, re.DOTALL
            )

        await log(f"Found {len(listings)} raw listings on page {page}")

        if not listings:
            break

        for block in listings:
            if len(results) >= limit:
                break

            # Business name
            name_m = re.search(r'<a[^>]*class="[^"]*business-name[^"]*"[^>]*>(.*?)</a>', block, re.DOTALL)
            name = _clean(name_m.group(1)) if name_m else ""

            # Phone
            phone_m = re.search(r'<div class="phones[^"]*">(.*?)</div>', block, re.DOTALL)
            phone = _clean(phone_m.group(1)) if phone_m else ""

            # Address
            street_m = re.search(r'<span[^>]*class="[^"]*street-address[^"]*"[^>]*>(.*?)</span>', block, re.DOTALL)
            city_m = re.search(r'<span[^>]*class="[^"]*locality[^"]*"[^>]*>(.*?)</span>', block, re.DOTALL)
            street = _clean(street_m.group(1)) if street_m else ""
            city = _clean(city_m.group(1)) if city_m else ""
            address = f"{street}, {city}".strip(", ")

            # Website URL
            web_m = re.search(r'<a[^>]*class="[^"]*track-visit-website[^"]*"[^>]*href="([^"]+)"', block)
            website = web_m.group(1) if web_m else ""

            # Listing URL for detail scrape
            listing_m = re.search(r'<a[^>]*class="[^"]*business-name[^"]*"[^>]*href="(/[^"]+)"', block)
            listing_url = f"https://www.yellowpages.com{listing_m.group(1)}" if listing_m else ""

            # Category
            cat_m = re.search(r'<div class="categories">(.*?)</div>', block, re.DOTALL)
            category = _clean(cat_m.group(1)) if cat_m else industry

            if not name:
                continue

            result = {
                "company": name,
                "website": website,
                "phone": phone,
                "address": address,
                "category": category,
                "listing_url": listing_url,
                "email": "",
                "first_name": "",
                "last_name": "",
                "job_title": "",
                "industry": category or industry,
                "employee_count": "",
                "description": "",
                "country": "US",
                "signal": f"YP: {industry} in {location}",
                "relevance_score": 0.0,
                "relevance_reason": "",
                "source_url": listing_url or website,
            }
            results.append(result)

        page += 1
        if page > 3:
            break
        await asyncio.sleep(1)

    # Enrich with detail page scraping
    await log(f"Enriching {len(results)} listings...")
    enriched = []
    for r in results:
        if r["listing_url"]:
            try:
                detail = await _scrape_listing(r["listing_url"])
                r.update({k: v for k, v in detail.items() if v})
            except Exception:
                pass
        enriched.append(r)
        await log(f"  ✓ {r['company']} | {r['phone']} | {r.get('email', '')}")
        await asyncio.sleep(0.5)

    return enriched


async def _scrape_listing(url: str) -> dict:
    """Scrape a YP listing detail page for email and more info."""
    html = await _get(url)
    if not html:
        return {}

    result = {}

    # Email
    emails = EMAIL_RE.findall(html)
    valid_emails = [e for e in emails if not any(x in e for x in ["example", "sentry", "png", "jpg", "css", "js"])]
    if valid_emails:
        result["email"] = valid_emails[0]

    # Website from detail page
    web_m = re.search(r'href="(https?://(?!www\.yellowpages)[^"]+)"[^>]*>[^<]*website[^<]*<', html, re.IGNORECASE)
    if web_m:
        result["website"] = web_m.group(1)

    # Description
    desc_m = re.search(r'<div[^>]*class="[^"]*business-description[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)
    if desc_m:
        result["description"] = _clean(desc_m.group(1))[:300]

    # Year established / employee count hints
    years_m = re.search(r'(\d{4})\s*(?:established|founded|est\.)', html, re.IGNORECASE)
    if years_m:
        year = int(years_m.group(1))
        result["signal"] = f"Est. {year}"

    return result
