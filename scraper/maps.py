"""
Google Maps scraper via SerpApi — finds businesses by industry + location,
then enriches each with website contact extraction.
"""

import asyncio
import httpx
from config import SERPAPI_KEY
from scraper.engine import _fetch_site_text, _regex_emails
from llm.groq_client import extract_contacts_enhanced, classify_industry


async def search_maps(industry: str, location: str, limit: int = 20, log_cb=None) -> list[dict]:
    async def log(msg):
        if log_cb: await log_cb(msg)

    await log(f"Searching Google Maps: {industry} in {location}...")

    # Call SerpApi Google Maps
    params = {
        "engine": "google_maps",
        "q": f"{industry} {location}",
        "type": "search",
        "api_key": SERPAPI_KEY,
        "num": min(limit, 20),
    }

    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get("https://serpapi.com/search", params=params)
            data = r.json()
    except Exception as e:
        await log(f"SerpApi error: {e}")
        return []

    raw = data.get("local_results", [])
    await log(f"Got {len(raw)} businesses from Google Maps")

    results = []
    for biz in raw[:limit]:
        name = biz.get("title", "")
        website = biz.get("website", "")
        phone = biz.get("phone", "")
        address = biz.get("address", "")
        rating = biz.get("rating", "")
        reviews = biz.get("reviews", "")
        category = biz.get("type", industry)

        await log(f"Enriching {name}...")

        # Scrape website for contacts
        first_name = last_name = email = job_title = description = employee_count = ""

        if website:
            try:
                # Clean URL - strip UTM params
                clean_url = website.split("?")[0].rstrip("/")
                text = await asyncio.wait_for(_fetch_site_text(clean_url), timeout=20)
                if text:
                    from urllib.parse import urlparse
                    domain = urlparse(clean_url).netloc.replace("www.", "")
                    emails = _regex_emails(text, domain)
                    email = emails[0] if emails else ""
                    try:
                        llm = await asyncio.wait_for(
                            extract_contacts_enhanced(name, text, domain_hint=domain),
                            timeout=15
                        )
                        first_name = llm.get("first_name", "")
                        last_name = llm.get("last_name", "")
                        job_title = llm.get("job_title", "")
                        description = llm.get("description", "")
                        employee_count = llm.get("employee_count", "")
                        if not email and llm.get("emails"):
                            email = llm["emails"][0]
                    except Exception:
                        pass
            except Exception as e:
                await log(f"  enrichment error: {e}")

        industry_tag = await classify_industry(name, description, website) if description else category

        await log(f"  ✓ {name} | {phone} | {email or 'no email'}")

        results.append({
            "company": name,
            "website": website,
            "phone": phone,
            "address": address,
            "industry": industry_tag,
            "employee_count": employee_count,
            "description": description,
            "country": "US",
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "job_title": job_title,
            "linkedin_url": "",
            "signal": f"⭐ {rating} ({reviews} reviews)" if rating else "",
            "relevance_score": float(rating) * 10 if rating else 0.0,
            "relevance_reason": f"{reviews} Google reviews",
            "source_url": website,
        })

        await asyncio.sleep(0.5)

    return results
