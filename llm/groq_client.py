import json
import re
from typing import Optional
from groq import AsyncGroq
from config import GROQ_API_KEY, GROQ_MODEL

_client: Optional[AsyncGroq] = None

SAAS_INDUSTRIES = [
    "HR Tech", "Sales Tech", "RevOps", "DevTools", "FinTech", "LegalTech",
    "MarTech", "CS Tech", "Security", "Analytics", "Product Analytics",
    "Workflow Automation", "EdTech", "HealthTech", "PropTech", "ERP",
    "Supply Chain", "E-commerce Tech", "AdTech", "InsurTech", "Other SaaS",
    "Professional Services", "Consulting", "Agency", "Home Services",
    "Construction", "Roofing", "HVAC", "Legal", "Dental", "Medical", "Other"
]


def client() -> AsyncGroq:
    global _client
    if _client is None:
        if not GROQ_API_KEY:
            raise RuntimeError("GROQ_API_KEY missing")
        _client = AsyncGroq(api_key=GROQ_API_KEY)
    return _client


def _extract_json(text: str):
    text = text.strip()
    fence = re.search(r"```(?:json)?\s*(.+?)\s*```", text, re.DOTALL)
    if fence:
        text = fence.group(1).strip()
    for opener, closer in [("[", "]"), ("{", "}")]:
        i = text.find(opener)
        if i == -1:
            continue
        depth = 0
        for j in range(i, len(text)):
            if text[j] == opener:
                depth += 1
            elif text[j] == closer:
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[i:j + 1])
                    except json.JSONDecodeError:
                        break
    return None


async def _chat(system: str, user: str, temperature: float = 0.2, max_tokens: int = 1024) -> str:
    resp = await client().chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return resp.choices[0].message.content or ""


async def extract_contacts_enhanced(company: str, page_text: str, domain_hint: str = "") -> dict:
    system = (
        "You are extracting B2B contact information from a company webpage. "
        "Focus on finding NAMED INDIVIDUALS, especially in sales, revenue, or leadership roles. "
        "Priority titles: CRO, VP Sales, Head of Sales, Sales Director, CEO, Founder, MD, COO. "
        "Split full names into first_name and last_name. "
        'Return JSON only: {"first_name": str, "last_name": str, "job_title": str, '
        '"emails": [str], "phones": [str], "linkedin_url": str, '
        '"employee_count": str, "description": str}. '
        "description = one punchy sentence about what the company does. "
        "Empty string if unknown. JSON only, no preamble."
    )
    user = (
        f"Company: {company}\n"
        f"Domain hint: {domain_hint}\n\n"
        f"Page content:\n{page_text[:6000]}"
    )
    raw = await _chat(system, user, temperature=0.0, max_tokens=600)
    parsed = _extract_json(raw)
    if isinstance(parsed, dict):
        return {
            "first_name": str(parsed.get("first_name", "") or ""),
            "last_name": str(parsed.get("last_name", "") or ""),
            "job_title": str(parsed.get("job_title", "") or ""),
            "emails": [e for e in parsed.get("emails", []) if isinstance(e, str)],
            "phones": [p for p in parsed.get("phones", []) if isinstance(p, str)],
            "linkedin_url": str(parsed.get("linkedin_url", "") or ""),
            "employee_count": str(parsed.get("employee_count", "") or ""),
            "description": str(parsed.get("description", "") or "")[:400],
        }
    return {"first_name": "", "last_name": "", "job_title": "", "emails": [], "phones": [], "linkedin_url": "", "employee_count": "", "description": ""}


async def classify_industry(company: str, description: str, website: str) -> str:
    industries_list = ", ".join(SAAS_INDUSTRIES)
    system = (
        "You classify companies into industry verticals. "
        f"Choose the single best fit from this list: {industries_list}. "
        'Return JSON only: {"industry": str}. JSON only.'
    )
    user = f"Company: {company}\nWebsite: {website}\nDescription: {description}"
    raw = await _chat(system, user, temperature=0.0, max_tokens=50)
    parsed = _extract_json(raw)
    if isinstance(parsed, dict) and "industry" in parsed:
        return str(parsed["industry"])
    return "Other"


async def score_company(company: str, website: str, snippet: str, icp_description: str, notes: str = "") -> dict:
    system = (
        "You score B2B sales leads for ICP fit. "
        "Score 0-100. Return JSON: {\"score\": int, \"reason\": str}. "
        "90-100 = strong fit; 60-80 = plausible; 30-50 = weak; 0-20 = no fit. JSON only."
    )
    extras = f"Additional notes: {notes}\n" if notes else ""
    user = (
        f"ICP / target: {icp_description}\n{extras}\n"
        f"Company: {company}\nURL: {website}\nSnippet: {snippet[:400]}"
    )
    raw = await _chat(system, user, temperature=0.1, max_tokens=150)
    parsed = _extract_json(raw)
    if isinstance(parsed, dict) and "score" in parsed:
        try:
            score = float(parsed["score"])
        except (TypeError, ValueError):
            score = 0.0
        return {"score": max(0.0, min(100.0, score)), "reason": str(parsed.get("reason", ""))[:300]}
    return {"score": 0.0, "reason": ""}


async def expand_search_queries(icp: str, country: str, notes: str = "", previous: list[str] | None = None) -> list[str]:
    avoid = ""
    if previous:
        avoid = "\n\nAvoid: " + "; ".join(previous[-20:])
    system = (
        "You generate B2B prospecting search queries that surface real company websites. "
        "Avoid directories, blogs, listicles. Mix sub-niches, locations, qualifiers. "
        "Return JSON array of strings. JSON only."
    )
    user = (
        f"ICP: {icp}\nCountry: {country or 'any'}\nNotes: {notes}{avoid}\n"
        "Return 8 diverse search queries as a JSON array."
    )
    raw = await _chat(system, user, temperature=0.5, max_tokens=600)
    parsed = _extract_json(raw)
    if isinstance(parsed, list):
        return [str(q).strip() for q in parsed if str(q).strip()][:8]
    return [f"{icp} {country}".strip()]
