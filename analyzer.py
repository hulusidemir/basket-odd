"""
analyzer.py — Gemini AI match analysis with Google Search grounding.
"""

import logging
from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Sen bağımsız bir profesyonel basketbol analisti ve deneyimli bir bahisçisin.
Sana bir basketbol maçının canlı barem (total over/under) verileri sunulacak.

Arka planda takımları, ligi, formu, sakatlıkları, hücum-savunma dengesini araştır.
AMA çıktın SADECE aşağıdaki formatta olsun, fazla yazma:

📊 Adil Baremim: [senin belirlediğin sayı]
📈 Canlı Barem: [verilen canlı barem]
🎯 Öneri: [ALT veya ÜST] — [1 cümle gerekçe]
📝 Yorum: [1-2 cümle kısa yorum, maç dinamiği/risk]

Bu kadar. Uzun analiz yazma. Araştırmanı arka planda yap ama çıktı kısa olsun.
Türkçe yaz. Tamamen bağımsız ol, kendi araştırmana güven."""


async def get_match_analysis(
    api_key: str,
    model: str,
    match_name: str,
    tournament: str,
    score: str,
    opening: float,
    inplay: float,
    diff: float,
    direction: str,
    status: str,
) -> str:
    """
    Calls Gemini with Google Search grounding to analyze a match anomaly.
    Returns analysis text or empty string on failure.
    """
    if not api_key:
        logger.warning("GEMINI_API_KEY not set, skipping analysis.")
        return ""

    prompt = f"""Maç: {match_name}
Turnuva/Lig: {tournament}
Mevcut Skor: {score or 'Bilinmiyor'}
Maç Durumu: {status or 'Canlı'}
(Not: Format "Q4 09:29" = 4. çeyrek, periyodun bitmesine 9 dk 29 sn kaldı. Basketbolda her çeyrek 10 veya 12 dakikadır. Q1=1.çeyrek, Q2=2.çeyrek, Q3=3.çeyrek, Q4=4.çeyrek, OT=uzatma, HT=devre arası)

Açılış Baremi (Total): {opening:.1f}
Güncel Canlı Barem: {inplay:.1f}
Fark: {abs(diff):.1f} puan

Bu maçı bağımsız olarak analiz et. Kendi araştırmanla takımları ve ligi değerlendir, adil bir barem belirle ve ALT/ÜST önerini sun."""

    try:
        client = genai.Client(api_key=api_key)
        response = await client.aio.models.generate_content(
            model=model,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                tools=[types.Tool(google_search=types.GoogleSearch())],
                temperature=0.2,
            ),
        )
        # Extract text safely — grounding responses may have different structure
        analysis = ""
        try:
            analysis = response.text or ""
        except (ValueError, AttributeError):
            # Fallback: extract from candidates/parts manually
            if response.candidates:
                parts = response.candidates[0].content.parts or []
                analysis = "".join(p.text for p in parts if hasattr(p, "text") and p.text)
        if not analysis:
            logger.warning("Gemini returned empty response for: %s", match_name)
            return ""
        logger.info("Gemini analysis received for: %s (%d chars)", match_name, len(analysis))
        return analysis
    except Exception as e:
        logger.error("Gemini analysis failed for %s: %s", match_name, e)
        return ""
