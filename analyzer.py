"""
analyzer.py — Gemini AI match analysis with Google Search grounding.
"""

import logging
from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Sen profesyonel bir basketbol analisti ve deneyimli bir bahisçisin.
Sana bir basketbol maçında tespit edilen barem (total over/under) anomalisi verilecek.

Görevin:
1. Takımları ve ligi internetten araştır — güncel form, son maç sonuçları, sakatlıklar, rotasyon haberleri
2. Bu ligdeki maçlarda ortalama toplam sayıyı değerlendir
3. Takımların hücum ve savunma güçlerini karşılaştır
4. Barem hareketinin olası nedenini açıkla (sakatlık, rotasyon, motivasyon vb.)
5. Maçın tahmini toplam sayısını ver
6. NET bir ALT veya ÜST önerisi yap, gerekçesiyle

Kurallar:
- Kısa ve öz yaz, laf salatası yapma
- Her cümle bilgi içersin
- Emoji kullan ama abartma
- Türkçe yaz
- Sana sunulan bilgilere bağlı kalma, örneğin bir maç sana alt önerisi olarak gelmiş ama sen bu maçın üst biteceğini düşünüyorsan tehlikeli olduğunu belirt. O bahisten uzak durulması gerektiğini belirt.
- Analizi sen kendin bil, uzun yazılar yazma, direkt önerini yap, net ol."""


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
Fark: {diff:+.1f} puan
Sinyal: {direction} ({'Barem yükseldi → ALT önerisi' if direction == 'ALT' else 'Barem düştü → ÜST önerisi'})

Bu anomaliyi analiz et. Takımları ve ligi araştır, güncel bilgilerle değerlendir."""

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
        analysis = response.text or ""
        logger.info("Gemini analysis received for: %s (%d chars)", match_name, len(analysis))
        return analysis
    except Exception as e:
        logger.error("Gemini analysis failed for %s: %s", match_name, e)
        return ""
