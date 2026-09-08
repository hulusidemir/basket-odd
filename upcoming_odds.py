"""Strict total-market DOM reader used only by upcoming matches."""

TOTAL_MARKET_JS = r"""() => {
    const text = el => (el?.innerText || '').replace(/\s+/g, ' ').trim();
    const totalLabel = value => /^(?:total(?: points)?|o\s*\/\s*u|over\s*[/&-]?\s*under)$/i.test(value);
    const locked = el => !el || /lock|suspend|unavail/i.test(el.innerHTML || '')
        || /^[-–—]+$/.test(text(el));
    const line = el => {
        if (locked(el)) return null;
        // Read distinct numeric cells; never scan a whole bookmaker's text.
        const leaves = [el, ...el.querySelectorAll('*')].filter(node => !node.children.length);
        const values = leaves.map(text).filter(value => /^(?:[ou]\s*)?\d+(?:\.\d+)?$/i.test(value))
            .map(value => Number(value.replace(/^[ou]\s*/i, '')))
            .filter(value => Number.isFinite(value) && value >= 100 && value <= 400);
        const unique = [...new Set(values)];
        return unique.length === 1 ? unique[0] : null;
    };
    const empty = {opening:null, prematch:null, inplay:null, bookmaker:'',
        market_verified:false, odds_source:''};
    for (const market of document.querySelectorAll('.oddsContent')) {
        if (!totalLabel(text(market.querySelector('.oddsType')))) continue;
        for (const [index, box] of [...market.querySelectorAll('.oddsBoxContent')].entries()) {
            const opening = line(box.querySelector('.border1'));
            const prematch = line(box.querySelector('.border2'));
            if (opening === null && prematch === null) continue;
            const company = box.closest('.oddsBox') || box.parentElement;
            const bookmaker = text(company?.querySelector('.companyName, .company, .name'))
                || company?.querySelector('img')?.getAttribute('alt') || `row-${index + 1}`;
            const candidate = {opening, prematch, inplay:line(box.querySelector('.border3')),
                bookmaker, market_verified:true, odds_source:'mobile_total_rows_v2'};
            if (opening !== null) return candidate;
            if (!empty.odds_source) Object.assign(empty, candidate);
        }
    }
    return empty;
}"""
