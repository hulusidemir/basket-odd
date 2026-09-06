"""Shared DOM reader for the current match's basketball period scoreboard."""

QUARTER_SCORES_JS = r"""scoreValue => {
    const scoreMatch = String(scoreValue || '').match(/^\s*(\d+)\s*[-–]\s*(\d+)\s*$/);
    if (!scoreMatch) return {};
    const expected = scoreMatch.slice(1).map(Number);
    const text = el => (el.textContent || '').replace(/\s+/g, ' ').trim();
    const leaves = el => [...el.querySelectorAll('*')].filter(child => !child.children.length);
    const numeric = el => leaves(el).filter(child =>
        /^\d{1,3}$/.test(text(child)) &&
        !child.closest('.name, .teamName, [class*="teamName"], [class*="team-name"]')
    );
    const quarter = el => text(el).match(/^(?:Q([1-4])|([1-4])Q|([1-4])(?:st|nd|rd|th)(?:\s+Quarter)?)$/i);
    const valid = (home, away) => home.length > 0 && home.length === away.length &&
        home.length <= 4 && [...home, ...away].every(value => Number.isInteger(value) && value >= 0 && value <= 100) &&
        home.reduce((a, b) => a + b, 0) === expected[0] &&
        away.reduce((a, b) => a + b, 0) === expected[1];
    const result = (home, away, source) => ({home, away, source, quality: 100});

    // Read only the current match scoreboard, never names or unrelated body text.
    const roots = [...document.querySelectorAll('.scoresDetails, [class*="scoresDetails"], [class*="scoreDetail"]')];
    for (const root of roots) {
        // Mobile layouts may store a column (label, home, away) per quarter.
        const columns = new Map();
        for (const label of leaves(root).filter(quarter)) {
            const match = quarter(label);
            const index = Number(match[1] || match[2] || match[3]);
            for (let node = label.parentElement; node && root.contains(node); node = node.parentElement) {
                if (leaves(node).filter(quarter).length !== 1) break;
                const cells = numeric(node);
                if (cells.length === 2) {
                    columns.set(index, cells.map(cell => Number(text(cell))));
                    break;
                }
            }
        }
        if (columns.size) {
            const indexes = [...columns.keys()].sort((a, b) => a - b);
            if (indexes.every((value, index) => value === index + 1)) {
                const home = indexes.map(index => columns.get(index)[0]);
                const away = indexes.map(index => columns.get(index)[1]);
                if (valid(home, away)) return result(home, away, 'scoreboard_columns');
            }
        }

        // Team rows can place the total before or after their period cells.
        const rows = [...root.querySelectorAll('tr, [role="row"], div, li')]
            .map(el => ({el, cells: numeric(el)}))
            .filter(row => row.cells.length >= 2 && row.cells.length <= 6);
        const periods = (cells, total) => {
            const values = cells.map(cell => Number(text(cell)));
            const options = [];
            if (values[values.length - 1] === total) options.push(values.slice(0, -1));
            if (values[0] === total) options.push(values.slice(1));
            return options.filter(values => values.slice(4).every(value => value === 0))
                .map(values => values.slice(0, 4))
                .find(values => values.length && values.reduce((a, b) => a + b, 0) === total);
        };
        for (let homeIndex = 0; homeIndex < rows.length; homeIndex++) {
            const homeRow = rows[homeIndex];
            const home = periods(homeRow.cells, expected[0]);
            if (!home) continue;
            for (const awayRow of rows.slice(homeIndex + 1)) {
                if (homeRow.el.contains(awayRow.el) || awayRow.el.contains(homeRow.el)) continue;
                const away = periods(awayRow.cells, expected[1]);
                if (away && valid(home, away)) return result(home, away, 'scoreboard_team_rows');
            }
        }
    }
    return {};
}"""
