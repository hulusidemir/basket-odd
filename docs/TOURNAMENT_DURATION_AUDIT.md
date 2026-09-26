# Tournament duration audit — 2026-09-26

Source: read-only union of distinct `alerts.tournament` (past and active alerts) and `upcoming_matches.tournament` in the local `basketball.db`. Counts are rows, not unique games. Runtime data may change after this snapshot. No team names or country-level duration inference were used.

All 94 observed tournament strings currently resolve to **40 minutes / 4 × 10**. None resolves to 48 minutes or 2 × 20. No PBA, NBA, NBA Summer League, or literal `NCAA` tournament name occurs in this DB snapshot. The full Philippines NCAA name resolves to 4 × 10 because it does not contain the literal acronym.

Evidence for independently verified 40-minute formats: [FIBA Official Rules, Article 8.1](https://assets.fiba.basketball/image/upload/documents-corporate-fiba-official-rules-2024-v10a.pdf) (FIBA Intercontinental Cup and FIBA Europe Cup); [EuroLeague bylaws, Article 23](https://ftpserver.euroleague.net/general/2025_26_EuroLeague_Bylaws.pdf) apply FIBA rules to EuroLeague games; [WNBA FAQ](https://www.wnba.com/faq) states 10-minute quarters. The other names are **not independently verified** here: 40 minutes is the existing fallback, not an established league rule. No observed quarter clock in `alerts.status` exceeded 10 minutes; this absence alone cannot prove a 10-minute quarter.

Names especially requiring caution: `Club Friendship` (generic exhibition label), `National Basketball League` (country unspecified), `Korean Basketball League Open Match Day` (event-specific format), and the Philippines MPBL/UAAP/NCAA strings (not covered by the PBA rule). None has evidence here supporting a 48-minute override.

| Tournament (exact DB value) | Alerts | Upcoming | Current regulation | Current periods | Evidence status |
|---|---:|---:|---:|---|---|
| Algeria National A Women | 3 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Algeria Super Division | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Asian Games - Men's Basketball | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Asian Games - Women's Basketball | 12 | 0 | 40 | 4 × 10 | Fallback; unverified |
| B.League Next | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| B.League One | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| B.League Premier | 9 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Bahrain Cup | 6 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Basketball Bundesliga | 13 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Basketball Champions League | 5 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Belgian Basketball League 2 | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Belgium Basketball Cup | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Brazil Campeonato FCB | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Brazil Campeonato Paulista U20 | 7 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Brazil Campeonato Women FCB | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Brazil Federação Paulista de Basquete | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Brazil LDB U22 | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Brazil Women's Paulista Basketball | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| British NBL Div 1 Trophy | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| British Super League Basketball | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Chile Liga Nacional Basketball | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Club Friendship | 77 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Copa del Rey de Baloncesto | 3 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Costa Rica Championship U24 | 5 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Costa Rica LSB Women | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Czech Liga1 Basketball League | 5 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Czech Women's Liga1 | 3 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Dameligaen | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Danish Basketball League | 6 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Dominican Republic TBS | 3 | 0 | 40 | 4 × 10 | Fallback; unverified |
| El Salvador Liga Mayor | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| El Salvador Women's Liga | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| England National Basketbal Division 1l League | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Estonia 1 Liiga | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Estonia and Latvia Basketball League | 5 | 0 | 40 | 4 × 10 | Fallback; unverified |
| EuroCup Women | 5 | 0 | 40 | 4 × 10 | Fallback; unverified |
| EuroLeague | 10 | 0 | 40 | 4 × 10 | Verified 40 |
| EuroLeague SuperCup | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| EuroLeague Women | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| European North Basketball League | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| FIBA Europe Cup | 2 | 0 | 40 | 4 × 10 | Verified 40 |
| FIBA Intercontinental Cup | 11 | 0 | 40 | 4 × 10 | Verified 40 |
| Finland Cup | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Firi-ligaen | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| France Nationale 1 | 8 | 0 | 40 | 4 × 10 | Fallback; unverified |
| France Super Cup | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Ghana ABL | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Hungary. NB ll | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Israel Basketball Cup | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Israel Basketball League Cup | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Italy Super Cup | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Kenya Premier League | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Kenya Women’s Premier League | 3 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Korean Basketball League Open Match Day | 3 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Korean University Basketball League | 3 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Latvia Cup | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Lietuvos Krepsinio Lyga | 8 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Liga Nacional de Baloncesto Profesional | 16 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Ligue Nationale de Basket Pro A | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Ligue Nationale de Basket Pro B | 3 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Mexico ABE League | 5 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Mexico ABE League Women | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Mexico CIBACOPA | 9 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Mozambique LMB | 7 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Narodni Basketbalova Liga | 14 | 0 | 40 | 4 × 10 | Fallback; unverified |
| National Basketball League | 12 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Nemzeti Bajnokság I/A | 5 | 0 | 40 | 4 × 10 | Fallback; unverified |
| New Zealand Basketball League Womens | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Novo Basquete Brasil | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Paraguay Primera | 9 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Philippines MPBL | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Philippines National Collegiate Athletic Association | 8 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Philippines University Athletic Association | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Poland 1st Division | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Puerto Rico Superior Nacional women | 10 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Salvador Liga Mayor Basketball | 10 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Singapore NBL Division 1 | 3 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Slovenska Basketbalova Liga | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Spain Basketball Supercopa | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Spain Primera FEB | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Superettan Basket | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Superliga | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Svenska Basketligan | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Turkey Super Cup | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Turkish Basketball First League | 7 | 0 | 40 | 4 × 10 | Fallback; unverified |
| UAE Federation Cup | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Uruguay Liga de Ascenso | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Uruguay Metro League | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Uruguay Tercera de Ascenso | 8 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Uruguay Women's Championship | 1 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Vietnam VBA | 3 | 0 | 40 | 4 × 10 | Fallback; unverified |
| VTB United League Supercup | 4 | 0 | 40 | 4 × 10 | Fallback; unverified |
| Women's National Basketball Association | 4 | 0 | 40 | 4 × 10 | Verified 40 |
| Zenska Basketbalova Liga | 2 | 0 | 40 | 4 × 10 | Fallback; unverified |

Diagnostic decision: `match_state.py` has no existing logger, and its fallback covers nearly every observed tournament. Emitting `UNKNOWN_DURATION_FORMAT` there would label known 40-minute competitions as unknown on every call. No diagnostic was added, and no signal path was changed.
