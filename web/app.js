const { h, render } = preact;
const { useEffect, useState, useRef } = preactHooks;

async function fetchEvents(day = null) {
  const url = day ? `/api/events?day=${encodeURIComponent(day)}` : "/api/events";
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch events: ${res.status}`);
  return res.json();
}

async function fetchStates(ids = []) {
  if (!ids.length) return { count: 0, states: [] };
  const url = `/api/states?ids=${encodeURIComponent(ids.join(","))}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch states: ${res.status}`);
  return res.json();
}

const POLL_SECS = 30; // states poll interval
const ODDS_POLL_SECS = 10; // odds poll interval
const QUOTES_POLL_SECS = 5; // quotes poll interval

function EventsTable() {
  const [state, setState] = useState({ loading: true, error: null, data: { count: 0, events: [] } });
    const [selectedDay, setSelectedDay] = useState(null); // null => today
    const [viewMode, setViewMode] = useState('all'); // 'all' | 'betted' | 'bettable' | 'starred'
    function ymd(offsetDays = 0) {
      const d = new Date();
      d.setDate(d.getDate() + offsetDays);
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, '0');
      const day = String(d.getDate()).padStart(2, '0');
      return `${y}-${m}-${day}`;
    }
  const [statesMap, setStatesMap] = useState({}); // id -> { scores_current, match_time, state }
  const [oddsMap, setOddsMap] = useState({}); // market_id -> contract_id -> price entry
  const [quotesMap, setQuotesMap] = useState({}); // contract_id -> { best_offer_bps, best_bid_bps, ... }
  const [toast, setToast] = useState(null); // { text, onUndo }
  const toastTimerRef = useRef(null);
  const [marksByDate, setMarksByDate] = useState({}); // dateKey -> eventId -> { maybe: bool, bet: bool }
  const idsRef = useRef([]);
  const eventsRef = useRef([]);
  const marketIdsRef = useRef([]);
  const contractIdsRef = useRef([]);
  const [expanded, setExpanded] = useState({}); // eventId -> bool
  const [insightsMap, setInsightsMap] = useState({}); // eventId -> insights payload
  // Visibility-driven insights fetch
  const observerRef = useRef(null);
  const seenRef = useRef(new Set()); // eventIds already observed (to avoid extra work)
  const insightsMapRef = useRef({});
  useEffect(() => { insightsMapRef.current = insightsMap; }, [insightsMap]);
  useEffect(() => {
    return () => { if (observerRef.current) { try { observerRef.current.disconnect(); } catch {} } };
  }, []);
  function onCardMountVisible(eid, node) {
    if (!node || !eid) return;
    if (!observerRef.current) {
      try {
        observerRef.current = new IntersectionObserver((entries) => {
          entries.forEach(entry => {
            const id = String(entry.target.dataset.eid || "");
            if (!id) return;
            if (entry.isIntersecting) {
              // Mark seen
              if (!seenRef.current.has(id)) seenRef.current.add(id);
              // Fetch insights only if not already loaded
              if (!insightsMapRef.current[id]) {
                fetch(`/api/analytics/match-insights?ids=${encodeURIComponent(id)}`)
                  .then(res => res.ok ? res.json() : Promise.reject(new Error("fetch insights failed")))
                  .then(data => {
                    const item = (data.results || []).find(r => String(r.event_id) === id) || null;
                    setInsightsMap(prev => ({ ...prev, [id]: item }));
                  })
                  .catch(() => {});
              }
            }
          });
        }, { root: null, threshold: 0.2 });
      } catch {}
    }
    try { node.dataset.eid = String(eid); } catch {}
    try { observerRef.current && observerRef.current.observe(node); } catch {}
  }
  function onCardUnmountVisible(eid, node) {
    if (observerRef.current && node) {
      try { observerRef.current.unobserve(node); } catch {}
    }
  }
  // Odds-highlevel state removed per request
  // No need to track ended timestamps client-side; we'll use API's actual_end_datetime

  function eligibleIds(events) {
    const now = Date.now();
    const leadMs = 60 * 60 * 1000; // include 60 minutes before start
    const pastWindowMs = 2 * 60 * 60 * 1000; // keep polling up to 2 hours after scheduled start
    return (events || [])
      .filter(e => {
        // Always include events already marked live
        const evState = (e && e.state) ? String(e.state).toLowerCase() : "";
        if (evState === "live") return true;
        const dt = e && e.start_datetime ? new Date(e.start_datetime).getTime() : null;
        if (!dt || Number.isNaN(dt)) return false;
        return now >= (dt - leadMs) && now <= (dt + pastWindowMs);
      })
      .map(e => e.id)
      .filter(Boolean);
  }

  useEffect(() => {
    let cancelled = false;
    setState(s => ({ ...s, loading: true, error: null }));
    fetchEvents(selectedDay)
      .then(data => {
        if (cancelled) return;
        setState({ loading: false, error: null, data });
        const ids = eligibleIds(data.events);
        idsRef.current = ids;
        eventsRef.current = data.events || [];
        // Collect market and contract IDs for odds/quotes
        const mids = [];
        const cids = [];
        (data.events || []).forEach(e => {
          if (e.winner_market_id) mids.push(String(e.winner_market_id));
          if (e.correct_score_market_id) mids.push(String(e.correct_score_market_id));
          if (e.winner_contract_home_id) cids.push(String(e.winner_contract_home_id));
          if (e.winner_contract_draw_id) cids.push(String(e.winner_contract_draw_id));
          if (e.winner_contract_away_id) cids.push(String(e.winner_contract_away_id));
          if (e.correct_score_any_other_home_win_contract_id) cids.push(String(e.correct_score_any_other_home_win_contract_id));
          if (e.correct_score_any_other_away_win_contract_id) cids.push(String(e.correct_score_any_other_away_win_contract_id));
          if (e.correct_score_any_other_draw_contract_id) cids.push(String(e.correct_score_any_other_draw_contract_id));
          if (e.over_under_45_market_id) mids.push(String(e.over_under_45_market_id));
          if (e.over_45_contract_id) cids.push(String(e.over_45_contract_id));
        });
        marketIdsRef.current = Array.from(new Set(mids));
        contractIdsRef.current = Array.from(new Set(cids));
        if (ids.length) {
          fetchStates(ids)
            .then(st => {
              if (cancelled) return;
              const map = {};
              (st.states || []).forEach(s => { map[s.id] = s; });
              setStatesMap(map);
            })
            .catch(() => {});
        }
        // Initial odds + quotes fetch
        if (marketIdsRef.current.length) {
          console.debug("Initial fetchOdds", { marketIds: marketIdsRef.current, contractIds: contractIdsRef.current });
          fetchOdds(marketIdsRef.current, contractIdsRef.current)
            .then(o => setOddsMap(o.prices || {}))
            .catch(err => console.warn("fetchOdds error", err));
          if (contractIdsRef.current.length) {
            console.debug("Initial fetchQuotes", { marketIds: marketIdsRef.current, contractIds: contractIdsRef.current });
            fetchQuotes(marketIdsRef.current, contractIdsRef.current)
              .then(q => setQuotesMap(q.quotes || {}))
              .catch(err => console.warn("fetchQuotes error", err));
          }
        } else {
          console.info("No market IDs found on initial load; odds/quotes disabled for now.");
        }
      })
      .catch(err => !cancelled && setState({ loading: false, error: err, data: { count: 0, events: [] } }));

    const timer = setInterval(() => {
      // Recompute eligibility as time passes so yet-to-start matches get picked up
      idsRef.current = eligibleIds(eventsRef.current || []);
      const ids = idsRef.current || [];
      if (!ids.length) return;
      fetchStates(ids)
        .then(st => {
          const map = {};
          (st.states || []).forEach(s => { map[s.id] = s; });
          setStatesMap(map);
        })
        .catch(() => {});
    }, POLL_SECS * 1000);

    // Odds polling
    const oddsTimer = setInterval(() => {
      const mids = marketIdsRef.current || [];
      const cids = contractIdsRef.current || [];
      if (!mids.length) return;
      console.debug("Polling fetchOdds", { marketIds: mids, contractIds: contractIdsRef.current || [] });
      fetchOdds(mids, contractIdsRef.current || [])
        .then(o => setOddsMap(o.prices || {}))
        .catch(err => console.warn("fetchOdds error", err));
    }, ODDS_POLL_SECS * 1000);

    // Quotes polling
    const quotesTimer = setInterval(() => {
      const mids = marketIdsRef.current || [];
      const cids = contractIdsRef.current || [];
      if (!mids.length || !cids.length) return;
      console.debug("Polling fetchQuotes", { marketIds: mids, contractIds: cids });
      fetchQuotes(mids, cids)
        .then(q => setQuotesMap(q.quotes || {}))
        .catch(err => console.warn("fetchQuotes error", err));
    }, QUOTES_POLL_SECS * 1000);

    return () => { cancelled = true; clearInterval(timer); clearInterval(oddsTimer); clearInterval(quotesTimer); };
  }, [selectedDay]);

  // Odds-highlevel fetch removed per request

  // Persisted marks (v2): by date bucket; prune older than KEEP_DAYS
  const MARKS_STORAGE_KEY = "matchscreener_marks_v2";
  const MARKS_KEEP_DAYS = 14; // prune buckets older than 14 days
  function dateKeyToDate(dk) {
    try {
      const y = parseInt(String(dk).slice(0,4), 10);
      const m = parseInt(String(dk).slice(4,6), 10) - 1;
      const d = parseInt(String(dk).slice(6,8), 10);
      return new Date(y, m, d);
    } catch { return new Date(0); }
  }
  useEffect(() => {
    try {
      const raw = localStorage.getItem(MARKS_STORAGE_KEY);
      if (!raw) return;
      const obj = JSON.parse(raw);
      if (!obj || typeof obj !== 'object') return;
      // Prune old buckets
      const now = Date.now();
      const keepMs = MARKS_KEEP_DAYS * 24 * 60 * 60 * 1000;
      const pruned = {};
      Object.keys(obj || {}).forEach(dk => {
        const dt = dateKeyToDate(dk);
        if ((now - dt.getTime()) <= keepMs) pruned[dk] = obj[dk];
      });
      setMarksByDate(pruned);
    } catch {}
  }, []);
  useEffect(() => {
    try {
      localStorage.setItem(MARKS_STORAGE_KEY, JSON.stringify(marksByDate));
    } catch {}
  }, [marksByDate]);

  // Keep the idsRef in sync with current events to avoid stale closures in setInterval
  useEffect(() => {
    idsRef.current = eligibleIds(state.data.events || []);
    const mids = [];
    const cids = [];
    (state.data.events || []).forEach(e => {
      if (e.winner_market_id) mids.push(String(e.winner_market_id));
      if (e.correct_score_market_id) mids.push(String(e.correct_score_market_id));
      if (e.winner_contract_home_id) cids.push(String(e.winner_contract_home_id));
      if (e.winner_contract_draw_id) cids.push(String(e.winner_contract_draw_id));
      if (e.winner_contract_away_id) cids.push(String(e.winner_contract_away_id));
      if (e.correct_score_any_other_home_win_contract_id) cids.push(String(e.correct_score_any_other_home_win_contract_id));
      if (e.correct_score_any_other_away_win_contract_id) cids.push(String(e.correct_score_any_other_away_win_contract_id));
      if (e.correct_score_any_other_draw_contract_id) cids.push(String(e.correct_score_any_other_draw_contract_id));
      if (e.over_under_45_market_id) mids.push(String(e.over_under_45_market_id));
      if (e.over_under_45_contract_over_id) cids.push(String(e.over_under_45_contract_over_id));
    });
    eventsRef.current = state.data.events || [];
    marketIdsRef.current = Array.from(new Set(mids));
    contractIdsRef.current = Array.from(new Set(cids));
  }, [state.data.events]);

  if (state.loading) return h("p", { class: "small" }, "Loading events...");
  if (state.error) return h("p", { class: "small" }, `Error: ${state.error}`);

  const rows = state.data.events || [];
  let displayRows = rows;
  if (viewMode === 'betted') {
    displayRows = rows.filter(e => {
      const dk = dateKeyForEvent(e);
      const entry = (marksByDate[dk] || {})[String(e.id)] || { maybe: false, bet: false };
      if (!entry.bet) return false;
      const st = statesMap[e.id] || {};
      const mp = (st.match_period || '').toLowerCase();
      if (mp === 'full_time') return false; // exclude ended events
      return true; // show all betted matches not ended
    });
  } else if (viewMode === 'bettable') {
    displayRows = rows.filter(e => {
      const dk = dateKeyForEvent(e);
      const entry = (marksByDate[dk] || {})[String(e.id)] || { maybe: false, bet: false };
      if (entry.bet) return false; // exclude already betted
      const st = statesMap[e.id] || {};
      const mp = (st.match_period || '').toLowerCase();
      if (mp === 'full_time') return false; // exclude ended events
      // Only include matches that have NOT started yet and start within 120 minutes
      const dt = e.start_datetime ? new Date(e.start_datetime) : null;
      if (!dt) return false;
      const diffMs = dt.getTime() - Date.now();
      if (diffMs <= 0) return false; // already started or kickoff passed
      const withinTwoHours = diffMs <= (120 * 60 * 1000);
      return withinTwoHours;
    });
  } else if (viewMode === 'starred') {
    displayRows = rows.filter(e => {
      const dk = dateKeyForEvent(e);
      const entry = (marksByDate[dk] || {})[String(e.id)] || { maybe: false, bet: false };
      if (!entry.maybe) return false;
      const st = statesMap[e.id] || {};
      const mp = (st.match_period || '').toLowerCase();
      if (mp === 'full_time') return false; // exclude ended events
      return true;
    });
  }
  const emptyText = (() => {
    if (viewMode === 'betted') return "No betted matches";
    if (viewMode === 'bettable') return "No matches starting within 2 hours";
    if (viewMode === 'starred') return "No starred matches";
    return "No matches to show";
  })();
  const dayOptions = [
    { label: "Today", value: null },
    { label: "Tomorrow", value: ymd(1) },
    { label: "In 2 Days", value: ymd(2) },
  ];
  function dateKeyForEvent(e) {
    const dt = e && e.start_datetime ? new Date(e.start_datetime) : new Date();
    const y = dt.getFullYear();
    const m = String(dt.getMonth() + 1).padStart(2, '0');
    const d = String(dt.getDate()).padStart(2, '0');
    return `${y}${m}${d}`;
  }
  function showUndoToast(text, onUndo) {
    if (toastTimerRef.current) { clearTimeout(toastTimerRef.current); toastTimerRef.current = null; }
    setToast({ text, onUndo });
    toastTimerRef.current = setTimeout(() => { setToast(null); toastTimerRef.current = null; }, 6000);
  }

  function toggleMaybe(ev) {
    const dk = dateKeyForEvent(ev);
    const id = String(ev.id);
    setMarksByDate(prev => {
      const byDate = { ...(prev || {}) };
      const bucket = { ...(byDate[dk] || {}) };
      const entry = { ...(bucket[id] || { maybe: false, bet: false }) };
      entry.maybe = !entry.maybe;
      bucket[id] = entry;
      byDate[dk] = bucket;
      return byDate;
    });
  }
  function toggleBet(ev) {
    const dk = dateKeyForEvent(ev);
    const id = String(ev.id);
    setMarksByDate(prev => {
      const byDate = { ...(prev || {}) };
      const bucket = { ...(byDate[dk] || {}) };
      const entry = { ...(bucket[id] || { maybe: false, bet: false }) };
      entry.bet = !entry.bet;
      bucket[id] = entry;
      byDate[dk] = bucket;
      return byDate;
    });
  }

  function removeEvent(eventId) {
    setState(s => {
      const evts = s.data.events || [];
      const idx = evts.findIndex(ev => String(ev.id) === String(eventId));
      if (idx === -1) return s;
      const removed = evts[idx];
      const next = [...evts.slice(0, idx), ...evts.slice(idx + 1)];
      // Show toast with undo
      const labelHome = removed.home_name || removed.home || "";
      const labelAway = removed.away_name || removed.away || "";
      showUndoToast(`Removed ${labelHome} vs ${labelAway}`, () => {
        setState(s2 => {
          const cur = s2.data.events || [];
          // If already present, do nothing
          if (cur.some(ev => String(ev.id) === String(removed.id))) return s2;
          const restored = [...cur];
          const pos = Math.min(Math.max(idx, 0), restored.length);
          restored.splice(pos, 0, removed);
          return { ...s2, data: { ...s2.data, events: restored } };
        });
        setToast(null);
        if (toastTimerRef.current) { clearTimeout(toastTimerRef.current); toastTimerRef.current = null; }
      });
      return { ...s, data: { ...s.data, events: next } };
    });
  }

  return h(
    "div",
    { class: "cards" },
    [
      // Highlevel Odds UI removed per request
      h("div", { class: "analysis-toggle" }, [
        h("label", { class: "small", style: { marginRight: "6px" } }, "Date:"),
        h("select", {
          class: "analysis-btn",
          value: selectedDay || "",
          onChange: (e) => {
            const v = e.target.value;
            setSelectedDay(v ? v : null);
          }
        }, dayOptions.map(opt => h("option", { value: opt.value || "" }, opt.label))),
        (() => {
          const labels = { all: "Show All Matches", betted: "Show Betted", bettable: "Show Starting Soon (≤ 2h)", starred: "Show Starred" };
          return h("button", {
            class: "analysis-btn",
            onClick: () => setViewMode(m => (m === 'all' ? 'bettable' : (m === 'bettable' ? 'betted' : (m === 'betted' ? 'starred' : 'all')))),
            title: "Cycle view: All → Starting Soon → Betted → Starred"
          }, labels[viewMode]);
        })(),
      ]),
      (displayRows.length === 0) ? h("div", { class: "empty-state" }, emptyText) : null,
      ...displayRows.map(e => {
        const dk = dateKeyForEvent(e);
        const entry = (marksByDate[dk] || {})[String(e.id)] || { maybe: false, bet: false };
        return h(MatchCard, {
          key: e.id,
          e,
          st: statesMap[e.id] || {},
          oddsMap,
          quotesMap,
          maybeActive: !!entry.maybe,
          betActive: !!entry.bet,
          onToggleMaybe: () => toggleMaybe(e),
          onToggleBet: () => toggleBet(e),
          onRemove: () => removeEvent(e.id),
          expandedOpen: !!expanded[String(e.id)],
          insights: insightsMap[String(e.id)] || null,
          onToggleExpand: async () => {
            const id = String(e.id);
            setExpanded(prev => ({ ...prev, [id]: !prev[id] }));
            // Lazy-fetch insights on first expand
            if (!insightsMap[id]) {
              try {
                const res = await fetch(`/api/analytics/match-insights?ids=${encodeURIComponent(id)}`);
                if (res.ok) {
                  const data = await res.json();
                  const item = (data.results || []).find(r => String(r.event_id) === id) || null;
                  setInsightsMap(prev => ({ ...prev, [id]: item }));
                }
              } catch {}
            }
          },
          onCardMount: onCardMountVisible,
          onCardUnmount: onCardUnmountVisible,
        });
      }),
      toast ? h(Toast, { text: toast.text, onUndo: toast.onUndo, onClose: () => { setToast(null); if (toastTimerRef.current) { clearTimeout(toastTimerRef.current); toastTimerRef.current = null; } } }) : null
    ]
  );
}

// OddsHLPanel removed per request

// Friendly league dropdown options ("All" first)
// HL_LEAGUE_OPTIONS removed per request

function extractLeagueName(fullSlug) {
  if (!fullSlug || typeof fullSlug !== 'string') return null;
  // Extract league slug from pattern: /sport/football/leagues/{league-slug}/...
  const match = fullSlug.match(/\/leagues\/([^\/]+)/);
  if (!match) return null;
  // Convert slug to friendly name: "italy-serie-a" → "Italy Serie A"
  const slug = match[1];
  return slug
    .split('-')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

function formatClock(st) {
  const period = (st.match_period || "").toLowerCase();
  if (period === "half_time") return "HT";
  if (period === "extra_time_half_time") return "ET HT";
  if (period === "full_time") return "FT";
  if (period === "penalty_shootout") return "PEN";
  const mt = st.match_time || "";
  let minute = null;
  if (typeof mt === "string" && mt.includes(":")) {
    const parts = mt.split(":").map(p => parseInt(p, 10));
    if (parts.every(n => !Number.isNaN(n))) {
      if (parts.length === 3) minute = parts[0] * 60 + parts[1];
      else if (parts.length === 2) minute = parts[0];
      else if (parts.length === 1) minute = parts[0];
    }
  }
  if (minute == null) return "";
  const announced = (() => {
    const v = st.stoppage_time_announced;
    if (typeof v === 'boolean') return v;
    if (typeof v === 'string') return v.trim().toLowerCase() === 'true';
    return false;
  })();
  let stoppageMins = 0;
  const stp = st.stoppage_time || '';
  if (typeof stp === 'string' && stp.includes(':')) {
    const parts = stp.split(':').map(p => parseInt(p, 10));
    if (parts.every(n => !Number.isNaN(n))) {
      if (parts.length === 3) stoppageMins = parts[0] * 60 + parts[1];
      else if (parts.length === 2) stoppageMins = parts[0];
      else if (parts.length === 1) stoppageMins = parts[0];
    }
  }
  if (announced && stoppageMins > 0) return `${minute}' (+${stoppageMins}')`;
  return `${minute}'`;
}

function App() {
  return h(EventsTable, {});
}

render(h(App), document.getElementById("app"));
function MatchCard({ e, st, oddsMap, quotesMap, maybeActive, betActive, onToggleMaybe, onToggleBet, onRemove, expandedOpen, insights, onToggleExpand, onCardMount, onCardUnmount }) {
  const eid = e.id || "";
  const dt = e.start_datetime ? new Date(e.start_datetime) : null;
  const home = e.home_name || e.home || "";
  const away = e.away_name || e.away || "";
  const url = e.event_url || "#";
  const status = computeStatus(e, st);
  const hasScores = Array.isArray(st.scores_current) && st.scores_current.length === 2;
  const homeSc = hasScores ? String(st.scores_current[0]) : null;
  const awaySc = hasScores ? String(st.scores_current[1]) : null;
  const rootRef = useRef(null);
  useEffect(() => {
    if (typeof onCardMount === 'function' && rootRef.current) onCardMount(eid, rootRef.current);
    return () => { if (typeof onCardUnmount === 'function' && rootRef.current) onCardUnmount(eid, rootRef.current); };
  }, []);
  const score = insights && typeof insights.score === 'number' ? insights.score : null;
  function scoreClass(s) {
    if (typeof s !== 'number' || !Number.isFinite(s)) return '';
    if (s >= 90) return 'confidence-green';
    if (s >= 80) return 'confidence-yellow';
    if (s >= 50) return 'confidence-amber';
    return 'confidence-red';
  }
  // Enable stats toggle only if any stats exist
  const homeN = (insights && insights.home && typeof insights.home.n === 'number') ? insights.home.n : 0;
  const awayN = (insights && insights.away && typeof insights.away.n === 'number') ? insights.away.n : 0;
  const h2hN = (insights && insights.h2h && typeof insights.h2h.n === 'number') ? insights.h2h.n : 0;
  const statsAvailable = (homeN > 0) || (awayN > 0) || (h2hN > 0);
  const hasInsights = !!insights;
  const statsBtnText = hasInsights && !statsAvailable ? "No Stats Available" : (expandedOpen ? "Hide Stats" : "View Stats");
  const leagueName = extractLeagueName(e.full_slug);

  // Calculate Home Others and Away Others scores
  function calculateOthersScores(insights) {
    if (!insights || !insights.home || !insights.away) return { homeOthers: null, awayOthers: null };
    
    const home = insights.home;
    const away = insights.away;
    
    // Helper to normalize percentages (0-1) to 0-100 scale
    const pct = (val) => (typeof val === 'number' && Number.isFinite(val)) ? val * 100 : null;
    
    // Helper to normalize avg goals to 0-100 (4+ = 100, 0 = 0)
    const goalScore = (val) => (typeof val === 'number' && Number.isFinite(val)) ? Math.min(100, (val / 4) * 100) : null;
    
    // Helper to average non-null values
    const avg = (...values) => {
      const valid = values.filter(v => v !== null && Number.isFinite(v));
      return valid.length > 0 ? valid.reduce((sum, v) => sum + v, 0) / valid.length : null;
    };
    
    // HOME OTHERS SCORE
    // Home team offensive (at home venue)
    const homeOffensive = avg(
      goalScore(home.avg_goals_scored),           // 25%: overall avg goals
      pct(home.wins_others_pct),                  // 35%: win with 4+ goals
      pct(home.home_ht_2plus_pct),                // 20%: HT 2+ at home
      pct(home.ht_2plus_to_win_others_pct)        // 20%: HT 2+ → Win 4+
    );
    
    // Away team defensive weakness (when away)
    const awayDefensive = avg(
      goalScore(away.avg_goals_conceded),                      // 25%: overall avg conceded
      pct(away.losses_others_pct),                             // 35%: lose with 4+ conceded
      pct(away.away_ht_2plus_conceded_pct),                    // 20%: HT 2+ conceded away
      pct(away.away_ht_2plus_conceded_to_lost_others_pct)      // 20%: HT 2+ conceded → Lose 4+
    );
    
    const homeOthersScore = avg(homeOffensive, awayDefensive);
    
    // AWAY OTHERS SCORE
    // Away team offensive (when away)
    const awayOffensive = avg(
      goalScore(away.avg_goals_scored),
      pct(away.wins_others_pct),
      pct(away.away_ht_2plus_pct),                // at away venue
      pct(away.ht_2plus_to_win_others_pct)
    );
    
    // Home team defensive weakness (at home)
    const homeDefensive = avg(
      goalScore(home.avg_goals_conceded),
      pct(home.losses_others_pct),
      pct(home.home_ht_2plus_conceded_pct),                   // at home venue
      pct(home.home_ht_2plus_conceded_to_lost_others_pct)
    );
    
    const awayOthersScore = avg(awayOffensive, homeDefensive);
    
    // Invert scores: high score = high confidence to LAY (unlikely to happen)
    return {
      homeOthers: homeOthersScore !== null ? Math.round(100 - homeOthersScore) : null,
      awayOthers: awayOthersScore !== null ? Math.round(100 - awayOthersScore) : null
    };
  }
  
  const othersScores = calculateOthersScores(insights);
  
  // Track highlight mode for stats
  const [highlightMode, setHighlightMode] = useState(null); // null | 'home' | 'away'

  // Per-contract odds helper
  function oddsFor(marketId, contractId) {
    if (!marketId || !contractId) return { back: null, lay: null, last: null };
    const q = quotesMap[String(contractId)] || null;
    const o = oddsMap[String(marketId)] && oddsMap[String(marketId)][String(contractId)] ? oddsMap[String(marketId)][String(contractId)] : null;
    const back = q && (q.best_offer_decimal ?? bpsToDecimal(q.best_offer_bps)) || null;
    const lay = q && (q.best_bid_decimal ?? bpsToDecimal(q.best_bid_bps)) || null;
    let last = null;
    if (o && typeof o.last_decimal === 'number') last = o.last_decimal;
    else if (o && o.last_executed_price != null) {
      const raw = parseFloat(o.last_executed_price);
      if (Number.isFinite(raw) && raw > 0) last = 100 / raw;
    }
    return { back, lay, last };
  }

  const wm = e.winner_market_id ? String(e.winner_market_id) : null;
  const cs = e.correct_score_market_id ? String(e.correct_score_market_id) : null;
  const ou45 = e.over_under_45_market_id ? String(e.over_under_45_market_id) : null;
  const homeOdds = oddsFor(wm, e.winner_contract_home_id ? String(e.winner_contract_home_id) : null);
  const drawOdds = oddsFor(wm, e.winner_contract_draw_id ? String(e.winner_contract_draw_id) : null);
  const awayOdds = oddsFor(wm, e.winner_contract_away_id ? String(e.winner_contract_away_id) : null);
  const anyHomeOdds = oddsFor(cs, e.correct_score_any_other_home_win_contract_id ? String(e.correct_score_any_other_home_win_contract_id) : null);
  const anyAwayOdds = oddsFor(cs, e.correct_score_any_other_away_win_contract_id ? String(e.correct_score_any_other_away_win_contract_id) : null);
  const anyDrawOdds = oddsFor(cs, e.correct_score_any_other_draw_contract_id ? String(e.correct_score_any_other_draw_contract_id) : null);
  const over45Odds = oddsFor(ou45, e.over_45_contract_id ? String(e.over_45_contract_id) : null);

  function scoreClass(s) {
    if (typeof s !== 'number' || !Number.isFinite(s)) return '';
    if (s >= 80) return 'confidence-green';
    if (s >= 65) return 'confidence-yellow';
    if (s >= 50) return 'confidence-amber';
    return 'confidence-red';
  }

  return h("div", { class: "card", ref: rootRef, 'data-eid': eid },
    h("div", { class: "actions" },
      h("button", { class: `icon-btn maybe${maybeActive ? ' active' : ''}`, title: "Bookmark: Maybe Bet", onClick: onToggleMaybe, 'aria-pressed': !!maybeActive },
        h("svg", { class: "icon", viewBox: "0 0 24 24", xmlns: "http://www.w3.org/2000/svg" },
          h("polygon", { points: "12,2 15,9 22,9 16.5,13.5 18.5,21 12,16.5 5.5,21 7.5,13.5 2,9 9,9" })
        )
      ),
      h("button", { class: `icon-btn bet${betActive ? ' active' : ''}`, title: "Bookmark: Have Betted", onClick: onToggleBet, 'aria-pressed': !!betActive },
        h("svg", { class: "icon", viewBox: "0 0 24 24", xmlns: "http://www.w3.org/2000/svg" },
          h("circle", { cx: 12, cy: 12, r: 9 }),
          h("polyline", { points: "8,12 11,15 16,9", fill: "none" })
        )
      ),
      h("button", { class: "icon-btn remove", title: "Remove", onClick: onRemove },
        h("svg", { class: "icon", viewBox: "0 0 24 24", xmlns: "http://www.w3.org/2000/svg" },
          h("line", { x1: 5, y1: 5, x2: 19, y2: 19 }),
          h("line", { x1: 19, y1: 5, x2: 5, y2: 19 })
        )
      )
    ),
    h("div", { class: "row" },
      h("div", { class: "teams" },
        h("div", { class: "header-grid" }, [
          h("div", { class: "team-name home-name" }, home),
          hasScores ? h("span", { class: "score-badge home-score" }, homeSc) : h("span", { class: "score-badge empty home-score" }, ""),
          h("div", { class: "team-name away-name" }, away),
          hasScores ? h("span", { class: "score-badge away-score" }, awaySc) : h("span", { class: "score-badge empty away-score" }, ""),
        ]),
        leagueName ? h("div", { class: "league-name", title: "Competition" }, leagueName) : null
      )
    ),
    h("div", { class: `status ${status.class}` }, h("span", { class: "time" }, status.text)),
    h("div", { class: "odds" },
      h(OddCell, { label: home || "Home", odds: homeOdds }),
      h(OddCell, { label: "Draw", odds: drawOdds }),
      h(OddCell, { label: away || "Away", odds: awayOdds }),
      h(OddCell, { label: "Over 4.5", odds: over45Odds }),
      h(OddCell, { label: "Home Others", odds: anyHomeOdds, score: othersScores.homeOthers, scoreClass, onScoreClick: () => setHighlightMode(highlightMode === 'home' ? null : 'home') }),
      h(OddCell, { label: "Away Others", odds: anyAwayOdds, score: othersScores.awayOthers, scoreClass, onScoreClick: () => setHighlightMode(highlightMode === 'away' ? null : 'away') }),
    ),
    h("div", { class: "analysis-toggle" },
      h("button", { class: "analysis-btn", onClick: onToggleExpand, disabled: !statsAvailable, title: (!statsAvailable ? "Stats not available" : "") }, statsBtnText),
      (url && url !== "#") ? h("a", { class: "analysis-btn", href: url, target: "smarkets", rel: "noopener noreferrer", title: "Open in Smarkets", onClick: (ev) => { ev.preventDefault(); try { window.open(url, 'smarkets', 'noopener'); } catch {} } }, "Open in Smarkets") : null
    ),
    expandedOpen ? h(AnalysisPanel, { insights, highlightMode }) : null
  );
}

function OddCell({ label, odds, score, scoreClass, onScoreClick }) {
  function fmtDec(n) {
    if (typeof n !== 'number' || !Number.isFinite(n)) return '-';
    const dp = n < 3 ? 2 : 1;
    return n.toFixed(dp).replace(/\.00$/, '').replace(/\.?0+$/, '');
  }
  const backStr = fmtDec(odds.back);
  const layStr = fmtDec(odds.lay);
  const lastStr = fmtDec(odds.last);
    return h("div", { class: "odd" },
      h("div", { class: "label", style: "display: flex; align-items: center; justify-content: center; gap: 4px;" }, 
        h("span", {}, label),
        (score != null && scoreClass) ? h("span", { 
          class: `confidence-badge ${scoreClass(score)}`, 
          title: "Lay Confidence (click to highlight)", 
          style: "font-size: 10px; padding: 2px 5px; min-width: 22px; cursor: pointer;",
          onClick: onScoreClick
        }, String(score)) : null
      ),
      h("div", { class: "prices" },
        h("span", { class: "badge back", title: "Back" }, backStr),
        h("span", { class: "badge lay", title: "Lay" }, layStr),
      ),
      h("div", { class: "last" }, lastStr)
    );
}

function computeStatus(e, st) {
  const period = (st && st.match_period) ? String(st.match_period).toLowerCase() : '';
  if (period === 'full_time') return { class: 'ended', text: 'EVENT ENDED' };
  const liveClock = (st && st.clock_text) ? String(st.clock_text) : null;
  if (liveClock) return { class: 'live', text: liveClock };
  const dt = e.start_datetime ? new Date(e.start_datetime) : null;
  if (!dt) return { class: 'pre', text: '' };
  const diffMs = dt.getTime() - Date.now();
  const diffMin = Math.max(0, Math.round(diffMs / 60000));
  // If kickoff time has passed but state hasn't gone live yet, reflect that explicitly
  if (diffMs < -60 * 1000 && !liveClock) {
    return { class: 'pre', text: 'KICKOFF PASSED' };
  }
  if (diffMin <= 60) return { class: 'pre', text: `IN ${diffMin} MINUTES` };
  if (diffMin <= 120) return { class: 'pre', text: 'IN 1 HOUR' };
  const hours = Math.floor(diffMin / 60);
  return { class: 'pre', text: `IN ${hours} HOURS` };
}

// Helpers: odds/quotes fetch and formatting
async function fetchOdds(marketIds = [], contractIds = []) {
  if (!marketIds.length) return { count: 0, prices: {} };
  console.debug("fetchOdds()", { marketIds, contractIds });
  const params = new URLSearchParams();
  params.set("market_ids", marketIds.join(","));
  if (contractIds.length) params.set("contract_ids", contractIds.join(","));
  const res = await fetch(`/api/odds?${params.toString()}`);
  if (!res.ok) throw new Error(`Failed to fetch odds: ${res.status}`);
  return res.json();
}

async function fetchQuotes(marketIds = [], contractIds = []) {
  if (!marketIds.length) return { count: 0, quotes: {} };
  console.debug("fetchQuotes()", { marketIds, contractIds });
  const params = new URLSearchParams();
  params.set("market_ids", marketIds.join(","));
  if (contractIds.length) params.set("contract_ids", contractIds.join(","));
  const res = await fetch(`/api/quotes?${params.toString()}`);
  if (!res.ok) throw new Error(`Failed to fetch quotes: ${res.status}`);
  return res.json();
}

function bpsToDecimal(bps) {
  const n = typeof bps === 'string' ? parseInt(bps, 10) : bps;
  if (!n || Number.isNaN(n) || n <= 0) return null;
  return 10000 / n;
}

function AnalysisPanel({ insights, highlightMode }) {
  if (!insights || !insights.home || !insights.away) {
    return h("div", { class: "analysis-panel" }, h("div", { class: "small" }, ""));
  }
  function percent(x) {
    if (typeof x !== 'number' || !Number.isFinite(x)) return '-';
    return `${Math.round(x * 100)}%`;
  }
  
  // Helper to determine if a stat should be dimmed
  function isDimmed(team, statKey) {
    if (!highlightMode) return false; // No dimming when not in highlight mode
    
    if (highlightMode === 'home') {
      // Home Others: highlight offensive home stats + defensive away stats
      if (team === 'home') {
        return !['avg_goals_scored', 'wins_others_pct', 'home_ht_2plus_pct', 'ht_2plus_to_win_others_pct'].includes(statKey);
      } else { // away team
        return !['avg_goals_conceded', 'losses_others_pct', 'away_ht_2plus_conceded_pct', 'away_ht_2plus_conceded_to_lost_others_pct'].includes(statKey);
      }
    } else if (highlightMode === 'away') {
      // Away Others: highlight offensive away stats + defensive home stats
      if (team === 'away') {
        return !['avg_goals_scored', 'wins_others_pct', 'away_ht_2plus_pct', 'ht_2plus_to_win_others_pct'].includes(statKey);
      } else { // home team
        return !['avg_goals_conceded', 'losses_others_pct', 'home_ht_2plus_conceded_pct', 'home_ht_2plus_conceded_to_lost_others_pct'].includes(statKey);
      }
    }
    return false;
  }
  
  const home = insights.home || {};
  const away = insights.away || {};
  const h2h = insights.h2h || {};
  const h2hMatches = Array.isArray(insights.h2h_matches) ? insights.h2h_matches : [];
  const homeCode = insights.home_code || null;
  const awayCode = insights.away_code || null;
  const leagueScope = Array.isArray(insights.league_scope) ? insights.league_scope.slice(0, 2) : [];
  const homeTitle = insights.home_name || "Home Team";
  const awayTitle = insights.away_name || "Away Team";
  const homeN = typeof home.n === 'number' ? home.n : 0;
  const awayN = typeof away.n === 'number' ? away.n : 0;
  const h2hN = typeof h2h.n === 'number' ? h2h.n : 0;
  
  // Create stat div with conditional dimming
  const statDiv = (team, statKeys, content) => {
    const shouldDim = statKeys.some(key => isDimmed(team, key));
    return h("div", { class: "stat", style: shouldDim ? "opacity: 0.35;" : "" }, content);
  };
  
  return h("div", { class: "analysis-panel" },
    h("div", { class: "stats-grid" }, [
      h("div", { class: "stats-col" }, [
        h("div", { class: "stats-title" }, homeTitle),
        home.league_name ? h("div", { class: "stats-subtitle" }, `(${home.league_name})`) : null,
        ...(homeN > 0 ? [
          h("div", { class: "stat" }, ["Matches: ", h("strong", {}, String(homeN))]),
          statDiv('home', ['avg_goals_scored', 'avg_goals_conceded'], [
            "Avg ⚽ / ⛔: ",
            h("strong", {}, home.avg_goals_scored != null ? home.avg_goals_scored.toFixed(2) : '-'),
            " / ",
            h("strong", {}, home.avg_goals_conceded != null ? home.avg_goals_conceded.toFixed(2) : '-'),
          ]),
          statDiv('home', ['wins_others_pct', 'losses_others_pct'], [
            "Win Others / Lost Others: ",
            h("strong", {}, (typeof home.wins_others_pct === 'number' ? percent(home.wins_others_pct) : '-')),
            " / ",
            h("strong", {}, (typeof home.losses_others_pct === 'number' ? percent(home.losses_others_pct) : '-')),
          ]),
          // Only show HT stats if they exist
          ...(typeof home.home_ht_2plus_pct === 'number' || typeof home.away_ht_2plus_pct === 'number' ? [
            statDiv('home', ['home_ht_2plus_pct', 'away_ht_2plus_pct'], [
              "HT 2+ ⚽ (H / A): ",
              h("strong", {}, (typeof home.home_ht_2plus_pct === 'number' ? percent(home.home_ht_2plus_pct) : '-')),
              " / ",
              h("strong", {}, (typeof home.away_ht_2plus_pct === 'number' ? percent(home.away_ht_2plus_pct) : '-')),
            ]),
            statDiv('home', ['ht_2plus_to_win_others_pct'], [
              "HT 2+ ⚽ → Win 4+ (H / A): ",
              h("strong", {}, (typeof home.ht_2plus_to_win_others_pct === 'number' ? percent(home.ht_2plus_to_win_others_pct) : '-')),
              " / ",
              h("strong", {}, (typeof home.ht_2plus_to_win_others_pct === 'number' ? percent(home.ht_2plus_to_win_others_pct) : '-')),
            ]),
            statDiv('home', ['home_ht_2plus_conceded_pct', 'away_ht_2plus_conceded_pct'], [
              "HT 2+ ⛔ (H / A): ",
              h("strong", {}, (typeof home.home_ht_2plus_conceded_pct === 'number' ? percent(home.home_ht_2plus_conceded_pct) : '-')),
              " / ",
              h("strong", {}, (typeof home.away_ht_2plus_conceded_pct === 'number' ? percent(home.away_ht_2plus_conceded_pct) : '-')),
            ]),
            statDiv('home', ['home_ht_2plus_conceded_to_lost_others_pct', 'away_ht_2plus_conceded_to_lost_others_pct'], [
              "HT 2+ ⛔ → Lost 4+ (H / A): ",
              h("strong", {}, (typeof home.home_ht_2plus_conceded_to_lost_others_pct === 'number' ? percent(home.home_ht_2plus_conceded_to_lost_others_pct) : '-')),
              " / ",
              h("strong", {}, (typeof home.away_ht_2plus_conceded_to_lost_others_pct === 'number' ? percent(home.away_ht_2plus_conceded_to_lost_others_pct) : '-')),
            ]),
          ] : []),
        ] : [
          h("div", { class: "stat small" }, "Stats not available"),
        ])
      ]),
      h("div", { class: "stats-col" }, [
        h("div", { class: "stats-title" }, awayTitle),
        away.league_name ? h("div", { class: "stats-subtitle" }, `(${away.league_name})`) : null,
        ...(awayN > 0 ? [
          h("div", { class: "stat" }, ["Matches: ", h("strong", {}, String(awayN))]),
          statDiv('away', ['avg_goals_scored', 'avg_goals_conceded'], 
            h("div", { class: "stat" }, [
              "Avg ⚽ / ⛔: ",
              h("strong", {}, away.avg_goals_scored != null ? away.avg_goals_scored.toFixed(2) : '-'),
              " / ",
              h("strong", {}, away.avg_goals_conceded != null ? away.avg_goals_conceded.toFixed(2) : '-'),
            ])
          ),
          statDiv('away', ['wins_others_pct', 'losses_others_pct'], 
            h("div", { class: "stat" }, [
              "Win Others / Lost Others: ",
              h("strong", {}, (typeof away.wins_others_pct === 'number' ? percent(away.wins_others_pct) : '-')),
              " / ",
              h("strong", {}, (typeof away.losses_others_pct === 'number' ? percent(away.losses_others_pct) : '-')),
            ])
          ),
          // Only show HT stats if they exist
          ...(typeof away.home_ht_2plus_pct === 'number' || typeof away.away_ht_2plus_pct === 'number' ? [
            statDiv('away', ['home_ht_2plus_pct', 'away_ht_2plus_pct'], 
              h("div", { class: "stat" }, [
                "HT 2+ ⚽ (H / A): ",
                h("strong", {}, (typeof away.home_ht_2plus_pct === 'number' ? percent(away.home_ht_2plus_pct) : '-')),
                " / ",
                h("strong", {}, (typeof away.away_ht_2plus_pct === 'number' ? percent(away.away_ht_2plus_pct) : '-')),
              ])
            ),
            statDiv('away', ['ht_2plus_to_win_others_pct'], 
              h("div", { class: "stat" }, [
                "HT 2+ ⚽ → Win 4+ (H / A): ",
                h("strong", {}, (typeof away.ht_2plus_to_win_others_pct === 'number' ? percent(away.ht_2plus_to_win_others_pct) : '-')),
                " / ",
                h("strong", {}, (typeof away.ht_2plus_to_win_others_pct === 'number' ? percent(away.ht_2plus_to_win_others_pct) : '-')),
              ])
            ),
            statDiv('away', ['home_ht_2plus_conceded_pct', 'away_ht_2plus_conceded_pct'], 
              h("div", { class: "stat" }, [
                "HT 2+ ⛔ (H / A): ",
                h("strong", {}, (typeof away.home_ht_2plus_conceded_pct === 'number' ? percent(away.home_ht_2plus_conceded_pct) : '-')),
                " / ",
                h("strong", {}, (typeof away.away_ht_2plus_conceded_pct === 'number' ? percent(away.away_ht_2plus_conceded_pct) : '-')),
              ])
            ),
            statDiv('away', ['home_ht_2plus_conceded_to_lost_others_pct', 'away_ht_2plus_conceded_to_lost_others_pct'], 
              h("div", { class: "stat" }, [
                "HT 2+ ⛔ → Lost 4+ (H / A): ",
                h("strong", {}, (typeof away.home_ht_2plus_conceded_to_lost_others_pct === 'number' ? percent(away.home_ht_2plus_conceded_to_lost_others_pct) : '-')),
                " / ",
                h("strong", {}, (typeof away.away_ht_2plus_conceded_to_lost_others_pct === 'number' ? percent(away.away_ht_2plus_conceded_to_lost_others_pct) : '-')),
              ])
            ),
          ] : []),
        ] : [
          h("div", { class: "stat small" }, "Stats not available"),
        ])
      ]),
      h("div", { class: "stats-col" }, [
        h("div", { class: "stats-title" }, "Head-to-Head"),
        ...(h2hMatches.length > 0 ? (
          h2hMatches.map(m => {
            const d = (m.date || '').toString();
            const dateStr = d.includes('T') ? d.slice(0,10) : (d.includes(' ') ? d.split(' ')[0] : d);
            const hn = (m.home_norm || '').toString();
            const an = (m.away_norm || '').toString();
            const left = (hn === (home.team_norm || '')) ? (homeCode || 'HOME') : (awayCode || 'AWAY');
            const right = (an === (away.team_norm || '')) ? (awayCode || 'AWAY') : (homeCode || 'HOME');
            const fthg = (typeof m.FTHG === 'number' && Number.isFinite(m.FTHG)) ? Math.round(m.FTHG) : '-';
            const ftag = (typeof m.FTAG === 'number' && Number.isFinite(m.FTAG)) ? Math.round(m.FTAG) : '-';
            return h("div", { class: "stat" }, [
              `${dateStr} ${left} `,
              h("strong", {}, `${fthg}:${ftag}`),
              ` ${right}`,
            ]);
          })
        ) : [
          h("div", { class: "stat small" }, "No head-to-head data"),
        ])
      ]),
      ...(leagueScope.length ? leagueScope.map(bl => {
        const n = typeof bl.n === 'number' ? bl.n : 0;
        const pct = (x) => (typeof x === 'number' && Number.isFinite(x)) ? `${Math.round(x * 100)}%` : '-';
        return h("div", { class: "stats-col" }, [
          h("div", { class: "stats-title" }, String(bl.name || bl.div || '')),
          ...(n > 0 ? [
            h("div", { class: "stat" }, ["Matches: ", h("strong", {}, String(n))]),
            h("div", { class: "stat" }, [
              "Avg ⚽ (H / A): ",
              h("strong", {}, (bl.avg_goals_home != null ? Number(bl.avg_goals_home).toFixed(2) : '-')),
              " / ",
              h("strong", {}, (bl.avg_goals_away != null ? Number(bl.avg_goals_away).toFixed(2) : '-')),
            ]),
            h("div", { class: "stat" }, [
              "Win 4+ (H / A): ",
              h("strong", {}, pct(bl.home_win_others_pct)),
              " / ",
              h("strong", {}, pct(bl.away_win_others_pct)),
            ]),
            // Only show HT stats if they exist
            ...(typeof bl.home_ht_2plus_pct === 'number' || typeof bl.away_ht_2plus_pct === 'number' ? [
              h("div", { class: "stat" }, [
                "HT 2+ ⚽ (H / A): ",
                h("strong", {}, pct(bl.home_ht_2plus_pct)),
                " / ",
                h("strong", {}, pct(bl.away_ht_2plus_pct)),
              ]),
            ] : []),
          ] : [
            h("div", { class: "stat small" }, (bl.type === 'event' ? 'Event league used' : (bl.type === 'home' ? 'Latest league used (Home)' : 'Latest league used (Away)'))),
          ])
        ])
      }) : [])
    ])
  );
}

function Toast({ text, onUndo, onClose }) {
  return h("div", { class: "toast" },
    h("span", { class: "toast-text" }, text || ""),
    h("div", { class: "toast-actions" },
      h("button", { class: "toast-btn", onClick: onUndo }, "Undo"),
      h("button", { class: "toast-btn secondary", onClick: onClose, title: "Dismiss" }, "Dismiss")
    )
  );
}
