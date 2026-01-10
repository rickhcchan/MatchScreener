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

// Merge new odds data with existing, preserving old values when new data is missing
function mergeOddsData(prevOdds, newOdds) {
  if (!newOdds || Object.keys(newOdds).length === 0) return prevOdds;
  const merged = { ...prevOdds };
  Object.keys(newOdds).forEach(marketId => {
    if (!merged[marketId]) merged[marketId] = {};
    Object.keys(newOdds[marketId]).forEach(contractId => {
      merged[marketId][contractId] = newOdds[marketId][contractId];
    });
  });
  return merged;
}

// Merge new quotes data with existing, preserving old values when new data is missing
function mergeQuotesData(prevQuotes, newQuotes) {
  if (!newQuotes || Object.keys(newQuotes).length === 0) return prevQuotes;
  const merged = { ...prevQuotes };
  Object.keys(newQuotes).forEach(contractId => {
    merged[contractId] = newQuotes[contractId];
  });
  return merged;
}

function EventsTable() {
  const [state, setState] = useState({ loading: true, error: null, data: { count: 0, events: [] } });
    const [selectedDay, setSelectedDay] = useState(null); // null => today
    const [viewMode, setViewMode] = useState('all'); // 'all' | 'inprogress' | 'betted' | 'bettable' | 'starred'
    const [selectedLeague, setSelectedLeague] = useState('all'); // league filter
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
  const [lastUpdateMap, setLastUpdateMap] = useState({}); // event_id -> timestamp of last successful odds/quotes update
  const [quotesLoading, setQuotesLoading] = useState(false); // true when quotes API request is in-flight
  const [noDataAlerts, setNoDataAlerts] = useState({}); // event_id -> timestamp when "no data" alert should be shown
  const [toast, setToast] = useState(null); // { text, onUndo }
  const toastTimerRef = useRef(null);
  const [marksByDate, setMarksByDate] = useState({}); // dateKey -> eventId -> { maybe: bool, bet: bool }
  const idsRef = useRef([]);
  const eventsRef = useRef([]);
  const marketIdsRef = useRef([]);
  const contractIdsRef = useRef([]);
  
  // Update timestamps for events that received fresh data
  function updateTimestamps(dataMap, events) {
    if (!dataMap || Object.keys(dataMap).length === 0) return;
    const now = Date.now();
    
    setLastUpdateMap(prev => {
      const updated = { ...prev };
      const alertsToSet = {};
      const alertsToClear = {};
      
      (events || []).forEach(e => {
        const eid = String(e.id);
        // Check if this event's markets/contracts are in the new data
        const hasData = 
          (e.winner_market_id && dataMap[e.winner_market_id]) ||
          (e.over_under_45_market_id && dataMap[e.over_under_45_market_id]) ||
          (e.over_under_55_market_id && dataMap[e.over_under_55_market_id]) ||
          (e.winner_contract_home_id && dataMap[e.winner_contract_home_id]) ||
          (e.winner_contract_draw_id && dataMap[e.winner_contract_draw_id]) ||
          (e.winner_contract_away_id && dataMap[e.winner_contract_away_id]) ||
          (e.over_45_contract_id && dataMap[e.over_45_contract_id]) ||
          (e.over_55_contract_id && dataMap[e.over_55_contract_id]);
        
        if (hasData) {
          updated[eid] = now;
          alertsToClear[eid] = true;
        } else if (prev[eid]) {
          // Event previously had data but didn't get any in this update
          alertsToSet[eid] = true;
          console.warn(`NO DATA for event ${eid} - timer at ${Math.floor((now - prev[eid]) / 1000)}s`);
        }
      });
      
      // Update alerts
      setNoDataAlerts(prevAlerts => {
        const newAlerts = { ...prevAlerts };
        // Add new alerts
        Object.keys(alertsToSet).forEach(eid => {
          newAlerts[eid] = true;
        });
        // Clear alerts for events that got data
        Object.keys(alertsToClear).forEach(eid => {
          delete newAlerts[eid];
        });
        return newAlerts;
      });
      
      return updated;
    });
  }
  
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
          if (e.winner_contract_home_id) cids.push(String(e.winner_contract_home_id));
          if (e.winner_contract_draw_id) cids.push(String(e.winner_contract_draw_id));
          if (e.winner_contract_away_id) cids.push(String(e.winner_contract_away_id));
          if (e.over_under_45_market_id) mids.push(String(e.over_under_45_market_id));
          if (e.over_45_contract_id) cids.push(String(e.over_45_contract_id));
          if (e.over_under_55_market_id) mids.push(String(e.over_under_55_market_id));
          if (e.over_55_contract_id) cids.push(String(e.over_55_contract_id));
          if (e.over_under_25_market_id) mids.push(String(e.over_under_25_market_id));
          if (e.over_25_contract_id) cids.push(String(e.over_25_contract_id));
          if (e.over_under_35_market_id) mids.push(String(e.over_under_35_market_id));
          if (e.over_35_contract_id) cids.push(String(e.over_35_contract_id));
          if (e.over_under_65_market_id) mids.push(String(e.over_under_65_market_id));
          if (e.over_65_contract_id) cids.push(String(e.over_65_contract_id));
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
            .then(o => setOddsMap(prev => mergeOddsData(prev, o.prices || {})))
            .catch(err => console.warn("fetchOdds error", err));
          if (contractIdsRef.current.length) {
            console.debug("Initial fetchQuotes", { marketIds: marketIdsRef.current, contractIds: contractIdsRef.current });
            setQuotesLoading(true);
            fetchQuotes(marketIdsRef.current, contractIdsRef.current)
              .then(q => {
                setQuotesMap(prev => mergeQuotesData(prev, q.quotes || {}));
                updateTimestamps(q.quotes || {}, eventsRef.current);
              })
              .catch(err => console.warn("fetchQuotes error", err))
              .finally(() => setQuotesLoading(false));
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
        .then(o => setOddsMap(prev => mergeOddsData(prev, o.prices || {})))
        .catch(err => console.warn("fetchOdds error", err));
    }, ODDS_POLL_SECS * 1000);

    // Quotes polling
    const quotesTimer = setInterval(() => {
      const mids = marketIdsRef.current || [];
      const cids = contractIdsRef.current || [];
      if (!mids.length || !cids.length) return;
      console.debug("Polling fetchQuotes", { marketIds: mids, contractIds: cids });
      setQuotesLoading(true);
      fetchQuotes(mids, cids)
        .then(q => {
          setQuotesMap(prev => mergeQuotesData(prev, q.quotes || {}));
          updateTimestamps(q.quotes || {}, eventsRef.current);
        })
        .catch(err => console.warn("fetchQuotes error", err))
        .finally(() => setQuotesLoading(false));
    }, QUOTES_POLL_SECS * 1000);

    return () => { cancelled = true; clearInterval(timer); clearInterval(oddsTimer); clearInterval(quotesTimer); };
  }, [selectedDay]);

  // Reset league filter when date changes
  useEffect(() => {
    setSelectedLeague('all');
  }, [selectedDay]);

  // Reset league filter when viewMode changes if selected league no longer exists in filtered view
  useEffect(() => {
    const rows = state.data.events || [];
    let filteredRows = rows;
    
    // Apply viewMode filter to get available leagues
    if (viewMode === 'inprogress') {
      filteredRows = rows.filter(e => {
        const st = statesMap[e.id] || {};
        const mp = (st.match_period || '').toLowerCase();
        if (mp === 'pre_match' || mp === 'full_time') return false;
        return true;
      });
    } else if (viewMode === 'betted') {
      filteredRows = rows.filter(e => {
        const dk = dateKeyForEvent(e);
        const entry = (marksByDate[dk] || {})[String(e.id)] || { maybe: false, bet: false };
        if (!entry.bet) return false;
        const st = statesMap[e.id] || {};
        const mp = (st.match_period || '').toLowerCase();
        if (mp === 'full_time') return false;
        return true;
      });
    } else if (viewMode === 'bettable') {
      filteredRows = rows.filter(e => {
        const st = statesMap[e.id] || {};
        const mp = (st.match_period || '').toLowerCase();
        if (mp === 'full_time') return false;
        const dt = e.start_datetime ? new Date(e.start_datetime) : null;
        if (!dt) return false;
        const diffMs = dt.getTime() - Date.now();
        if (diffMs <= 0) return false;
        const withinTwoHours = diffMs <= (120 * 60 * 1000);
        return withinTwoHours;
      });
    } else if (viewMode === 'starred') {
      filteredRows = rows.filter(e => {
        const dk = dateKeyForEvent(e);
        const entry = (marksByDate[dk] || {})[String(e.id)] || { maybe: false, bet: false };
        if (!entry.maybe) return false;
        const st = statesMap[e.id] || {};
        const mp = (st.match_period || '').toLowerCase();
        if (mp === 'full_time') return false;
        return true;
      });
    }
    
    // Build available leagues from filtered rows
    const availableLeagues = new Set();
    filteredRows.forEach(e => {
      const league = extractLeagueName(e.full_slug);
      if (league) availableLeagues.add(league);
    });
    
    // If current selection doesn't exist in filtered view, reset to 'all'
    if (selectedLeague !== 'all' && !availableLeagues.has(selectedLeague)) {
      setSelectedLeague('all');
    }
  }, [viewMode, state.data.events, marksByDate, statesMap]);

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
      if (e.over_45_contract_id) cids.push(String(e.over_45_contract_id));
      if (e.over_under_55_market_id) mids.push(String(e.over_under_55_market_id));
      if (e.over_55_contract_id) cids.push(String(e.over_55_contract_id));
    });
    eventsRef.current = state.data.events || [];
    marketIdsRef.current = Array.from(new Set(mids));
    contractIdsRef.current = Array.from(new Set(cids));
  }, [state.data.events]);

  if (state.loading) return h("p", { class: "small" }, "Loading events...");
  if (state.error) return h("p", { class: "small" }, `Error: ${state.error}`);

  const rows = state.data.events || [];
  
  let displayRows = rows;
  
  // Apply view mode filter first
  if (viewMode === 'inprogress') {
    displayRows = displayRows.filter(e => {
      const st = statesMap[e.id] || {};
      const mp = (st.match_period || '').toLowerCase();
      const stateFromStates = (st.state || '').toLowerCase();
      const stateFromEvent = (e.state || '').toLowerCase();
      // Exclude finished matches (check both states API and event data)
      if (mp === 'full_time' || stateFromStates === 'ended' || stateFromEvent === 'ended') return false;
      // Include only if there's a live clock OR match period indicates live play
      const liveClock = (st && st.clock_text) ? String(st.clock_text) : null;
      const isLivePeriod = mp && mp !== 'pre_match' && mp !== '';
      // Also check if start time has passed
      const dt = e.start_datetime ? new Date(e.start_datetime) : null;
      const hasStarted = dt ? (dt.getTime() - Date.now() < 0) : false;
      return liveClock || (isLivePeriod && hasStarted);
    });
  } else if (viewMode === 'betted') {
    displayRows = displayRows.filter(e => {
      const dk = dateKeyForEvent(e);
      const entry = (marksByDate[dk] || {})[String(e.id)] || { maybe: false, bet: false };
      if (!entry.bet) return false;
      const st = statesMap[e.id] || {};
      const mp = (st.match_period || '').toLowerCase();
      const stateFromStates = (st.state || '').toLowerCase();
      const stateFromEvent = (e.state || '').toLowerCase();
      if (mp === 'full_time' || stateFromStates === 'ended' || stateFromEvent === 'ended') return false; // exclude ended events
      return true; // show all betted matches not ended
    });
  } else if (viewMode === 'bettable') {
    displayRows = displayRows.filter(e => {
      const st = statesMap[e.id] || {};
      const mp = (st.match_period || '').toLowerCase();
      const stateFromStates = (st.state || '').toLowerCase();
      const stateFromEvent = (e.state || '').toLowerCase();
      if (mp === 'full_time' || stateFromStates === 'ended' || stateFromEvent === 'ended') return false; // exclude ended events
      // Only include matches that have NOT started yet and start within 120 minutes
      const dt = e.start_datetime ? new Date(e.start_datetime) : null;
      if (!dt) return false;
      const diffMs = dt.getTime() - Date.now();
      if (diffMs <= 0) return false; // already started or kickoff passed
      const withinTwoHours = diffMs <= (120 * 60 * 1000);
      return withinTwoHours;
    });
  } else if (viewMode === 'starred') {
    displayRows = displayRows.filter(e => {
      const dk = dateKeyForEvent(e);
      const entry = (marksByDate[dk] || {})[String(e.id)] || { maybe: false, bet: false };
      if (!entry.maybe) return false;
      const st = statesMap[e.id] || {};
      const mp = (st.match_period || '').toLowerCase();
      const stateFromStates = (st.state || '').toLowerCase();
      const stateFromEvent = (e.state || '').toLowerCase();
      if (mp === 'full_time' || stateFromStates === 'ended' || stateFromEvent === 'ended') return false; // exclude ended events
      return true;
    });
  }
  
  // Extract distinct leagues from filtered rows (after viewMode filter)
  const leaguesSet = new Set();
  displayRows.forEach(e => {
    const league = extractLeagueName(e.full_slug);
    if (league) leaguesSet.add(league);
  });
  const leagueOptions = ['all', ...Array.from(leaguesSet).sort()];
  
  // Then apply league filter
  if (selectedLeague !== 'all') {
    displayRows = displayRows.filter(e => {
      const league = extractLeagueName(e.full_slug);
      return league === selectedLeague;
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
    // Clear starred/betted status from localStorage
    const removed = (state.data.events || []).find(ev => String(ev.id) === String(eventId));
    if (removed) {
      const dk = dateKeyForEvent(removed);
      setMarksByDate(prev => {
        const updated = { ...prev };
        if (updated[dk] && updated[dk][String(eventId)]) {
          const dayMarks = { ...updated[dk] };
          delete dayMarks[String(eventId)];
          if (Object.keys(dayMarks).length === 0) {
            delete updated[dk];
          } else {
            updated[dk] = dayMarks;
          }
        }
        return updated;
      });
    }
    
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
        //h("label", { class: "small", style: { marginRight: "6px" } }, "Date:"),
        h("select", {
          class: "analysis-btn",
          value: selectedDay || "",
          onChange: (e) => {
            const v = e.target.value;
            setSelectedDay(v ? v : null);
          }
        }, dayOptions.map(opt => h("option", { value: opt.value || "" }, opt.label))),
        (() => {
          const labels = { all: "All Matches", inprogress: "In Progress", betted: "Betted", bettable: "Starting in 2 Hours", starred: "Starred" };
          return h("button", {
            class: "analysis-btn",
            onClick: () => setViewMode(m => (m === 'all' ? 'inprogress' : (m === 'inprogress' ? 'bettable' : (m === 'bettable' ? 'betted' : (m === 'betted' ? 'starred' : 'all'))))),
            title: "Cycle view: All → In Progress → Starting Soon → Betted → Starred"
          }, labels[viewMode]);
        })(),
        h("select", {
          class: "analysis-btn league-dropdown",
          value: selectedLeague,
          onChange: (e) => setSelectedLeague(e.target.value)
        }, leagueOptions.map(opt => h("option", { value: opt }, opt === 'all' ? 'All Leagues' : opt))),
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
          lastUpdate: lastUpdateMap[String(e.id)] || null,
          isLoading: quotesLoading,
          showNoDataAlert: !!noDataAlerts[String(e.id)],
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
  // Extract league slug from pattern: /sport/football/leagues/{league-slug}/... OR /sport/football/{competition-slug}/...
  let match = fullSlug.match(/\/leagues\/([^\/]+)/);
  if (!match) {
    // Try direct pattern: /sport/football/{competition}/
    match = fullSlug.match(/\/sport\/football\/([^\/]+)/);
  }
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
function MatchCard({ e, st, oddsMap, quotesMap, lastUpdate, isLoading, showNoDataAlert, maybeActive, betActive, onToggleMaybe, onToggleBet, onRemove, expandedOpen, insights, onToggleExpand, onCardMount, onCardUnmount }) {
  const eid = e.id || "";
  const dt = e.start_datetime ? new Date(e.start_datetime) : null;
  const home = e.home_name || e.home || "";
  const away = e.away_name || e.away || "";
  const url = e.event_url || "#";
  const status = computeStatus(e, st);
  const hasScores = Array.isArray(st.scores_current) && st.scores_current.length === 2;
  const homeSc = hasScores ? String(st.scores_current[0]) : null;
  const awaySc = hasScores ? String(st.scores_current[1]) : null;
  const homeScNum = hasScores ? parseInt(st.scores_current[0], 10) : null;
  const awayScNum = hasScores ? parseInt(st.scores_current[1], 10) : null;
  const homeLeading = (homeScNum !== null && awayScNum !== null && homeScNum > awayScNum);
  const awayLeading = (homeScNum !== null && awayScNum !== null && awayScNum > homeScNum);
  const rootRef = useRef(null);
  
  // Auto-refresh timer for "X seconds ago" display
  const [, setTick] = useState(0);
  useEffect(() => {
    const timer = setInterval(() => setTick(t => t + 1), 1000);
    return () => clearInterval(timer);
  }, []);
  
  function formatLastUpdate(timestamp) {
    if (!timestamp) return null;
    const seconds = Math.floor((Date.now() - timestamp) / 1000);
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    return `${hours}h`;
  }
  
  function getStaleClass(timestamp) {
    return '';
  }
  
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
  const ou25 = e.over_under_25_market_id ? String(e.over_under_25_market_id) : null;
  const ou35 = e.over_under_35_market_id ? String(e.over_under_35_market_id) : null;
  const ou45 = e.over_under_45_market_id ? String(e.over_under_45_market_id) : null;
  const ou55 = e.over_under_55_market_id ? String(e.over_under_55_market_id) : null;
  const ou65 = e.over_under_65_market_id ? String(e.over_under_65_market_id) : null;
  const homeOdds = oddsFor(wm, e.winner_contract_home_id ? String(e.winner_contract_home_id) : null);
  const drawOdds = oddsFor(wm, e.winner_contract_draw_id ? String(e.winner_contract_draw_id) : null);
  const awayOdds = oddsFor(wm, e.winner_contract_away_id ? String(e.winner_contract_away_id) : null);
  const over25Odds = oddsFor(ou25, e.over_25_contract_id ? String(e.over_25_contract_id) : null);
  const over35Odds = oddsFor(ou35, e.over_35_contract_id ? String(e.over_35_contract_id) : null);
  const over45Odds = oddsFor(ou45, e.over_45_contract_id ? String(e.over_45_contract_id) : null);
  const over55Odds = oddsFor(ou55, e.over_55_contract_id ? String(e.over_55_contract_id) : null);
  const over65Odds = oddsFor(ou65, e.over_65_contract_id ? String(e.over_65_contract_id) : null);

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
          hasScores ? h("span", { class: `score-badge home-score${homeLeading ? ' leading' : ''}` }, homeSc) : h("span", { class: "score-badge empty home-score" }, ""),
          h("div", { class: "team-name away-name" }, away),
          hasScores ? h("span", { class: `score-badge away-score${awayLeading ? ' leading' : ''}` }, awaySc) : h("span", { class: "score-badge empty away-score" }, ""),
        ]),
        leagueName ? h("div", { class: "league-name", title: "Competition" }, leagueName) : null
      )
    ),
    h("div", { class: `status ${status.class}` }, h("span", { class: "time" }, status.text)),
    lastUpdate ? h("div", { class: `last-update ${getStaleClass(lastUpdate)}`, title: "Time since last offers update" }, [
      showNoDataAlert ? h("span", { class: "no-data-alert" }, "Delayed") : null,
      isLoading ? h("svg", { class: "refresh-icon", viewBox: "0 0 24 24", xmlns: "http://www.w3.org/2000/svg" }, [
        h("path", { d: "M21 10c-1 0-2 1-2 2 0 3-2 6-6 6s-6-3-6-6 3-6 6-6c2 0 3 1 4 2l-3 0 0 2 6 0 0-6-2 0 0 3c-1-2-3-3-5-3-5 0-8 4-8 8s3 8 8 8 8-4 8-8c0-1-1-2-2-2z", fill: "currentColor" })
      ]) : null,
      h("span", {}, formatLastUpdate(lastUpdate))
    ]) : null,
    h("div", { class: "odds" },
      h(OddCell, { label: home || "Home", odds: homeOdds }),
      h(OddCell, { label: "Draw", odds: drawOdds }),
      h(OddCell, { label: away || "Away", odds: awayOdds }),
      h(OddCell, { label: "Over 2.5", odds: over25Odds }),
      h(OddCell, { label: "Over 3.5", odds: over35Odds }),
      h(OddCell, { label: "Over 4.5", odds: over45Odds }),
      h(OddCell, { label: "Over 5.5", odds: over55Odds }),
      h(OddCell, { label: "Over 6.5", odds: over65Odds }),
    ),
    h("div", { class: "analysis-toggle" },
      h("button", { class: "analysis-btn", onClick: onToggleExpand, disabled: !statsAvailable, title: (!statsAvailable ? "Stats not available" : "") }, statsBtnText),
      (url && url !== "#") ? h("a", { class: "analysis-btn", href: url, target: "smarkets", rel: "noopener noreferrer", title: "Open in Smarkets", onClick: (ev) => { ev.preventDefault(); try { window.open(url, 'smarkets', 'noopener'); } catch {} } }, "Open in Smarkets") : null
    ),
    expandedOpen ? h(AnalysisPanel, { insights }) : null
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
  const state = (st && st.state) ? String(st.state).toLowerCase() : '';
  // Check both match_period and state for ended matches
  if (period === 'full_time' || state === 'ended') return { class: 'ended', text: 'EVENT ENDED' };
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

function AnalysisPanel({ insights }) {
  if (!insights || !insights.home || !insights.away) {
    return h("div", { class: "analysis-panel" }, h("div", { class: "small" }, ""));
  }
  function percent(x) {
    if (typeof x !== 'number' || !Number.isFinite(x)) return '-';
    return `${Math.round(x * 100)}%`;
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
  

  
  return h("div", { class: "analysis-panel" },
    h("div", { class: "stats-grid" }, [
      h("div", { class: "stats-col" }, [
        h("div", { class: "stats-title" }, homeTitle),
        home.league_name ? h("div", { class: "stats-subtitle" }, `(${home.league_name})`) : null,
        ...(homeN > 0 ? [
          h("div", { class: "stat" }, ["Matches: ", h("strong", {}, String(homeN))]),
          h("div", { class: "stat" }, [
            "HT Avg ⚽ / ⛔: ",
            h("strong", {}, home.avg_ht_goals_scored != null ? home.avg_ht_goals_scored.toFixed(2) : '-'),
            " / ",
            h("strong", {}, home.avg_ht_goals_conceded != null ? home.avg_ht_goals_conceded.toFixed(2) : '-'),
          ]),
          h("div", { class: "stat" }, [
            "FT Avg ⚽ / ⛔: ",
            h("strong", {}, home.avg_goals_scored != null ? home.avg_goals_scored.toFixed(2) : '-'),
            " / ",
            h("strong", {}, home.avg_goals_conceded != null ? home.avg_goals_conceded.toFixed(2) : '-'),
          ]),
          h("div", { class: "stat" }, [
            "Win Others",
            " / ",
            "Lost Others",
            ": ",
            h("strong", {}, (typeof home.wins_others_pct === 'number' ? percent(home.wins_others_pct) : '-')),
            " / ",
            h("strong", {}, (typeof home.losses_others_pct === 'number' ? percent(home.losses_others_pct) : '-')),
          ]),
          // Only show HT stats if they exist
          ...(typeof home.home_ht_2plus_pct === 'number' || typeof home.away_ht_2plus_pct === 'number' ? [
            h("div", { class: "stat" }, [
              "HT 2+ ⚽ → Win 4+ (H / A): ",
              h("strong", {}, (typeof home.home_ht_2plus_pct === 'number' ? percent(home.home_ht_2plus_pct) : '-')),
              " / ",
              h("strong", {}, (typeof home.away_ht_2plus_pct === 'number' ? percent(home.away_ht_2plus_pct) : '-')),
              " → ",
              h("strong", {}, (typeof home.ht_2plus_to_win_others_pct === 'number' ? percent(home.ht_2plus_to_win_others_pct) : '-')),
              " / ",
              h("strong", {}, (typeof home.ht_2plus_to_win_others_pct === 'number' ? percent(home.ht_2plus_to_win_others_pct) : '-')),
            ]),
            h("div", { class: "stat" }, [
              "HT 2+ ⛔ → Lost 4+ (H / A): ",
              h("strong", {}, (typeof home.home_ht_2plus_conceded_pct === 'number' ? percent(home.home_ht_2plus_conceded_pct) : '-')),
              " / ",
              h("strong", {}, (typeof home.away_ht_2plus_conceded_pct === 'number' ? percent(home.away_ht_2plus_conceded_pct) : '-')),
              " → ",
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
          h("div", { class: "stat" }, [
            "HT Avg ⚽ / ⛔: ",
            h("strong", {}, away.avg_ht_goals_scored != null ? away.avg_ht_goals_scored.toFixed(2) : '-'),
            " / ",
            h("strong", {}, away.avg_ht_goals_conceded != null ? away.avg_ht_goals_conceded.toFixed(2) : '-'),
          ]),
          h("div", { class: "stat" }, [
            "FT Avg ⚽ / ⛔: ",
            h("strong", {}, away.avg_goals_scored != null ? away.avg_goals_scored.toFixed(2) : '-'),
            " / ",
            h("strong", {}, away.avg_goals_conceded != null ? away.avg_goals_conceded.toFixed(2) : '-'),
          ]),
          h("div", { class: "stat" }, [
            "Win Others",
            " / ",
            "Lost Others",
            ": ",
            h("strong", {}, (typeof away.wins_others_pct === 'number' ? percent(away.wins_others_pct) : '-')),
            " / ",
            h("strong", {}, (typeof away.losses_others_pct === 'number' ? percent(away.losses_others_pct) : '-')),
          ]),
          // Only show HT stats if they exist
          ...(typeof away.home_ht_2plus_pct === 'number' || typeof away.away_ht_2plus_pct === 'number' ? [
            h("div", { class: "stat" }, [
              "HT 2+ ⚽ → Win 4+ (H / A): ",
              h("strong", {}, (typeof away.home_ht_2plus_pct === 'number' ? percent(away.home_ht_2plus_pct) : '-')),
              " / ",
              h("strong", {}, (typeof away.away_ht_2plus_pct === 'number' ? percent(away.away_ht_2plus_pct) : '-')),
              " → ",
              h("strong", {}, (typeof away.ht_2plus_to_win_others_pct === 'number' ? percent(away.ht_2plus_to_win_others_pct) : '-')),
              " / ",
              h("strong", {}, (typeof away.ht_2plus_to_win_others_pct === 'number' ? percent(away.ht_2plus_to_win_others_pct) : '-')),
            ]),
            h("div", { class: "stat" }, [
              "HT 2+ ⛔ → Lost 4+ (H / A): ",
              h("strong", {}, (typeof away.home_ht_2plus_conceded_pct === 'number' ? percent(away.home_ht_2plus_conceded_pct) : '-')),
              " / ",
              h("strong", {}, (typeof away.away_ht_2plus_conceded_pct === 'number' ? percent(away.away_ht_2plus_conceded_pct) : '-')),
              " → ",
              h("strong", {}, (typeof away.home_ht_2plus_conceded_to_lost_others_pct === 'number' ? percent(away.home_ht_2plus_conceded_to_lost_others_pct) : '-')),
              " / ",
              h("strong", {}, (typeof away.away_ht_2plus_conceded_to_lost_others_pct === 'number' ? percent(away.away_ht_2plus_conceded_to_lost_others_pct) : '-')),
            ]),
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
