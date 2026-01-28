#!/usr/bin/env node
"use strict";

const MAX_CANDS = 25;
const OBS_WEIGHT = Number(process.env.GEOSCORE_OBS_WEIGHT || 1.0);
const REL_WEIGHT = Number(process.env.GEOSCORE_REL_WEIGHT || 1.0);
const DIST_WEIGHT = Number(process.env.GEOSCORE_DIST_WEIGHT || 0.3);
const EDGE_WEIGHT = Number(process.env.GEOSCORE_EDGE_WEIGHT || 0.15);
const ROUTE_CONF_MIN = Number(process.env.GEOSCORE_ROUTE_CONF || 0.65);
const HOP_CONF_MIN = Number(process.env.GEOSCORE_HOP_CONF || 0.60);
function sigmoid(value) {
  const clipped = Math.max(-20, Math.min(20, value));
  return 1 / (1 + Math.exp(-clipped));
}

function distPenaltyKm(dKm) {
  if (!Number.isFinite(dKm)) return -50;
  if (dKm <= 100) return -(dKm * 0.01);
  if (dKm <= 260) return -(1 + (dKm - 100) * 0.02);
  return -(4 + (dKm - 260) * 0.06);
}

function severityFromMz(nowMs, lastSeenMs) {
  if (!Number.isFinite(lastSeenMs)) return -2;
  const ageSec = (nowMs - lastSeenMs) / 1000;
  if (ageSec <= 24 * 3600) return 0;
  if (ageSec <= 7 * 24 * 3600) return -1;
  return -3;
}

function scoreEmission(cand, observerHome, nowMs) {
  let score = 0;
  if (observerHome && cand.gps) {
    const d = distanceKm(observerHome, cand.gps);
    if (Number.isFinite(d)) {
      score += -Math.log1p(d / 10) * OBS_WEIGHT;
    }
  }
  score += severityFromMz(nowMs, cand.lastSeenMs) * REL_WEIGHT;
  return score;
}

function scoreTransition(prevCand, cand, edgeCount) {
  if (!prevCand || !cand) return 0;
  const d = distanceKm(prevCand.gps, cand.gps);
  const penalty = distPenaltyKm(d);
  const boost = EDGE_WEIGHT * Math.log1p(edgeCount || 0);
  return penalty * DIST_WEIGHT + boost;
}

function distanceKm(a, b) {
  if (!a || !b || !Number.isFinite(a.lat) || !Number.isFinite(a.lon) || !Number.isFinite(b.lat) || !Number.isFinite(b.lon)) return null;
  const toRad = (val) => (val * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lon - a.lon);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const aVal = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(aVal), Math.sqrt(1 - aVal));
  return R * c;
}

function inferRouteViterbi({
  tokens = [],
  observerHome = null,
  ts = Date.now(),
  now = Date.now(),
  candidatesByToken = new Map(),
  edgePrior = () => 0
}) {
  if (!tokens.length) return null;
  const candidateLists = tokens.map((token) => {
    const raw = candidatesByToken.get(token) || [];
    const limited = raw
      .slice(0, MAX_CANDS)
      .map((cand) => ({
        ...cand,
        emission: scoreEmission(cand, observerHome, now)
      }));
    return {
      token,
      list: limited,
      count: limited.length
    };
  });

  const candidateMetadata = candidateLists.map((entry) => {
    const { list, token } = entry;
    const sorted = list.length > 1
      ? [...list]
          .sort((a, b) => b.emission - a.emission)
          .slice(0, 5)
          .map((cand) => {
            const rawDistance = observerHome && cand.gps ? distanceKm(observerHome, cand.gps) : null;
            return {
              pub: cand.pub,
              name: cand.name,
              emission: Number(cand.emission.toFixed(2)),
              distanceKm: Number.isFinite(rawDistance) ? Number(rawDistance.toFixed(2)) : null
            };
          })
      : [];
    return {
      token,
      count: entry.count,
      top: sorted
    };
  });

  const zeroTokens = candidateMetadata.filter((entry) => entry.count === 0).map((entry) => entry.token);
  if (candidateLists.some((entry) => !entry.list.length)) {
    return {
      inferredPubs: Array(tokens.length).fill(null),
      hopConfidences: Array(tokens.length).fill(0),
      routeConfidence: 0,
      unresolved: 1,
      candidatesJson: JSON.stringify(candidateMetadata),
      candidateMetadata,
      teleportMaxKm: null,
      diagnostics: {
        candidateCounts: candidateMetadata.map((entry) => entry.count),
        zeroCandidateTokens: zeroTokens,
        bestScore: null,
        runnerUpScore: null,
        routeMargin: null,
        routeConfidence: 0
      }
    };
  }

  const dp = [];
  candidateLists.forEach((entry, idx) => {
    dp[idx] = [];
    entry.list.forEach((cand) => {
      if (idx === 0) {
        dp[idx].push({
          pub: cand.pub,
          candidate: cand,
          score: cand.emission,
          prevIdx: -1
        });
        return;
      }
      let bestScore = -Infinity;
      let bestPrev = -1;
      dp[idx - 1].forEach((prevState, prevIdx) => {
        const transition = scoreTransition(prevState.candidate, cand, edgePrior(prevState.pub, cand.pub));
        const total = prevState.score + cand.emission + transition;
        if (total > bestScore) {
          bestScore = total;
          bestPrev = prevIdx;
        }
      });
      dp[idx].push({
        pub: cand.pub,
        candidate: cand,
        score: bestScore,
        prevIdx: bestPrev
      });
    });
  });

  const lastIndex = tokens.length - 1;
  const lastStates = dp[lastIndex];
  if (!lastStates.length) return null;
  const sortedLast = [...lastStates].sort((a, b) => b.score - a.score);
  const best = sortedLast[0];
  const runnerUp = sortedLast[1] || { score: best.score - 3 };
  const routeMargin = best.score - runnerUp.score;
  const routeConfidence = sigmoid(routeMargin);
  const path = [];
  let cursor = best;
  for (let idx = lastIndex; idx >= 0; idx -= 1) {
    if (!cursor) break;
    path[idx] = cursor;
    cursor = cursor.prevIdx >= 0 ? dp[idx - 1][cursor.prevIdx] : null;
  }

  const inferredPubs = path.map((state) => state?.pub || null);
  const hopConfidences = path.map((state, idx) => {
    if (!state) return 0;
    const states = dp[idx];
    const bestState = states.find((s) => s.pub === state.pub);
    const alt = states.filter((s) => s.pub !== state.pub).sort((a, b) => b.score - a.score)[0];
    const margin = bestState && alt ? bestState.score - alt.score : bestState?.score ?? 0;
    return sigmoid(margin);
  });

  let teleportMaxKm = null;
  for (let idx = 0; idx < path.length - 1; idx += 1) {
    const a = path[idx]?.candidate?.gps;
    const b = path[idx + 1]?.candidate?.gps;
    const d = distanceKm(a, b);
    if (Number.isFinite(d)) {
      if (!teleportMaxKm || d > teleportMaxKm) teleportMaxKm = d;
    }
  }

  const unresolved = routeConfidence < ROUTE_CONF_MIN || hopConfidences.some((conf) => conf < HOP_CONF_MIN) ? 1 : 0;
  const teleportMaxKmFinal = teleportMaxKm !== null ? Number(teleportMaxKm.toFixed(2)) : null;
  return {
    inferredPubs,
    hopConfidences,
    routeConfidence,
    unresolved,
    candidatesJson: JSON.stringify(candidateMetadata),
    candidateMetadata,
    teleportMaxKm: teleportMaxKmFinal,
    diagnostics: {
      candidateCounts: candidateMetadata.map((entry) => entry.count),
      zeroCandidateTokens: zeroTokens,
      bestScore: best.score,
      runnerUpScore: runnerUp.score,
      routeMargin,
      routeConfidence
    }
  };
}

module.exports = {
  distPenaltyKm,
  scoreEmission,
  scoreTransition,
  inferRouteViterbi
};
