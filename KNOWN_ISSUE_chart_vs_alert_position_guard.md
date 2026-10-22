# Known Issue: Chart paints signals that LIVE alert cannot fire

**Created**: 2026-05-13
**Strategy**: Walk The Dog (Pine v6) on COMEX_MINI:MGC1!, 5min
**Status**: Architectural limitation — NOT a bug in code

## TL;DR

The Pine strategy uses `if short_entry and strategy.position_size == 0` as an entry guard. This means:

- If you have an open LONG position when a SHORT signal appears → SHORT does NOT fire alert
- The chart's **strategy tester** can paint the SHORT signal anyway because its internal simulated position state may differ from live broker state

**Result**: chart paints signals that the live alert engine cannot fire. The strategy is doing exactly what it's coded to do — but live and backtest diverge.

## Concrete Evidence (2026-05-13 evening)

### Event 1: 21:55 LONG fires correctly
- Bar: 2026-05-13 21:55 (5min close)
- Signature: `TTTTT65+`
- Pine evaluation: bias all 5 TFs bullish, RSI=65, in tier_d long whitelist
- Guard: `strategy.position_size == 0` → PASS
- Action: `strategy.entry("Long", qty=4)` fires
- Alert: SHIPPED to webhook → Tradovate filled 4 contracts LONG @ ~4719
- Live position: 4 LONG

### Event 2: 22:10 SHORT painted on chart, NO live alert
- Bar: 2026-05-13 22:10 (5min close)
- Signature: `TFTTT60-`
- Pine evaluation: bias H1 flipped bearish, RSI=60, in tier short whitelist
- LIVE guard: `strategy.position_size == 0` → FAIL (still 4 LONG)
- LIVE action: SHORT entry **blocked by guard** → no alert
- BACKTEST tester: paints SHORT signal anyway (its simulated state had position=0)
- Chart shows: `-4 TFTTT60-` label + `TP1 +35 pips +1` + `Stop Hit +3` exit annotations
- Live result: LONG from 21:55 stuck at -$306, SHORT opportunity uncaptured

### Event 0 (REVERSE direction): 21:55 LONG alert fired LIVE, chart did NOT paint it
- Bar: 2026-05-13 21:55 (5min close)
- Signature: `TTTTT65+`
- LIVE: position was 0 (prior LONG had exited) → guard PASS → alert fires → 4 contracts LONG @ ~4719
- CHART: position was still occupied from an earlier trade (likely TFTTT45+ around 21:30 hadn't fully exited in simulator) → guard FAIL → no entry painted
- This is the **opposite divergence** from Events 2 and 3 — here live takes a trade that chart doesn't show
- Implication: chart can be MISSING trades that live actually executes, not just showing trades live can't execute

### Event 3: 22:30 second LONG painted on chart, NO live alert
- Bar: 2026-05-13 ~22:30 (5min close)
- Signature: `TTTTT55+`
- Pine evaluation: bias all 5 TFs bullish again, RSI=55, in tier_d long whitelist
- LIVE guard: `strategy.position_size == 0` → FAIL (still 4 LONG from 21:55)
- LIVE action: second LONG entry **blocked by guard** → no alert
- BACKTEST tester: paints LONG entry + exit (`TP1 +35 +1`, `Stop Hit +3`)
- Live result: would have been a LOSING add ($+35 TP1, then SL on 3 contracts ≈ net -$200)
- **Verdict on this event**: guard actually SAVED money — preventing a double-down on a losing position direction. The "missing alert" is a feature, not a bug here.

### Cost across events
- Event 1 → 2: $306+ unrealized on 21:55 LONG that should have been a SHORT per chart
- Event 1 → 3: guard saved ~$200 of additional loss from pyramid-add
- Net: the guard cuts both ways — sometimes it blocks alpha (event 2), sometimes it blocks losses (event 3)

## Visual Misdirection

The strategy tester paints EVERY signal where its simulated state allows entry. Because the simulator's position state diverges from live (different fill timing, different SL hit timing), the chart shows trades that LIVE could never have taken. This creates the illusion that "the system is missing signals" when actually the system is correctly enforcing the single-position rule.

A user looking at chart sees: "5 signals, 2 alerts fired"
Reality: 5 chart signals were generated, but only 2 had matching live position states allowing entry. The other 3 were blocked by the existing position — exactly as designed.

## Why This Happens (deeper)

`strategy.position_size` in Pine reflects the **simulated** position state during backtest, but in LIVE it reflects whatever Pine "thinks" the position is.

In live, Pine's position state mirrors the LIVE webhook server's broker fills (approximately). But:
- Live fills can happen at slightly different prices/timings than backtest assumed
- If backtest assumed SL fired at 22:05, position=0 at 22:10 → SHORT fires
- If live SL hasn't fired yet, position=4 at 22:10 → SHORT blocked

So **same Pine, same bar, different position state → different action**.

## Why I Can't Fix It With "Recreate Alert"

The operator had been recreating the alert every time inputs or Pine changed. But this issue is INDEPENDENT of snapshot freshness — it's a position-state divergence problem that no alert recreation can fix.

## Possible Fixes (Future Work)

### Option A: Accept the limitation
- Single position at a time = lower frequency, fewer overlapping trades
- This is the current design

### Option B: Stop-and-Reverse (SAR)
- When a reverse signal fires while in position:
  - If unrealized loss > X → close current + reverse
  - If unrealized profit > Y → ignore reverse (lock in)
- Requires Pine source modification

### Option C: Long/Short Split into Two Strategies
- Run Walk The Dog LONG-only on chart 1
- Run Walk The Dog SHORT-only on chart 2
- Each strategy has its own position state
- Doubles webhook complexity

### Option D: Webhook-Server-Level Logic
- Server holds state of current position
- On reverse alert, if drawdown > X → close + reverse
- Doesn't require Pine modification
- Less clean but pragmatic

## Recommendation

Decide after this week's observation period (live runs until 2026-05-20). Then decide A/B/C/D based on:
1. How often this divergence costs real money
2. Whether the choppy market continues (more reversals = more divergences)
3. Tolerance for live-vs-backtest mismatch

## Don't Forget

When debugging chart-vs-alert mismatch in future:
1. Check if **bar timestamps differ** (might be legit different signals at different times) ✓
2. Check if **input snapshot differs** (alert created before recent input change?) ✓
3. Check if **Pine source differs** (alert created before recent code edit?) ✓
4. **Check live position state vs backtest simulated state** ← this issue
