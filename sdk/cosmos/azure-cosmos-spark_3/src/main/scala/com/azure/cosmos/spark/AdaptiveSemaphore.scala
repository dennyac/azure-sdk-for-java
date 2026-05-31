// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.spark

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

/**
 * Two-phase TCP-Reno-style admission controller for the bulk-writer pending-operations
 * semaphore. Used by [[BulkWriter]] in place of the default fixed-size [[Semaphore]] when
 * `spark.cosmos.write.bulk.maxPendingOperations.adaptive = true`.
 *
 * Algorithm — same shape as TCP Reno congestion control circa 1988, and the same shape
 * used by Netflix's `concurrency-limits` library (`AIMDLimit`), Envoy's adaptive
 * concurrency filter, and AWS Lambda's Adaptive Concurrency Throttling. The pattern is
 * standard; the only Cosmos-specific bits are which 429 sub-status counts as a "drop"
 * signal and the integration with the existing per-task [[Semaphore]].
 *
 * Phases:
 *
 *   - SLOW_START — `cap` starts at `initialPermits` (8). Every `growthStreak` (20)
 *     consecutive successful per-op responses, `cap` doubles, up to either `ssthresh`
 *     or `ceiling`. This phase ends on the first backend 429 (whichever first).
 *
 *   - CONGESTION_AVOIDANCE — entered after the first backend 429 (or when `cap`
 *     reaches `ssthresh`). Every `growthStreak` successes, `cap += 1` (additive
 *     increase). On every backend 429 while the semaphore is saturated: halve `cap`,
 *     set `ssthresh = cap_before / 2`, 100ms cooldown to prevent the same overload
 *     event from collapsing the cap below `floor`.
 *
 * The first phase finds the right operating cap fast (RU-agnostic — slow-start doubles
 * per ~1 RTT so it reaches `ceiling = maxPendingOperations` in ~10 doublings = O(1s)
 * on a 50ms link); the second phase keeps it stable around the discovered ceiling.
 *
 * The throughput-control cold-start over-allocation (every client believes it owns 100%
 * of the group budget for `controlItemRenewInterval`, SDK-enforced minimum 5s) is
 * handled implicitly: slow-start growth during the over-allocation window may produce
 * a brief 429 burst right after share redistribution, which `onServerThrottle` halves
 * away within a couple of RTTs. We did simulate a more elaborate three-phase variant
 * with an explicit "warmup" phase that grows slowly during this window; the simulator
 * showed it added complexity without measurably reducing 429s (see project README of
 * `cosmos-bulk-writer-sim` for the per-RU sweep).
 *
 * Server vs client 429:
 *  - [[onServerThrottle]] is called for backend 429s (Cosmos returned
 *    `TOO_MANY_REQUESTS` with a sub-status that is NOT throughput-control). This
 *    shrinks.
 *  - [[onClientThrottle]] is called for `THROUGHPUT_CONTROL_REQUEST_RATE_TOO_LARGE`
 *    (10003) and `THROUGHPUT_CONTROL_BULK_REQUEST_RATE_TOO_LARGE` (10005) — the
 *    SDK's local gate rejected the request before it left the SDK, so no RU was
 *    consumed beyond bookkeeping. This is an explicit no-op; reacting would pull
 *    permits below what's needed to keep the pipeline busy when the bucket refills.
 *
 * == Why `Semaphore` extends, not wraps ==
 *
 * `java.util.concurrent.Semaphore` exposes a protected `reducePermits(int)` that
 * lazily shrinks the cap — future `release()` calls are absorbed paying down the
 * deficit, so we never have to yank permits from in-flight ops. By extending
 * `Semaphore` rather than wrapping, the rest of its API (`acquire` / `tryAcquire` /
 * `release` / `availablePermits`) flows through unchanged for callers — `BulkWriter`
 * doesn't have to know whether the field is `Semaphore` or `AdaptiveSemaphore`.
 *
 * == What `ceiling` means now ==
 *
 * Originally `maxPendingOperations` was a rate cap (hard ceiling on in-flight ops).
 * With this controller in play, the rate is governed by the adaptive `cap` and the
 * throughput-control bucket — the `ceiling` is now primarily a **memory bound**.
 * The default of `DefaultMaxPendingOperationPerCore = 10688` was derived from a
 * memory calculation (1024 docs × 167 partitions / 16 cores ≈ 10MB per core at
 * 1KB documents); it remains the right value for that reason.
 *
 * == Thread-safety ==
 *
 * Hot-path `onSuccess` uses `compareAndSet` and is lock-free. `onServerThrottle` is
 * `synchronized` to coordinate `cap` + `ssthresh` + `reducePermits` atomically.
 * `phase` and `ssthresh` are atomics so phase transitions are visible to concurrent
 * `onSuccess` calls without extra synchronization.
 */
private[spark] final class AdaptiveSemaphore(
    initialPermits: Int,
    floor: Int,
    ceiling: Int,
    growthStreak: Int
) extends Semaphore(initialPermits) {

  require(initialPermits >= 1, s"initialPermits must be >= 1, got $initialPermits")
  require(floor >= 1, s"floor must be >= 1, got $floor")
  require(ceiling >= initialPermits, s"ceiling ($ceiling) must be >= initialPermits ($initialPermits)")
  require(growthStreak >= 1, s"growthStreak must be >= 1, got $growthStreak")

  import AdaptiveSemaphore._

  /** Convenience ctor — uses the default `growthStreak` (20). */
  def this(initialPermits: Int, floor: Int, ceiling: Int) =
    this(initialPermits, floor, ceiling, AdaptiveSemaphore.DefaultGrowthStreak)

  private val cap = new AtomicInteger(initialPermits)
  private val ssthresh = new AtomicInteger(ceiling)
  private val phase = new AtomicReference[Phase](SlowStart)
  private val consecutiveSuccesses = new AtomicLong(0L)
  private val lastShrinkNanos = new AtomicLong(0L)

  def onSuccess(): Unit = {
    val streak = consecutiveSuccesses.incrementAndGet()
    val current = cap.get()
    if (current >= ceiling) return
    if (streak % growthStreak != 0) return

    phase.get() match {
      case SlowStart =>
        val sst = ssthresh.get()
        if (current >= sst) {
          phase.compareAndSet(SlowStart, CongestionAvoidance)
          if (cap.compareAndSet(current, current + 1)) release(1)
        } else {
          val target = math.min(math.min(current * 2, sst), ceiling)
          val delta = target - current
          if (delta > 0 && cap.compareAndSet(current, target)) release(delta)
        }
      case CongestionAvoidance =>
        if (cap.compareAndSet(current, current + 1)) release(1)
    }
  }

  def onServerThrottle(): Unit = synchronized {
    consecutiveSuccesses.set(0L)
    if (availablePermits() != 0) return
    val now = System.nanoTime()
    if (now - lastShrinkNanos.get() < ShrinkCooldownNanos) return

    val current = cap.get()
    val target = math.max(floor, current / 2)
    val reduceBy = current - target
    if (reduceBy > 0 && cap.compareAndSet(current, target)) {
      reducePermits(reduceBy)
      ssthresh.set(target)
      phase.set(CongestionAvoidance)
      lastShrinkNanos.set(now)
    }
  }

  def onClientThrottle(): Unit = ()

  def currentCap: Int = cap.get()
  def currentSsthresh: Int = ssthresh.get()
  def currentPhase: Phase = phase.get()
}

private[spark] object AdaptiveSemaphore {
  /** 100ms — prevents the same overload event (which produces bursts of 429s) from
    * collapsing the cap below `floor` in a single burst. TCP RTO_MIN intuition. */
  val ShrinkCooldownNanos: Long = 100L * 1000000L

  /** Successes between growth events. 20 corresponds to ~1 RTT per growth (at 50ms
    * latency × initial cap of 8 = 160 successes/s). Standard TCP slow-start streak. */
  val DefaultGrowthStreak: Int = 20

  sealed trait Phase
  case object SlowStart extends Phase
  case object CongestionAvoidance extends Phase
}
