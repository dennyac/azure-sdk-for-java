// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.spark

import com.azure.cosmos.spark.AdaptiveSemaphore.{CongestionAvoidance, SlowStart}
import com.azure.cosmos.spark.diagnostics.BasicLoggingTrait

/**
 * Unit tests for [[AdaptiveSemaphore]] — the two-phase TCP-Reno-style controller used
 * by [[BulkWriter]] when `spark.cosmos.write.bulk.maxPendingOperations.adaptive = true`.
 *
 * Tests cover:
 *  - Initial state (cap = initialPermits, phase = SlowStart, ssthresh = ceiling).
 *  - Slow-start: cap doubles every `growthStreak` successes.
 *  - Slow-start -> congestion-avoidance transition when cap reaches ssthresh.
 *  - Congestion-avoidance: cap += 1 per `growthStreak` successes.
 *  - Server-throttle: halves cap when saturated (NOT when not saturated), remembers
 *    ssthresh, forces phase transition, respects floor and 100ms cooldown.
 *  - Client-throttle: explicit no-op (doesn't shrink, doesn't reset growth streak).
 *  - Ceiling clamp.
 */
class AdaptiveSemaphoreSpec extends UnitSpec with BasicLoggingTrait {

  private def newSemaphore(initial: Int = 8, floor: Int = 2, ceiling: Int = 1024, streak: Int = 20)
    : AdaptiveSemaphore = new AdaptiveSemaphore(initial, floor, ceiling, streak)

  "AdaptiveSemaphore" should "start in SlowStart at initialPermits" in {
    val sem = newSemaphore()
    sem.availablePermits() shouldBe 8
    sem.currentCap shouldBe 8
    sem.currentPhase shouldBe SlowStart
    sem.currentSsthresh shouldBe 1024
  }

  it should "double the cap every growthStreak successes in SlowStart" in {
    val sem = newSemaphore(initial = 8, streak = 4)
    (1 until 4).foreach(_ => sem.onSuccess())
    sem.currentCap shouldBe 8
    sem.onSuccess() // 4th -> double
    sem.currentCap shouldBe 16
    sem.availablePermits() shouldBe 16
    (1 to 4).foreach(_ => sem.onSuccess())
    sem.currentCap shouldBe 32
    sem.currentPhase shouldBe SlowStart
  }

  it should "transition SlowStart -> CongestionAvoidance when cap reaches ssthresh" in {
    val sem = newSemaphore(initial = 8, ceiling = 16, streak = 1)
    sem.onSuccess() // doubles to 16 (= ssthresh = ceiling)
    sem.currentCap shouldBe 16
    sem.onSuccess() // cap >= ssthresh -> transitions
    sem.currentPhase shouldBe CongestionAvoidance
  }

  it should "grow additively (+1 per growthStreak) in CongestionAvoidance" in {
    val sem = newSemaphore(initial = 8, ceiling = 100, streak = 1)
    // Force into CongestionAvoidance via a 429
    (1 to 8).foreach(_ => sem.acquire())
    sem.onServerThrottle()
    sem.currentPhase shouldBe CongestionAvoidance
    val capBefore = sem.currentCap
    (1 to 8).foreach(_ => sem.release()) // pay down the deficit so permits are available
    // Now in CA. Each success +1 (streak = 1).
    sem.onSuccess()
    sem.currentCap shouldBe (capBefore + 1)
    sem.onSuccess()
    sem.currentCap shouldBe (capBefore + 2)
  }

  it should "halve cap on server throttle when saturated, remember ssthresh, force phase" in {
    val sem = newSemaphore(initial = 64, floor = 2)
    (1 to 64).foreach(_ => sem.acquire())
    sem.availablePermits() shouldBe 0
    sem.onServerThrottle()
    sem.currentCap shouldBe 32
    sem.currentSsthresh shouldBe 32
    sem.currentPhase shouldBe CongestionAvoidance
  }

  it should "NOT shrink on server throttle when not saturated" in {
    val sem = newSemaphore(initial = 64)
    sem.acquire(); sem.acquire() // 2 used, 62 free
    sem.onServerThrottle()
    sem.currentCap shouldBe 64
    sem.availablePermits() shouldBe 62
  }

  it should "respect the floor when halving" in {
    val sem = newSemaphore(initial = 4, floor = 3)
    (1 to 4).foreach(_ => sem.acquire())
    sem.onServerThrottle()
    sem.currentCap shouldBe 3 // floor honoured, not 2
  }

  it should "respect the 100ms shrink cooldown" in {
    val sem = newSemaphore(initial = 64)
    (1 to 64).foreach(_ => sem.acquire())
    sem.onServerThrottle()
    sem.currentCap shouldBe 32
    sem.onServerThrottle() // within cooldown — no-op
    sem.currentCap shouldBe 32
    Thread.sleep(120)
    sem.onServerThrottle()
    sem.currentCap shouldBe 16
  }

  it should "ignore client-throttle (no shrink, no reset of growth streak)" in {
    val sem = newSemaphore(initial = 8, streak = 4)
    sem.onSuccess(); sem.onSuccess(); sem.onSuccess() // streak = 3
    sem.onClientThrottle()
    sem.onSuccess() // streak = 4 -> double (still in SlowStart)
    sem.currentCap shouldBe 16
    sem.currentPhase shouldBe SlowStart
  }

  it should "reset the success streak on server throttle (regardless of saturation)" in {
    val sem = newSemaphore(initial = 8, streak = 4)
    sem.onSuccess(); sem.onSuccess(); sem.onSuccess()
    sem.acquire() // 1 used, 7 free
    sem.onServerThrottle() // not saturated; no shrink, BUT streak reset
    sem.onSuccess()
    sem.currentCap shouldBe 8 // streak = 1, no grow
  }

  it should "respect the ceiling across both phases" in {
    val sem = newSemaphore(initial = 8, ceiling = 10, streak = 1)
    (1 to 50).foreach(_ => sem.onSuccess())
    sem.currentCap shouldBe 10
    sem.availablePermits() shouldBe 10
  }
}
