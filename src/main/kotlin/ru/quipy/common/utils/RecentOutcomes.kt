package ru.quipy.common.utils

import java.time.Duration

/**
 * Исходы (успех / неуспех) с моментом записи. successRateOver(horizon) - доля успешных среди записанных не раньше чем horizon назад.
 * Хранятся только исходы в пределах последнего запрошенного горизонта.
 */
class RecentOutcomes {
    private val outcomes = ArrayDeque<Outcome>()
    private var horizon = Duration.ZERO

    @Synchronized
    fun record(success: Boolean) {
        outcomes.addLast(Outcome(System.currentTimeMillis(), success))
        dropOlderThan(horizon)
    }

    @Synchronized
    fun successRateOver(horizon: Duration): Double? {
        this.horizon = horizon
        dropOlderThan(horizon)
        if (outcomes.isEmpty()) return null
        return outcomes.count { it.success }.toDouble() / outcomes.size
    }

    private fun dropOlderThan(horizon: Duration) {
        val oldest = System.currentTimeMillis() - horizon.toMillis()
        while (outcomes.isNotEmpty() && outcomes.first().recordedAt < oldest) outcomes.removeFirst()
    }

    private data class Outcome(val recordedAt: Long, val success: Boolean)
}
