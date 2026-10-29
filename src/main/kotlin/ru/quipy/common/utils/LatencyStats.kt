package ru.quipy.common.utils

import kotlin.math.sqrt

/**
 * Статистика по последним [capacity] замерам длительности (в мс): среднее, стандартное отклонение и максимум.
 * Пока замеров меньше [minSamples], snapshot() возвращает null.
 */
class LatencyStats(
    private val capacity: Int = 1000,
    private val minSamples: Int = 20,
) {
    private val samples = LongArray(capacity)
    private var count = 0
    private var next = 0
    private var cached: Snapshot? = null

    @Synchronized
    fun record(millis: Long) {
        samples[next] = millis
        next = (next + 1) % capacity
        if (count < capacity) count++
        cached = null
    }

    @Synchronized
    fun snapshot(): Snapshot? {
        if (count < minSamples) return null
        cached?.let { return it }

        val window = samples.copyOf(count)
        val mean = window.average()
        val std = sqrt(window.sumOf { (it - mean) * (it - mean) } / count)
        return Snapshot(mean, std, window.max()).also { cached = it }
    }

    data class Snapshot(val meanMs: Double, val stdMs: Double, val maxMs: Long)
}
