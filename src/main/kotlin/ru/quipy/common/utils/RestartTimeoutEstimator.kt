package ru.quipy.common.utils

import java.time.Duration

/**
 * Тайм-аут попытки, при котором успешный ответ обходится дешевле всего.
 *
 * Попытка с тайм-аутом t длится min(D, t), где D - сколько длился бы вызов без обрыва, и удаётся, только если
 * успешный ответ пришёл не позже t (зависание или ответ с ошибкой успеха не дают). Если повторять до успеха,
 * на один успешный ответ в среднем уходит g(t) = E[min(D, t)] / P(успех не позже t). Это и ожидаемое время
 * до успеха для одной оплаты, и время слота parallelRequests на одну успешную оплату. Тайм-аут - argmin g(t).
 * Множителей и квантилей тут нет: слишком короткий t теряет ответы, которые пришли бы (падает знаменатель),
 * слишком длинный держит зависшие вызовы (растёт числитель), и g сам взвешивает одно против другого.
 *
 * Скачком вниз g меняется только в моменты успешных ответов, а между ними растёт, поэтому минимум
 * достигается на длительности одного из успешных ответов выборки - их и перебираем.
 *
 * Тонкость: при тайм-ауте T ответы позже T не видны, и по такой выборке g(t) известна только для t <= T.
 * Если пересчитывать argmin по ней же, он может только уменьшаться, и тайм-аут сползает вниз.
 * Поэтому две фазы:
 *  - разведка: тайм-аут = ceiling (дольше ответа не бывает), выборка видит всё распределение. По ней
 *    выбираем t* = argmin g(t), но не ниже floor, и запоминаем цену успеха без обрыва g(ceiling);
 *  - работа: тайм-аут = t*, выборка нужна только для проверки. С момента выбора t* копим сумму min(D, t*)
 *    и число успехов и раз в windowSize попыток сравниваем накопленную цену успеха с g(ceiling). Если она выше,
 *    обрыв на t* уже хуже, чем вовсе не обрывать (провайдер стал отвечать дольше), - снова разведка.
 *    Копим, а не берём последнее окно: g(ceiling) и цена при t* оценены по разным выборкам, и на одном окне
 *    их шум того же порядка, что и разница между ними, - строгое сравнение срабатывало бы без смены профиля.
 *    С накоплением шум со временем уходит; цена - настоящую смену профиля замечаем тем позже, чем дольше фаза.
 *
 * Попытки, у которых тайм-аут был не текущий (урезан дедлайном или попытка начата до смены фазы),
 * не учитываются: о текущем тайм-ауте они ничего не говорят.
 */
class RestartTimeoutEstimator(
    private val ceiling: Duration,
    private val floor: Duration,
    private val windowSize: Int,
) {
    private val durations = LongArray(windowSize)
    private val successes = BooleanArray(windowSize)
    private var filled = 0
    private var exploring = true
    // Цена успеха без обрыва по последней разведке
    private var baselineCost = Double.POSITIVE_INFINITY
    // Накоплено в фазе работы с момента выбора t*: сумма min(D, t*) и число успешных ответов
    private var workTotalMs = 0L
    private var workSuccesses = 0L

    @Volatile
    private var current: Duration = ceiling

    init {
        require(windowSize > 0) { "Empty window" }
        require(floor <= ceiling) { "Floor $floor is above ceiling $ceiling" }
    }

    fun timeout(): Duration = current

    /** Попытка с тайм-аутом [timeout] длилась [durationMs] и закончилась успешным ответом или нет. */
    @Synchronized
    fun record(timeout: Duration, durationMs: Long, success: Boolean) {
        if (timeout != current) return
        durations[filled] = durationMs
        successes[filled] = success
        if (++filled < windowSize) return
        filled = 0
        if (exploring) learn() else check()
    }

    // Выборка при тайм-ауте ceiling: выбираем t*
    private fun learn() {
        val cap = ceiling.toMillis()
        val successCount = successes.count { it }
        // Ни одного успеха - выбирать не из чего, разведываем дальше
        if (successCount == 0) return
        baselineCost = (0 until windowSize).sumOf { minOf(durations[it], cap) }.toDouble() / successCount
        var best = cap
        var bestCost = baselineCost
        // Перебираем t по возрастанию: prefix - сумма длительностей попыток, закончившихся не позже t,
        // остальные попытки при тайм-ауте t длились бы ровно t
        var prefix = 0L
        var successesUpToT = 0
        for ((rank, i) in (0 until windowSize).sortedBy { durations[it] }.withIndex()) {
            val d = minOf(durations[i], cap)
            prefix += d
            if (!successes[i]) continue
            successesUpToT++
            val cost = (prefix + (windowSize - rank - 1).toLong() * d).toDouble() / successesUpToT
            if (cost < bestCost) {
                bestCost = cost
                best = d
            }
        }
        current = Duration.ofMillis(maxOf(best, floor.toMillis()))
        workTotalMs = 0L
        workSuccesses = 0L
        exploring = false
    }

    // Выборка при тайм-ауте t*: проверяем, что обрыв всё ещё выгоднее, чем ждать до ceiling
    private fun check() {
        val t = current.toMillis()
        // Окно только пополняет накопленное с момента выбора t*, сравниваем по всей фазе работы
        for (i in 0 until windowSize) {
            workTotalMs += minOf(durations[i], t)
            if (successes[i]) workSuccesses++
        }
        val cost = if (workSuccesses == 0L) {
            Double.POSITIVE_INFINITY
        } else {
            workTotalMs.toDouble() / workSuccesses
        }
        if (cost > baselineCost) {
            current = ceiling
            exploring = true
        }
    }
}
