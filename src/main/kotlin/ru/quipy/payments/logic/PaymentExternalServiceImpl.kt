package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tags
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LatencyStats
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.SlidingWindowReservations
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.sqrt


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        // Число попыток ограничиваем всегда: дедлайн ограничивает время, но при быстрых ошибках не число платных вызовов
        private const val MAX_ATTEMPTS = 3
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // Повторы делаем сами, с учётом лимитов и дедлайна; встроенный в OkHttp повтор при обрыве соединения
    // отправил бы тот же запрос ещё раз в обход лимитера (и мог бы списать оплату дважды)
    private val client = OkHttpClient.Builder().retryOnConnectionFailure(false).build()

    // Лимит провайдера - rateLimitPerSec за секунду, используем его полностью
    private val rateLimitWindow = Duration.ofSeconds(1)
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), rateLimitWindow)

    // Не больше parallelRequests запросов одновременно в полёте; fair - чтобы ожидающие шли по FIFO и ранние оплаты не опоздали к дедлайну
    private val parallelLimiter = Semaphore(parallelRequests, true)

    private val providerRequests = Metrics.counter("shop_provider_requests", "account", accountName)

    private val retries = Metrics.counter("shop_payment_retries", "account", accountName)

    // Сколько оплат в секунду реально пропускает аккаунт: упираемся либо в наш rate limiter, либо в окно parallelRequests
    private val rateBound = rateLimitPerSec * 1000.0 / rateLimitWindow.toMillis()
    private val windowBound = parallelRequests * 1000.0 / requestAverageProcessingTime.toMillis()
    private val throughputPerSec = minOf(rateBound, windowBound)

    // Входной лимитер: бронирует каждой принятой оплате момент отправки к провайдеру.
    // Если упираемся в rate limit - то же скользящее окно, что и на выходе, но длиннее на шаг опроса выходного лимитера:
    // к моменту брони разрешение там уже точно освободилось, и запросы не накапливают отставание от расписания.
    // Этот же зазор - запас на разброс доставки, ведь провайдер считает лимит по времени прихода.
    // Если упираемся в окно - parallelRequests отправок за averageProcessingTime (столько освобождается слотов семафора)
    private val sendSchedule = if (windowBound < rateBound) {
        SlidingWindowReservations(parallelRequests, requestAverageProcessingTime)
    } else {
        SlidingWindowReservations(rateLimitPerSec, rateLimitWindow + SlidingWindowRateLimiter.POLL_INTERVAL)
    }

    // Сколько реально длится оплата у этого аккаунта (от забронированного момента отправки до ответа провайдера): меряем на лету
    private val executionStats = LatencyStats()

    // До первых замеров опираемся на то, что обещает провайдер: время ответа в среднем avg,
    // считаем его равномерным от 0 до 2 * avg (это же даёт и худшее время 2 * avg)
    private val declaredStats = requestAverageProcessingTime.toMillis().let { avg ->
        LatencyStats.Snapshot(meanMs = avg.toDouble(), stdMs = avg / sqrt(3.0), maxMs = 2 * avg)
    }

    init {
        Metrics.gauge("shop_payment_execution_max_ms", Tags.of("account", accountName), executionStats) {
            it.snapshot()?.maxMs?.toDouble() ?: 0.0
        }
    }

    // Закон Литтла с худшим временем вызова: одновременно в полёте до throughputPerSec * 2 * avg вызовов
    // (провайдер обещает среднее avg, худшее по замерам ~2 * avg). Больше parallelRequests брать бессмысленно
    override fun parallelism(): Int {
        val inFlight = Math.ceil(throughputPerSec * 2 * requestAverageProcessingTime.toMillis() / 1000.0).toInt()
        return minOf(parallelRequests, inFlight)
    }

    // Худшее время оплаты: замеренное, но не меньше обещанного провайдером (время ответа до 2 * avg)
    private fun worstExecutionMs(stats: LatencyStats.Snapshot) = maxOf(stats.maxMs, declaredStats.maxMs)

    override fun reserveSendSlot(deadline: Long): Long? {
        val now = now()
        val stats = executionStats.snapshot() ?: declaredStats
        return sendSchedule.reserve(now) { slot -> slot + waitJitter(slot - now, stats) + worstExecutionMs(stats) <= deadline }
    }

    // Если упираемся в окно, слоты освобождаются в случайные моменты (когда кто-то из parallelRequests закончил),
    // и ожидание разбросано на sigma * sqrt(впереди + 1) / parallelRequests. Берём 3 сигмы.
    // Если упираемся в rate limit, разрешения выдаются строго по времени и разброса нет
    private fun waitJitter(waitMs: Long, stats: LatencyStats.Snapshot): Double {
        if (windowBound >= rateBound) return 0.0
        val ahead = waitMs * throughputPerSec / 1000
        return 3 * stats.stdMs * sqrt(ahead + 1.0) / parallelRequests
    }

    // Когда запрос с тем же запасом времени до дедлайна уже поместится: ближайший слот минус этот запас
    override fun retryAfter(deadline: Long): Long {
        val now = now()
        val budget = deadline - now - worstExecutionMs(executionStats.snapshot() ?: declaredStats)
        return maxOf(now + (1000 / throughputPerSec).toLong(), sendSchedule.nextSlot(now) - budget)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, sendAt: Long) {
        // Повторяем временные ошибки, пока новая попытка успевает до дедлайна: её момент отправки бронируем так же,
        // как для первой, поэтому повторы не нарушают лимиты аккаунта и не отнимают слоты у уже принятых оплат.
        // Число попыток ограничено и явно: при быстрых ошибках один только дедлайн разрешил бы много платных попыток подряд
        var attemptSendAt = sendAt
        for (attempt in 1..MAX_ATTEMPTS) {
            if (attemptPayment(paymentId, amount, paymentStartedAt, deadline, attemptSendAt) != AttemptResult.RETRIABLE_FAILURE) return
            if (attempt == MAX_ATTEMPTS) return

            attemptSendAt = reserveSendSlot(deadline) ?: return
            retries.increment()
            Thread.sleep(maxOf(0, attemptSendAt - now()))
        }
    }

    private enum class AttemptResult { SUCCESS, RETRIABLE_FAILURE, FAILURE }

    private fun attemptPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, sendAt: Long): AttemptResult {
        // Оплату засчитывает сам провайдер в момент ответа, поэтому меряем от забронированного момента до получения ответа:
        // сюда попадают и задержка старта задачи, и запись в базу перед отправкой, а запись результата после ответа - нет
        val startedAt = sendAt
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        parallelLimiter.acquire()
        val (code, rawBody) = try {
            rateLimiter.tickBlocking()

            // Дедлайн уже прошёл - успеха не будет, а вызов платный: не отправляем
            if (now() >= deadline) {
                logProcessing(paymentId, transactionId, "Deadline exceeded before sending.")
                return AttemptResult.FAILURE
            }

            // Deadline propagation: после дедлайна ответ уже никому не нужен - ни нам, ни провайдеру
            val timeout = Duration.ofMillis(deadline - now())
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$timeout")
                post(emptyBody)
            }.build()

            providerRequests.increment()
            val call = client.newCall(request).apply { timeout().timeout(timeout.toMillis(), TimeUnit.MILLISECONDS) }
            call.execute().use { response -> response.code to response.body?.string() }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            logProcessing(paymentId, transactionId, "Interrupted.")
            return AttemptResult.FAILURE
        } catch (e: IOException) {
            // Таймаут или сетевая ошибка: ответа нет, это временный сбой
            executionStats.record(now() - startedAt)
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            logProcessing(paymentId, transactionId, if (e is InterruptedIOException) "Request timeout." else e.message)
            return if (Thread.currentThread().isInterrupted) AttemptResult.FAILURE else AttemptResult.RETRIABLE_FAILURE
        } finally {
            parallelLimiter.release()
        }
        executionStats.record(now() - startedAt)

        val body = try {
            mapper.readValue(rawBody, ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: $code, reason: $rawBody")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, rawBody)
        }

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

        // Исход решаем по ответу провайдера, до записи в базу: прошедшую оплату нельзя повторять, даже если запись упадёт.
        // Временные ошибки: 2xx с result=false ("Temporary error"), 429 (лимиты), 5xx (сбой на стороне провайдера).
        // Остальные 4xx - ошибка в самом запросе, повтор не поможет
        val result = when {
            body.result -> AttemptResult.SUCCESS
            code == 429 || code >= 500 || code in 200..299 -> AttemptResult.RETRIABLE_FAILURE
            else -> AttemptResult.FAILURE
        }

        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }
        return result
    }

    private fun logProcessing(paymentId: UUID, transactionId: UUID, reason: String?) {
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()