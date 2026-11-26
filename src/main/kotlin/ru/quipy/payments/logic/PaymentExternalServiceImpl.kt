package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.SlidingWindowReservations
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


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
    // Провайдер считает окно по моменту прихода запроса, а наш лимитер - по моменту tick перед отправкой.
    // При окне ровно rateLimitWindow запрос, ушедший сразу после освобождения слота, из-за разброса задержек
    // может прийти к провайдеру раньше, чем самый старый выпал из его окна (c7-v4: 1 rate_limit_breached на первой попытке).
    // Расписание sendSchedule уже держит запас POLL_INTERVAL, но повторы после таймаутов идут мимо него, только через
    // этот лимитер, поэтому тот же запас нужен и здесь. Цена - ~1% пропускной способности (rateLimitPerSec за 1.01 с).
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), rateLimitWindow + SlidingWindowRateLimiter.POLL_INTERVAL)

    // Не больше parallelRequests запросов одновременно в полёте; fair - чтобы ожидающие шли по FIFO и ранние оплаты не опоздали к дедлайну
    private val parallelLimiter = Semaphore(parallelRequests, true)

    private val providerRequests = Metrics.counter("shop_provider_requests", "account", accountName)

    private val retries = Metrics.counter("shop_payment_retries", "account", accountName)

    // Длительность одного вызова провайдера, отдельно для успешных и неуспешных (включая оборванные по таймауту).
    // По квантилям успешных выбираем таймаут попытки: дольше них ждать бесполезно
    private fun providerLatency(outcome: String) = Timer.builder("shop_provider_request_duration")
        .tag("account", accountName)
        .tag("outcome", outcome)
        .publishPercentiles(0.5, 0.8, 0.9, 0.95, 0.99)
        .publishPercentileHistogram()
        .register(Metrics.globalRegistry)
    private val successLatency = providerLatency("success")
    private val failureLatency = providerLatency("failure")

    // Тайм-аут одной попытки не константа: он считается на ходу по квантилям успешных ответов (successLatency).
    // Дольше p99 успешных ответов ждать бесполезно: если ответа к этому моменту нет, дальше обычно только зависание.
    // Micrometer считает квантили по скользящему окну (distributionStatisticExpiry, по умолчанию 2 мин),
    // поэтому тайм-аут следует за текущим профилем задержек провайдера и подходит любому аккаунту.
    // Запас нужен, потому что попытки обрываем мы сами и ответы дольше тайм-аута в выборку успешных не попадают:
    // без запаса p99 успешных, а вместе с ним и тайм-аут, сползали бы вниз.
    private val attemptTimeoutMargin = 1.2
    // Пока успешных замеров мало (холодный старт) или окно квантилей пустое - 2·avg из свойств аккаунта
    private val attemptTimeoutMinSamples = 100L
    private val coldStartAttemptTimeout = requestAverageProcessingTime.multipliedBy(2)
    // Квантили пересчитываем не на каждую попытку, а не чаще раза в секунду: снимок гистограммы не бесплатный
    private val attemptTimeoutRefresh = Duration.ofSeconds(1)
    @Volatile private var currentAttemptTimeout = coldStartAttemptTimeout
    @Volatile private var attemptTimeoutUpdatedAt = 0L

    init {
        // Текущий тайм-аут попытки - в Grafana
        Gauge.builder("shop_provider_attempt_timeout", this) { it.currentAttemptTimeout.toMillis().toDouble() }
            .tag("account", accountName)
            .baseUnit("milliseconds")
            .register(Metrics.globalRegistry)
    }

    private fun attemptTimeout(): Duration {
        val now = now()
        if (now - attemptTimeoutUpdatedAt >= attemptTimeoutRefresh.toMillis()) {
            attemptTimeoutUpdatedAt = now
            currentAttemptTimeout = computeAttemptTimeout()
        }
        return currentAttemptTimeout
    }

    private fun computeAttemptTimeout(): Duration {
        if (successLatency.count() < attemptTimeoutMinSamples) return coldStartAttemptTimeout
        val p99 = successLatency.takeSnapshot().percentileValues()
            .firstOrNull { it.percentile() == 0.99 }
            ?.value(TimeUnit.MILLISECONDS)
        if (p99 == null || p99.isNaN() || p99 <= 0.0) return coldStartAttemptTimeout
        // Ниже averageProcessingTime не опускаемся: обрывать попытки быстрее среднего ответа провайдера нельзя
        return maxOf(Duration.ofMillis((p99 * attemptTimeoutMargin).toLong()), requestAverageProcessingTime)
    }

    // Сколько оплат в секунду реально пропускает аккаунт: упираемся либо в наш rate limiter, либо в окно parallelRequests
    private val rateBound = rateLimitPerSec * 1000.0 / rateLimitWindow.toMillis()
    private val windowBound = parallelRequests * 1000.0 / requestAverageProcessingTime.toMillis()
    private val throughputPerSec = minOf(rateBound, windowBound)

    // Худшее время ответа: провайдер обещает среднее averageProcessingTime, ответ разбросан от нуля - значит, не дольше 2 * avg
    private val worstProcessingTime = requestAverageProcessingTime.multipliedBy(2)

    // Входной лимитер: бронирует каждой принятой оплате момент отправки к провайдеру.
    // Если упираемся в rate limit - то же скользящее окно, что и на выходе, но длиннее на шаг опроса выходного лимитера:
    // к моменту брони разрешение там уже точно освободилось, и запросы не накапливают отставание от расписания.
    // Этот же зазор - запас на разброс доставки, ведь провайдер считает лимит по времени прихода.
    // Если упираемся в окно - parallelRequests отправок за averageProcessingTime (столько освобождается мест в окне)
    private val sendSchedule = SlidingWindowReservations(if (windowBound < rateBound) parallelRequests else rateLimitPerSec)
    private val scheduleWindow = if (windowBound < rateBound) {
        requestAverageProcessingTime
    } else {
        rateLimitWindow + SlidingWindowRateLimiter.POLL_INTERVAL
    }

    // Пул не должен становиться узким местом: столько потоков, сколько провайдер разрешает запросов одновременно
    override fun parallelism(): Int = parallelRequests

    // Берём оплату, только если даже самый долгий ответ после забронированного момента успеет к дедлайну
    override fun reserveSendSlot(deadline: Long): Long? =
        sendSchedule.reserve(now(), scheduleWindow) { slot -> slot + worstProcessingTime.toMillis() <= deadline }

    // Когда запрос с тем же запасом времени до дедлайна уже поместится: ближайший слот минус этот запас
    override fun retryAfter(deadline: Long): Long {
        val now = now()
        val budget = deadline - now - worstProcessingTime.toMillis()
        return maxOf(now + (1000 / throughputPerSec).toLong(), sendSchedule.nextSlot(now, scheduleWindow) - budget)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        // Ошибки провайдера случайны и не связаны с нагрузкой, поэтому неудачную попытку сразу повторяем, пока не истёк дедлайн.
        // Повтор идёт через те же rate limiter и семафор, что и первая попытка, - лимиты аккаунта не нарушаются
        while (!attemptPayment(paymentId, amount, paymentStartedAt, deadline) && now() < deadline) {
            retries.increment()
        }
    }

    private fun attemptPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): Boolean {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        // Слот провайдера держим только на время самого вызова: запись результата в базу идёт уже после
        parallelLimiter.acquire()
        val body = try {
            rateLimiter.tickBlocking()

            // Deadline propagation: после дедлайна ответ уже никому не нужен - ни нам, ни провайдеру.
            // Если дедлайн уже прошёл, платный вызов не делаем (да и timeout(0) у OkHttp значит "без ограничения")
            // Таймаут попытки: долгий хвост ответов обрываем на attemptTimeout (выбран по квантилям, см. выше), чтобы зависший вызов не держал
            // место в окне до самого дедлайна и оставалось время на повтор. Но не дольше, чем осталось до дедлайна
            val timeout = minOf(attemptTimeout(), Duration.ofMillis(deadline - now()))
            if (timeout <= Duration.ZERO) {
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Deadline exceeded before sending.")
            } else {
                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$timeout")
                    post(emptyBody)
                }.build()

                providerRequests.increment()
                val call = client.newCall(request).apply { timeout().timeout(timeout.toMillis(), TimeUnit.MILLISECONDS) }
                // Замеряем каждый вызов, в том числе оборванный по таймауту
                val startedAt = System.nanoTime()
                val response = try {
                    call.execute().use { response ->
                        val rawBody = response.body?.string()
                        try {
                            mapper.readValue(rawBody, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "HTTP ${response.code}: $rawBody")
                        }
                    }
                } catch (e: Exception) {
                    failureLatency.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS)
                    throw e
                }
                (if (response.result) successLatency else failureLatency).record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS)
                response
            }
        } catch (e: Exception) {
            // Таймаут, обрыв соединения и т.п.
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message ?: e.javaClass.simpleName)
        } finally {
            parallelLimiter.release()
        }

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }
        return body.result
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()