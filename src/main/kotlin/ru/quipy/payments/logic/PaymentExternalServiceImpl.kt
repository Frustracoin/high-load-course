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
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
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
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    // Провайдер считает лимит за секунду по своим часам (время прихода), а мы - по времени отправки.
    // Одно разрешение оставляем на запрос, который сетевая задержка может сдвинуть в соседнее окно провайдера
    private val rateLimitWindow = Duration.ofSeconds(1)
    private val ourRateLimit = rateLimitPerSec - 1
    private val rateLimiter = SlidingWindowRateLimiter(ourRateLimit.toLong(), rateLimitWindow)

    // Не больше parallelRequests запросов одновременно в полёте; fair - чтобы ожидающие шли по FIFO и ранние оплаты не опоздали к дедлайну
    private val parallelLimiter = Semaphore(parallelRequests, true)

    private val providerRequests = Metrics.counter("shop_provider_requests", "account", accountName)

    // Сколько оплат в секунду реально пропускает аккаунт: упираемся либо в наш rate limiter, либо в окно parallelRequests
    private val rateBound = ourRateLimit * 1000.0 / rateLimitWindow.toMillis()
    private val windowBound = parallelRequests * 1000.0 / requestAverageProcessingTime.toMillis()
    private val throughputPerSec = minOf(rateBound, windowBound)

    // Сколько реально длится оплата у этого аккаунта (от взятия из очереди до ответа провайдера): меряем на лету
    private val executionStats = LatencyStats()

    init {
        Metrics.gauge("shop_payment_execution_max_ms", Tags.of("account", accountName), executionStats) {
            it.snapshot()?.maxMs?.toDouble() ?: 0.0
        }
    }

    // Закон Литтла: чтобы держать throughputPerSec при среднем времени avg, одновременно нужно throughputPerSec * avg вызовов (+10% запаса).
    // Больше parallelRequests брать бессмысленно: лишние потоки просто ждали бы на семафоре
    override fun parallelism(): Int {
        val inFlight = Math.ceil(throughputPerSec * requestAverageProcessingTime.toMillis() / 1000.0).toInt()
        return minOf(parallelRequests, inFlight + Math.ceil(inFlight * 0.1).toInt())
    }

    override fun canProcessInTime(queued: Int, deadline: Long): Boolean {
        // Пока замеров нет (холодный старт), ничего не знаем о времени выполнения: в очередь не ставим, берём только на свободный поток
        val stats = executionStats.snapshot() ?: return queued == 0

        // Перед нами queued оплат в очереди, они уходят к провайдеру со скоростью throughputPerSec
        val waitInQueue = (queued + 1) * 1000 / throughputPerSec

        // Если упираемся в окно, места освобождаются в случайные моменты (когда кто-то из parallelRequests закончил),
        // и ожидание (queued + 1) освобождений разбросано на sigma * sqrt(queued + 1) / parallelRequests. Берём 3 сигмы.
        // Если упираемся в rate limit, разрешения выдаются строго по времени и разброса нет
        val waitJitter = if (windowBound < rateBound) {
            3 * stats.stdMs * sqrt(queued + 1.0) / parallelRequests
        } else 0.0

        // Своя оплата: худшее замеренное время из последних 1000 оплат
        val execution = stats.maxMs

        return now() + waitInQueue + waitJitter + execution <= deadline
    }

    // В среднем место в очереди освобождается раз в 1 / throughputPerSec секунд
    override fun retryAfter() = now() + (1000 / throughputPerSec).toLong()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val startedAt = now()
        try {
            performPayment(paymentId, amount, paymentStartedAt)
        } finally {
            executionStats.record(now() - startedAt)
        }
    }

    private fun performPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        parallelLimiter.acquire()
        try {
            rateLimiter.tickBlocking()

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            providerRequests.increment()
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            parallelLimiter.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()