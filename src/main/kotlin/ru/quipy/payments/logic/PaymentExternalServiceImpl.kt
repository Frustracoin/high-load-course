package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore


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

    // Окно чуть больше секунды: запас на сетевой джиттер, чтобы провайдер не увидел > rateLimitPerSec в своём окне
    private val rateLimitWindow = Duration.ofMillis(1100)
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), rateLimitWindow)

    // Не больше parallelRequests запросов одновременно в полёте; fair - чтобы ожидающие шли по FIFO и ранние оплаты не опоздали к дедлайну
    private val parallelLimiter = Semaphore(parallelRequests, true)

    private val providerRequests = Metrics.counter("shop_provider_requests", "account", accountName)

    // Сколько оплат в секунду реально пропускает аккаунт: упираемся либо в наш rate limiter, либо в окно parallelRequests
    private val rateBound = rateLimitPerSec * 1000.0 / rateLimitWindow.toMillis()
    private val windowBound = parallelRequests * 1000.0 / requestAverageProcessingTime.toMillis()
    private val throughputPerSec = minOf(rateBound, windowBound)

    // Время ответа равномерно от 0 до 2 * averageProcessingTime, поэтому своя оплата займёт не больше 2 * avg.
    // Если упираемся в окно, слоты освобождаются в случайные моменты и ожидание в очереди тоже разбросано: ещё 2 * avg.
    // Если упираемся в rate limit, разрешения выдаются ровно по времени и ожидание предсказуемо.
    // Плюс накладные расходы вокруг вызова: запись платежа и событий в БД, HTTP до магазина (по метрикам ~100-150 мс)
    private val processingBudget = requestAverageProcessingTime.multipliedBy(if (windowBound < rateBound) 4 else 2)
        .plus(Duration.ofMillis(200))

    // Закон Литтла: чтобы держать throughputPerSec при среднем времени avg, одновременно нужно throughputPerSec * avg вызовов (+10% запаса).
    // Больше parallelRequests брать бессмысленно: лишние потоки просто ждали бы на семафоре
    override fun parallelism(): Int {
        val inFlight = Math.ceil(throughputPerSec * requestAverageProcessingTime.toMillis() / 1000.0).toInt()
        return minOf(parallelRequests, inFlight + Math.ceil(inFlight * 0.1).toInt())
    }

    override fun canProcessInTime(queued: Int, deadline: Long): Boolean {
        // Перед нами queued оплат в очереди, они уходят к провайдеру со скоростью throughputPerSec
        val expectedFinish = now() + ((queued + 1) * 1000 / throughputPerSec).toLong() + processingBudget.toMillis()
        return expectedFinish <= deadline
    }

    // В среднем место в очереди освобождается раз в 1 / throughputPerSec секунд
    override fun retryAfter() = now() + (1000 / throughputPerSec).toLong()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
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