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
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), rateLimitWindow)

    // Не больше parallelRequests запросов одновременно в полёте; fair - чтобы ожидающие шли по FIFO и ранние оплаты не опоздали к дедлайну
    private val parallelLimiter = Semaphore(parallelRequests, true)

    private val providerRequests = Metrics.counter("shop_provider_requests", "account", accountName)

    private val retries = Metrics.counter("shop_payment_retries", "account", accountName)

    // Сколько оплат в секунду реально пропускает аккаунт: упираемся либо в наш rate limiter, либо в окно parallelRequests
    private val rateBound = rateLimitPerSec * 1000.0 / rateLimitWindow.toMillis()
    private val windowBound = parallelRequests * 1000.0 / requestAverageProcessingTime.toMillis()
    private val throughputPerSec = minOf(rateBound, windowBound)

    // Худшее время ответа: провайдер обещает среднее averageProcessingTime, ответ разбросан от нуля - значит, не дольше 2 * avg
    private val worstProcessingTime = requestAverageProcessingTime.multipliedBy(2)

    // Входной лимитер - leaky bucket в форме GCRA: оплаты уходят к провайдеру ровно по одной раз в period.
    // Тогда k-я в очереди уйдёт не позже now + k * period, и можно заранее сказать, успеет ли она к дедлайну
    private val sendPeriodMillis = Math.ceil(1000 / throughputPerSec).toLong()
    private var nextSendAt = 0L

    // Пул не должен становиться узким местом: столько потоков, сколько провайдер разрешает запросов одновременно
    override fun parallelism(): Int = parallelRequests

    // Берём оплату, только если даже самый долгий ответ после её момента отправки успеет к дедлайну
    @Synchronized
    override fun reserveSendSlot(deadline: Long): Long? {
        val slot = maxOf(now(), nextSendAt)
        if (slot + worstProcessingTime.toMillis() > deadline) return null
        nextSendAt = slot + sendPeriodMillis
        return slot
    }

    // Когда запрос с тем же запасом времени до дедлайна уже поместится: ближайший момент отправки минус этот запас
    @Synchronized
    override fun retryAfter(deadline: Long): Long {
        val now = now()
        val budget = deadline - now - worstProcessingTime.toMillis()
        return maxOf(now + sendPeriodMillis, nextSendAt - budget)
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
            val timeout = Duration.ofMillis(deadline - now())
            if (timeout <= Duration.ZERO) {
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Deadline exceeded before sending.")
            } else {
                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$timeout")
                    post(emptyBody)
                }.build()

                providerRequests.increment()
                val call = client.newCall(request).apply { timeout().timeout(timeout.toMillis(), TimeUnit.MILLISECONDS) }
                call.execute().use { response ->
                    val rawBody = response.body?.string()
                    try {
                        mapper.readValue(rawBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "HTTP ${response.code}: $rawBody")
                    }
                }
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