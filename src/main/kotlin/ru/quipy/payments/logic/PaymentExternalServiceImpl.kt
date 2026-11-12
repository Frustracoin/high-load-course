package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.SlidingWindowReservations
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

    // Лимит провайдера - rateLimitPerSec за секунду, используем его полностью
    private val rateLimitWindow = Duration.ofSeconds(1)
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), rateLimitWindow)

    // Не больше parallelRequests запросов одновременно в полёте; fair - чтобы ожидающие шли по FIFO и ранние оплаты не опоздали к дедлайну
    private val parallelLimiter = Semaphore(parallelRequests, true)

    private val providerRequests = Metrics.counter("shop_provider_requests", "account", accountName)

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
        val (code, rawBody) = try {
            rateLimiter.tickBlocking()

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            providerRequests.increment()
            client.newCall(request).execute().use { response -> response.code to response.body?.string() }
        } catch (e: Exception) {
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = if (e is SocketTimeoutException) "Request timeout." else e.message)
            }
            return
        } finally {
            parallelLimiter.release()
        }

        val body = try {
            mapper.readValue(rawBody, ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: $code, reason: $rawBody")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()