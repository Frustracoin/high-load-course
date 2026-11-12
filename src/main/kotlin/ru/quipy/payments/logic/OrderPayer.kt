package ru.quipy.payments.logic

import io.micrometer.core.instrument.Metrics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    // Явная очередь оплат, которые ждут свободного потока. Сколько в неё можно положить, решаем по дедлайну (см. processPayment),
    // а ёмкость - лишь страховка по памяти
    private val paymentQueue = ArrayBlockingQueue<Runnable>(8_000).also { queue ->
        Metrics.gauge("shop_payment_queue_size", queue) { it.size.toDouble() }
    }

    // Потоков ровно столько, сколько оплат аккаунты могут вести одновременно: лишние потоки ждали бы внутри
    // rate limiter / семафора, то есть образовали бы ещё одну, неявную очередь. При переполнении - отказ, а не блокировка вызывающего
    private val paymentExecutor by lazy {
        val threads = paymentService.parallelism()
        logger.info("Payment executor threads: $threads")
        ThreadPoolExecutor(
            threads,
            threads,
            0L,
            TimeUnit.MILLISECONDS,
            paymentQueue,
            NamedThreadFactory("payment-submission-executor"),
            ThreadPoolExecutor.AbortPolicy()
        )
    }

    private val acceptedPayments = Metrics.counter("shop_payment_requests", "result", "accepted")
    private val rejectedPayments = Metrics.counter("shop_payment_requests", "result", "rejected")

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        // Не берём оплату, которую не успеем провести до дедлайна: просим клиента прийти позже, вместо того чтобы платить провайдеру за опоздавший запрос
        if (!paymentService.canProcessInTime(paymentQueue.size, deadline)) {
            reject()
        }

        val createdAt = System.currentTimeMillis()
        try {
            paymentExecutor.submit {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
        } catch (e: RejectedExecutionException) {
            reject()
        }
        acceptedPayments.increment()
        return createdAt
    }

    private fun reject(): Nothing {
        rejectedPayments.increment()
        throw PaymentRejectedException(paymentService.retryAfter())
    }
}

class PaymentRejectedException(val retryAfter: Long) : RuntimeException("Payment can't be processed before the deadline")