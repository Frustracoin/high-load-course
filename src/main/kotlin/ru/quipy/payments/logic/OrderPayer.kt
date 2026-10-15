package ru.quipy.payments.logic

import io.micrometer.core.instrument.Metrics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
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

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val acceptedPayments = Metrics.counter("shop_payment_requests", "result", "accepted")
    private val rejectedPayments = Metrics.counter("shop_payment_requests", "result", "rejected")

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        // Не берём оплату, которую не успеем провести до дедлайна: просим клиента прийти позже, вместо того чтобы платить провайдеру за опоздавший запрос
        if (!paymentService.tryAcceptPayment(deadline)) {
            rejectedPayments.increment()
            throw PaymentRejectedException(paymentService.retryAfter())
        }
        acceptedPayments.increment()

        val createdAt = System.currentTimeMillis()
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
        return createdAt
    }
}

class PaymentRejectedException(val retryAfter: Long) : RuntimeException("Payment can't be processed before the deadline")