package ru.quipy.payments.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.convert.DurationStyle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.*


@Configuration
class PaymentAccountsConfig {
    companion object {
        private val javaClient = HttpClient.newBuilder().build()
        private val mapper = ObjectMapper().registerKotlinModule().registerModules(JavaTimeModule())
    }

    @Value("\${payment.hostPort}")
    lateinit var paymentProviderHostPort: String

    @Value("\${payment.service-name}")
    lateinit var serviceName: String

    @Value("\${payment.token}")
    lateinit var token: String

    @Value("#{'\${payment.accounts}'.split(',')}")
    lateinit var allowedAccounts: List<String>

    // Для свойств аккаунтов, которых нет у провайдера: payment.attempt-timeout.<account>=1300ms
    @Autowired
    lateinit var env: Environment

    @Bean
    fun accountAdapters(paymentService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>): List<PaymentExternalSystemAdapter> {
        val request = HttpRequest.newBuilder()
            .uri(URI("http://${paymentProviderHostPort}/external/accounts?serviceName=$serviceName&token=$token"))
            .GET()
            .build()

        val resp = javaClient.send(request, HttpResponse.BodyHandlers.ofString())

        println("\nPayment accounts list:")
        return mapper.readValue<List<PaymentAccountProperties>>(
            resp.body(),
            mapper.typeFactory.constructCollectionType(List::class.java, PaymentAccountProperties::class.java)
        )
            .filter { it.accountName in allowedAccounts }
            .map { acc ->
                val attemptTimeout = env.getProperty("payment.attempt-timeout.${acc.accountName}")?.let { DurationStyle.detectAndParse(it) }
                acc.copy(enabled = true, attemptTimeout = attemptTimeout)
            }
            .onEach(::println)
            .map {
                PaymentExternalSystemAdapterImpl(
                    it,
                    paymentService,
                    paymentProviderHostPort,
                    token
                )
            }
    }
}