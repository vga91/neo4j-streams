package streams.kafka.connect.sink

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.plus
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.Transaction
import org.neo4j.driver.Values.parameters
import org.neo4j.driver.async.AsyncSession
import org.neo4j.driver.async.AsyncTransaction
import org.neo4j.driver.async.ResultCursor
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.exceptions.TransientException
import org.neo4j.driver.net.ServerAddress
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.extensions.awaitAll
import streams.extensions.errors
import streams.kafka.connect.utils.PropertiesUtil
import streams.service.StreamsSinkEntity
import streams.service.StreamsSinkService
import streams.utils.retryForException
import java.time.Instant
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit
import kotlin.streams.toList


class Neo4jService(private val config: Neo4jSinkConnectorConfig):
        StreamsSinkService(Neo4jStrategyStorage(config)) {

    private val log: Logger = LoggerFactory.getLogger(Neo4jService::class.java)

    private val driver: Driver
    private val sessionConfig: SessionConfig
    private val session: Session

    init {
        val configBuilder = Config.builder()
        configBuilder.withUserAgent("neo4j-kafka-connect-sink/${PropertiesUtil.getVersion()}")

        if (!this.config.hasSecuredURI()) {
            if (this.config.encryptionEnabled) {
                configBuilder.withEncryption()
                val trustStrategy: Config.TrustStrategy = when (this.config.encryptionTrustStrategy) {
                    Config.TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES -> Config.TrustStrategy.trustAllCertificates()
                    Config.TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES -> Config.TrustStrategy.trustSystemCertificates()
                    Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES -> Config.TrustStrategy.trustCustomCertificateSignedBy(this.config.encryptionCACertificateFile)
                    else -> {
                        throw ConfigException(Neo4jSinkConnectorConfig.ENCRYPTION_TRUST_STRATEGY, this.config.encryptionTrustStrategy.toString(), "Encryption Trust Strategy is not supported.")
                    }
                }
                configBuilder.withTrustStrategy(trustStrategy)
            } else {
                configBuilder.withoutEncryption()
            }
        }

        val authToken = when (this.config.authenticationType) {
            AuthenticationType.NONE -> AuthTokens.none()
            AuthenticationType.BASIC -> {
                if (this.config.authenticationRealm != "") {
                    AuthTokens.basic(this.config.authenticationUsername, this.config.authenticationPassword, this.config.authenticationRealm)
                } else {
                    AuthTokens.basic(this.config.authenticationUsername, this.config.authenticationPassword)
                }
            }
            AuthenticationType.KERBEROS -> AuthTokens.kerberos(this.config.authenticationKerberosTicket)
        }
        configBuilder.withMaxConnectionPoolSize(this.config.connectionPoolMaxSize)
        configBuilder.withMaxConnectionLifetime(this.config.connectionMaxConnectionLifetime, TimeUnit.MILLISECONDS)
        configBuilder.withConnectionAcquisitionTimeout(this.config.connectionAcquisitionTimeout, TimeUnit.MILLISECONDS)
        configBuilder.withMaxTransactionRetryTime(config.retryBackoff, TimeUnit.MILLISECONDS)
        configBuilder.withResolver { address -> this.config.serverUri.map { ServerAddress.of(it.host, it.port) }.toSet() }
        val neo4jConfig = configBuilder.build()

        this.driver = GraphDatabase.driver(this.config.serverUri.firstOrNull(), authToken, neo4jConfig)
        val sessionConfigBuilder = SessionConfig.builder()
        if (config.database.isNotBlank()) {
            sessionConfigBuilder.withDatabase(config.database)
        }
        this.sessionConfig = sessionConfigBuilder.build()
        this.session = driver.session(sessionConfig)
    }

    fun close() {
        session.close()
        driver.close()
    }

    override fun write(query: String, events: Collection<Any>/*, session: Session?*/, scope: CoroutineScope?) /*= runBlocking*/ {
    
        
    
        val data = mapOf<String, Any>("events" to events)
            try {
                runBlocking { 
                    driver.session(sessionConfig).beginTransaction().use { tx -> 
                        val epochSecond = Instant.now().epochSecond
                        val asd = scope?.async {
                            println("prima" + epochSecond)
                            val run = tx.run(query, data).consume()
                            run
                        }
//                        try {
                            asd?.await()
                            println("dopo" + (Instant.now().epochSecond - epochSecond).toString())
                            tx.commit()
//                        } catch (e: Exception) {
//                            println("Cancellation Exception + $e")
//                        }
                    }
                    }

            } catch (e: Exception) {
            println("eccez.. di neo4j $e")
                if (log.isDebugEnabled) {
                    val subList = events.stream()
                            .limit(5.coerceAtMost(events.size).toLong())
                            .toList()
                    log.debug("Exception `${e.message}` while executing query: `$query`, with data: `$subList` total-records ${events.size}")
                }
//                throw e
            }
        
    }

    fun writeData(data: Map<String, List<List<StreamsSinkEntity>>>) {
        val errors = if (config.parallelBatches) writeDataAsync(data) else writeDataSync(data)
        println("Instant2" + Instant.now().toEpochMilli())
        println("Instant3 $errors")
        if (errors.isNotEmpty()) {
            throw ConnectException(errors.map { it.message }.toSet()
                    .joinToString("\n", "Errors executing ${data.values.map { it.size }.sum()} jobs:\n"))
        }
    }

//    @ExperimentalCoroutinesApi
//    @ObsoleteCoroutinesApi
//    private fun writeDataAsync(data: Map<String, List<List<StreamsSinkEntity>>>) = runBlocking {
//        val jobs = data
//                .flatMap { (topic, records) ->
//                    records.mapIndexed { index, it -> async(Dispatchers.IO) { println("index!! $index"); writeForTopic(topic, it) } }
//                }
//        println("sono async" + Instant.now().toEpochMilli())
//        jobs.awaitAll(config.batchTimeout)
//        jobs.mapNotNull { it.errors() }
//    }

    @ExperimentalCoroutinesApi
    @ObsoleteCoroutinesApi
    private fun writeDataAsync(data: Map<String, List<List<StreamsSinkEntity>>>) = runBlocking {
//        val scope = this
        val jobs = data
                .flatMap { (topic, records) ->
                    records.map { async(Dispatchers.IO) { writeForTopic(topic, it, this/*, driver*/) } }
                }

        try {
            jobs.awaitAll(config.batchTimeout)
            jobs.mapNotNull { it.errors() }
        } catch (e: Exception) {
            listOfNotNull(e)
        }
//    emptyList<Any>()
    }
    
    
//    private fun writeDataSync(data: Map<String, List<List<StreamsSinkEntity>>>) =
//            data.flatMap { (topic, records) ->
//                records.mapNotNull {
//                    try {
//                        writeForTopic(topic, it)
//                        null
//                    } catch (e: Exception) {
//                        e
//                    }
//                }
//            }

    

    @ExperimentalCoroutinesApi
    private fun writeDataSync(data: Map<String, List<List<StreamsSinkEntity>>>) = runBlocking {
    
//        driver.session().beginTransaction().use {  }

        val scope = this

        val handler = CoroutineExceptionHandler { _, e -> e}
            
            
                println("Instant" + Instant.now().toEpochMilli())
                val job: Deferred<List<String>> = async(handler) { 
                    data.flatMap { (topic, records) ->
                        records.mapNotNull {
                            try {
                                writeForTopic(topic, it, scope)
//                                println("index!! $index")
                                "null"
//                            } catch (e: TimeoutCancellationException) {
//                                println("timeout")
//                                throw e
                            } catch (e: Exception) {
                                println("altra ecc.")
//                                "e"
                                throw e
                            }
                        }
                    }
                }
//        var listOf: List<Exception> = mutableListOf()
//            try {
        val withTimeout: List<String> = withTimeout(config.batchTimeout) {
            val await = job.await()
            await
        }
//        withTimeout
        listOfNotNull(Exception(""))
//        val errors = job.errors()
//        listOfNotNull(errors)
                
//                withTimeout
//            } catch (e: TimeoutCancellationException) {
//                println("AAAAA")
////                scope@launch.cancel()
////                session.reset()
//                scope.coroutineContext.cancelChildren(e)
////                session.close()
//                // todo - questo non lo caga.....
//                listOf = listOf(e)
//                throw e
//            } finally {
//                listOf
//            }
        }
}