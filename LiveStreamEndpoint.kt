package org.opensky.atco.rest.endpoints

import io.quarkus.security.identity.SecurityIdentity
import io.smallrye.common.annotation.Blocking
import io.smallrye.mutiny.Multi
import io.smallrye.reactive.messaging.kafka.Record
import org.eclipse.microprofile.openapi.annotations.Operation
import org.eclipse.microprofile.openapi.annotations.tags.Tag
import org.eclipse.microprofile.reactive.messaging.Channel
import org.jboss.logging.Logger
import org.opensky.atco.db.repository.DeviceConfigRepository
import org.opensky.atco.model.Receiver
import org.opensky.atco.model.TranscriptStreamResponse
import org.opensky.atco.queue.TranscriptQueueItem
import java.security.Principal
import java.time.Duration
import javax.annotation.security.RolesAllowed
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType

@Path("/live")
class LiveStreamEndpoint(
    @Channel("transcripts-in") val transcripts: Multi<Record<String, TranscriptQueueItem>>,
    private val deviceConfigRepository: DeviceConfigRepository,
    private val securityIdentity: SecurityIdentity,
    private val logger: Logger
) {

    @GET
    @Path("/transcripts/{deviceid}")
    @RolesAllowed(value = ["/opensky/atc/admin", "/opensky/atc/feeder", "/opensky/atc/data"])
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @Operation(
        summary = "Get live stream of transcripts for airport of specific device",
        description = "Get live stream of transcripts. Requires the Feeder role for the user."
    )
    @Tag(name = "data")
    @Suppress("MagicNumber")
    @Blocking
    fun getTranscriptStreamByDevice(
        @PathParam("deviceid") deviceId: String
    ): Multi<TranscriptStreamResponse> {
        logger.info("Called transcript stream by id $deviceId service")
        val user: Principal? = securityIdentity.principal
        val isAdmin = securityIdentity.roles.contains("/opensky/atc/admin")
        val receiver = deviceConfigRepository.findConfigByDeviceId(deviceId)?.let { Receiver.of(it) }
        return if (receiver?.userFilterFunction(user = user?.name ?: "", userParam = null, isAdmin = isAdmin) == true) {
            Multi.createBy().merging()
                .streams(
                    transcripts
                        .map {
                        TranscriptStreamResponse(
                            airport = it.value().airport,
                            device = it.value().device,
                            frequency = (it.value().frequency / 1_000_000.0).toString() + "MHz",
                            transcriptText = it.value().transcriptText,
                            lengthSeconds = it.value().recordingLengthSeconds,
                            callSign = it.value().detectedCallsign,
                            fileName = it.key().split(' ')[1],
                            recordingDate = it.value().timestamp
                        )}
                        .filter { it.device.equals(deviceId ignoreCase = true) },
                    emitAPeriodicPingForAirport(airport = receiver.airport ?: "none")
                )
        } else {
            Multi.createFrom().empty()
        }
    }

    private fun emitAPeriodicPingForAirport(airport: String): Multi<TranscriptStreamResponse> {
        return Multi.createFrom().ticks()
            .every(Duration.ofSeconds(KEEP_ALIVE_SECONDS))
            .onItem().transform { TranscriptStreamResponse.empty(airport = airport) }
    }

    companion object {
        const val KEEP_ALIVE_SECONDS = 10L
    }
}
