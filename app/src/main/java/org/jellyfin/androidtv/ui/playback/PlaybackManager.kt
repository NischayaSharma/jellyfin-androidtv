package org.jellyfin.androidtv.ui.playback

import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import okhttp3.Call
import okhttp3.Callback
import okhttp3.OkHttpClient
import okhttp3.Request
import org.jellyfin.androidtv.data.compat.PlaybackException
import org.jellyfin.androidtv.data.compat.StreamInfo
import org.jellyfin.androidtv.data.compat.VideoOptions
import org.jellyfin.androidtv.util.apiclient.Response
import org.jellyfin.sdk.api.client.ApiClient
import org.jellyfin.sdk.api.client.extensions.hlsSegmentApi
import org.jellyfin.sdk.api.client.extensions.mediaInfoApi
import org.jellyfin.sdk.api.client.extensions.videosApi
import org.jellyfin.sdk.api.okhttp.OkHttpFactory
import org.jellyfin.sdk.model.api.PlayMethod
import org.jellyfin.sdk.model.api.PlaybackInfoDto
import org.jellyfin.sdk.model.api.PlaybackInfoResponse
import timber.log.Timber
import java.io.IOException
import java.net.URI
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

private fun createStreamInfo(
	api: ApiClient,
	options: VideoOptions,
	response: PlaybackInfoResponse,
): StreamInfo = StreamInfo().apply {
	val source = response.mediaSources.firstOrNull {
		options.mediaSourceId != null && it.id == options.mediaSourceId
	} ?: response.mediaSources.firstOrNull()

	itemId = options.itemId
	mediaSource = source
	runTimeTicks = source?.runTimeTicks
	playSessionId = response.playSessionId

	if (source == null) return@apply

	if (options.enableDirectPlay && source.supportsDirectPlay) {
		playMethod = PlayMethod.DIRECT_PLAY
		container = source.container
		mediaUrl = when {
			source.isRemote && source.path != null -> source.path
			else -> api.videosApi.getVideoStreamUrl(
				itemId = itemId,
				container = container,
				mediaSourceId = source.id,
				static = true,
				tag = source.eTag,
				liveStreamId = source.liveStreamId,
			)
		}
	} else if (options.enableDirectStream && source.supportsDirectStream) {
		playMethod = PlayMethod.DIRECT_STREAM
		container = source.transcodingContainer
		mediaUrl = api.createUrl(requireNotNull(source.transcodingUrl), ignorePathParameters = true)
	} else if (source.supportsTranscoding) {
		playMethod = PlayMethod.TRANSCODE
		container = source.transcodingContainer
		mediaUrl = api.createUrl(requireNotNull(source.transcodingUrl), ignorePathParameters = true)
	}
}

class PlaybackManager(
	private val api: ApiClient
) {
	private data class PrewarmKey(
		val itemId: UUID?,
		val mediaSourceId: String?,
		val audioStreamIndex: Int?,
		val subtitleStreamIndex: Int?,
		val enableDirectPlay: Boolean,
		val enableDirectStream: Boolean,
		val alwaysBurnInSubtitleWhenTranscoding: Boolean,
		val startTimeTicks: Long,
	)

	private data class PrewarmEntry(
		val key: PrewarmKey,
		val createdAtMillis: Long,
		val job: kotlinx.coroutines.Deferred<Result<Unit>>,
	)

	private val prewarmScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
	private val prewarmLock = Any()
	private var prewarmEntry: PrewarmEntry? = null
	private val prewarmClient: OkHttpClient by lazy {
		OkHttpFactory().createClient(
			api.httpClientOptions.copy(requestTimeout = Duration.ZERO)
		)
	}

	fun getVideoStreamInfo(
		lifecycleOwner: LifecycleOwner,
		options: VideoOptions,
		startTimeTicks: Long,
		callback: Response<StreamInfo>,
	) = lifecycleOwner.lifecycleScope.launch {
		awaitMatchingPrewarm(options, startTimeTicks)

		getVideoStreamInfoInternal(options, startTimeTicks).fold(
			onSuccess = { callback.onResponse(it) },
			onFailure = { callback.onError(Exception(it)) },
		)
	}

	/**
	 * Starts the expensive server work for the next legacy-player item without changing the
	 * currently playing ExoPlayer item. The warmup is keyed by stream choices so an audio or
	 * subtitle preference change cannot accidentally reuse work prepared for another stream.
	 */
	fun prewarmVideoStreamInfo(options: VideoOptions, startTimeTicks: Long = 0L) {
		val optionsSnapshot = options.copyForPrewarm()
		val key = optionsSnapshot.prewarmKey(startTimeTicks)
		val now = System.currentTimeMillis()
		var staleEntry: PrewarmEntry? = null

		synchronized(prewarmLock) {
			val current = prewarmEntry
			if (current != null && current.key == key && now - current.createdAtMillis < PREWARM_RETENTION.inWholeMilliseconds) {
				return
			}

			staleEntry = current
			val job = prewarmScope.async {
				prewarmVideoStream(optionsSnapshot, startTimeTicks)
			}
			prewarmEntry = PrewarmEntry(key, now, job)

			job.invokeOnCompletion { error ->
				when {
					error is CancellationException -> Timber.d("Next item prewarm cancelled for %s", key.itemId)
					error != null -> Timber.w(error, "Next item prewarm failed for %s", key.itemId)
				}
			}
		}

		staleEntry?.job?.cancel()
	}

	fun cancelVideoStreamPrewarm() {
		val entry = synchronized(prewarmLock) {
			prewarmEntry.also { prewarmEntry = null }
		}
		entry?.job?.cancel()
	}

	fun changeVideoStream(
		lifecycleOwner: LifecycleOwner,
		stream: StreamInfo,
		options: VideoOptions,
		startTimeTicks: Long,
		callback: Response<StreamInfo>
	) = lifecycleOwner.lifecycleScope.launch {
		if (stream.playSessionId != null && stream.playMethod != PlayMethod.DIRECT_PLAY) {
			withContext(Dispatchers.IO) {
				api.hlsSegmentApi.stopEncodingProcess(api.deviceInfo.id, stream.playSessionId)
			}
		}

		getVideoStreamInfoInternal(options, startTimeTicks).fold(
			onSuccess = { callback.onResponse(it) },
			onFailure = { callback.onError(Exception(it)) },
		)
	}

	private suspend fun getVideoStreamInfoInternal(
		options: VideoOptions,
		startTimeTicks: Long
	) = runCatching {
		val response = withContext(Dispatchers.IO) {
			api.mediaInfoApi.getPostedPlaybackInfo(
				itemId = requireNotNull(options.itemId) { "Item id cannot be null" },
				data = PlaybackInfoDto(
					mediaSourceId = options.mediaSourceId,
					startTimeTicks = startTimeTicks,
					deviceProfile = options.profile,
					enableDirectStream = options.enableDirectStream,
					enableDirectPlay = options.enableDirectPlay,
					maxAudioChannels = options.maxAudioChannels,
					audioStreamIndex = options.audioStreamIndex.takeIf { it != null && it >= 0 },
					subtitleStreamIndex = options.subtitleStreamIndex,
					allowVideoStreamCopy = true,
					allowAudioStreamCopy = true,
					autoOpenLiveStream = true,
					alwaysBurnInSubtitleWhenTranscoding = options.alwaysBurnInSubtitleWhenTranscoding,
				)
			).content
		}

		if (response.errorCode != null) {
			throw PlaybackException().apply {
				errorCode = response.errorCode!!
			}
		}

		createStreamInfo(api, options, response)
	}

	private suspend fun awaitMatchingPrewarm(options: VideoOptions, startTimeTicks: Long) {
		val key = options.prewarmKey(startTimeTicks)
		val entry = synchronized(prewarmLock) {
			val current = prewarmEntry
			when {
				current == null -> null
				System.currentTimeMillis() - current.createdAtMillis >= PREWARM_RETENTION.inWholeMilliseconds -> {
					prewarmEntry = null
					current.job.cancel()
					null
				}
				current.key == key -> current
				current.key.itemId == key.itemId -> {
					// The target is now playing with different stream preferences. Do not leave a
					// second transcode running for the stale selection.
					prewarmEntry = null
					current.job.cancel()
					null
				}
				else -> null
			}
		}

		if (entry == null) return

		Timber.i("Waiting for next item prewarm to finish for %s", key.itemId)
		val result = try {
			entry.job.await()
		} catch (error: CancellationException) {
			if (!currentCoroutineContext().isActive) throw error
			Result.failure(error)
		}
		result.onFailure { error ->
			if (error !is CancellationException) {
				Timber.w(error, "Next item prewarm did not complete for %s", key.itemId)
			}
		}
		synchronized(prewarmLock) {
			if (prewarmEntry === entry) prewarmEntry = null
		}
	}

	private suspend fun prewarmVideoStream(options: VideoOptions, startTimeTicks: Long): Result<Unit> {
		var stream: StreamInfo? = null
		return try {
			stream = getVideoStreamInfoInternal(options, startTimeTicks).getOrThrow()
			if (stream.playMethod != PlayMethod.DIRECT_PLAY && stream.mediaUrl?.contains(".m3u8", ignoreCase = true) == true) {
				Timber.i("Prewarming next item HLS stream for %s", options.itemId)
				prewarmHls(stream.mediaUrl!!)
			}
			Timber.i("Next item prewarm complete for %s", options.itemId)
			Result.success(Unit)
		} catch (error: CancellationException) {
			throw error
		} catch (error: Throwable) {
			Timber.w(error, "Unable to prewarm next item %s", options.itemId)
			Result.failure(error)
		} finally {
			val warmedStream = stream
			if (warmedStream?.playSessionId != null && warmedStream.playMethod != PlayMethod.DIRECT_PLAY) {
				withContext(NonCancellable + Dispatchers.IO) {
					runCatching {
						api.hlsSegmentApi.stopEncodingProcess(api.deviceInfo.id, warmedStream.playSessionId)
					}.onFailure { Timber.d(it, "Unable to stop completed next item prewarm") }
				}
			}
		}
	}

	private suspend fun prewarmHls(initialUrl: String) {
		var playlistUrl = initialUrl
		val visited = mutableSetOf<String>()

		repeat(MAX_PLAYLIST_DEPTH) {
			if (!visited.add(playlistUrl)) return
			val playlist = fetchText(playlistUrl)
			val resources = parseHlsPrewarmResources(playlist)

			val childPlaylist = resources.childPlaylist
			if (childPlaylist != null) {
				playlistUrl = resolveUrl(playlistUrl, childPlaylist)
				return@repeat
			}

			resources.initializationSegment?.let { fetchBytes(resolveUrl(playlistUrl, it)) }
			resources.firstMediaSegment?.let { fetchBytes(resolveUrl(playlistUrl, it)) }
			return
		}
	}

	private suspend fun fetchText(url: String): String {
		val request = Request.Builder().url(url).get().build()
		return prewarmClient.newCall(request).awaitResponse().use { response ->
			check(response.isSuccessful) { "HLS prewarm request failed with HTTP ${response.code}" }
			requireNotNull(response.body) { "HLS prewarm response had no body" }.string()
		}
	}

	private suspend fun fetchBytes(url: String) {
		val request = Request.Builder().url(url).get().build()
		prewarmClient.newCall(request).awaitResponse().use { response ->
			check(response.isSuccessful) { "HLS prewarm request failed with HTTP ${response.code}" }
			requireNotNull(response.body) { "HLS prewarm response had no body" }.byteStream().use { input ->
				val buffer = ByteArray(64 * 1024)
				var remaining = MAX_PREWARM_RESOURCE_BYTES
				while (remaining > 0) {
					val read = input.read(buffer, 0, minOf(buffer.size, remaining))
					if (read == -1) break
					remaining -= read
				}
			}
		}
	}

	private suspend fun Call.awaitResponse(): okhttp3.Response = suspendCancellableCoroutine { continuation ->
		continuation.invokeOnCancellation { cancel() }
		enqueue(object : Callback {
			override fun onFailure(call: Call, error: IOException) {
				if (continuation.isActive) continuation.resumeWith(Result.failure(error))
			}

			override fun onResponse(call: Call, response: okhttp3.Response) {
				if (continuation.isActive) {
					continuation.resumeWith(Result.success(response))
				} else {
					response.close()
				}
			}
		})
	}

	private fun resolveUrl(baseUrl: String, reference: String): String =
		URI(baseUrl).resolve(reference).toString()

	private fun VideoOptions.prewarmKey(startTimeTicks: Long) = PrewarmKey(
		itemId = itemId,
		mediaSourceId = mediaSourceId,
		audioStreamIndex = audioStreamIndex,
		subtitleStreamIndex = subtitleStreamIndex,
		enableDirectPlay = enableDirectPlay,
		enableDirectStream = enableDirectStream,
		alwaysBurnInSubtitleWhenTranscoding = alwaysBurnInSubtitleWhenTranscoding,
		startTimeTicks = startTimeTicks,
	)

	private fun VideoOptions.copyForPrewarm() = VideoOptions().also { copy ->
		copy.enableDirectPlay = enableDirectPlay
		copy.enableDirectStream = enableDirectStream
		copy.itemId = itemId
		copy.mediaSources = mediaSources
		copy.profile = profile
		copy.mediaSourceId = mediaSourceId
		copy.maxAudioChannels = maxAudioChannels
		copy.audioStreamIndex = audioStreamIndex
		copy.subtitleStreamIndex = subtitleStreamIndex
		copy.alwaysBurnInSubtitleWhenTranscoding = alwaysBurnInSubtitleWhenTranscoding
	}

	private companion object {
		val PREWARM_RETENTION = 15.minutes
		const val MAX_PLAYLIST_DEPTH = 3
		const val MAX_PREWARM_RESOURCE_BYTES = 16 * 1024 * 1024
	}
}

internal data class HlsPrewarmResources(
	val childPlaylist: String? = null,
	val initializationSegment: String? = null,
	val firstMediaSegment: String? = null,
)

internal fun parseHlsPrewarmResources(playlist: String): HlsPrewarmResources {
	val lines = playlist.lineSequence().map(String::trim).filter(String::isNotEmpty).toList()
	val mediaReferences = lines.filterNot { it.startsWith('#') }
	val childPlaylist = mediaReferences.firstOrNull { reference ->
		runCatching { URI(reference).path }.getOrNull()?.endsWith(".m3u8", ignoreCase = true) == true
	}
	if (childPlaylist != null) return HlsPrewarmResources(childPlaylist = childPlaylist)

	val initializationSegment = lines.firstNotNullOfOrNull { line ->
		if (!line.startsWith("#EXT-X-MAP:", ignoreCase = true)) return@firstNotNullOfOrNull null
		HLS_MAP_URI.find(line)?.groupValues?.getOrNull(1)
	}

	return HlsPrewarmResources(
		initializationSegment = initializationSegment,
		firstMediaSegment = mediaReferences.firstOrNull(),
	)
}

private val HLS_MAP_URI = Regex("""URI="([^"]+)""", RegexOption.IGNORE_CASE)
