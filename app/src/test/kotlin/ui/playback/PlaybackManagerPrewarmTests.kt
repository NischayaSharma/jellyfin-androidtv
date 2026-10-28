package org.jellyfin.androidtv.ui.playback

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class PlaybackManagerPrewarmTests : FunSpec({
	test("prewarm parser follows the first variant in a master playlist") {
		val playlist = """
			#EXTM3U
			#EXT-X-STREAM-INF:BANDWIDTH=8000000
			/videos/episode/main.m3u8?api_key=test
		""".trimIndent()

		parseHlsPrewarmResources(playlist) shouldBe HlsPrewarmResources(
			childPlaylist = "/videos/episode/main.m3u8?api_key=test"
		)
	}

	test("prewarm parser finds the initialization and first media segment") {
		val playlist = """
			#EXTM3U
			#EXT-X-MAP:URI="episode-init.mp4"
			#EXTINF:3.0,
			episode0.mp4
			#EXTINF:3.0,
			episode1.mp4
		""".trimIndent()

		parseHlsPrewarmResources(playlist) shouldBe HlsPrewarmResources(
			initializationSegment = "episode-init.mp4",
			firstMediaSegment = "episode0.mp4",
		)
	}

	test("prewarm parser handles a media playlist without an initialization segment") {
		val playlist = """
			#EXTM3U
			#EXTINF:6.0,
			episode0.ts?api_key=test
		""".trimIndent()

		parseHlsPrewarmResources(playlist) shouldBe HlsPrewarmResources(
			firstMediaSegment = "episode0.ts?api_key=test"
		)
	}
})
