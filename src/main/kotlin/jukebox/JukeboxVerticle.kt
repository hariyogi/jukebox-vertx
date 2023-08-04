package jukebox

import io.vertx.core.AbstractVerticle
import io.vertx.core.eventbus.Message
import io.vertx.core.file.AsyncFile
import io.vertx.core.file.OpenOptions
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import mu.KotlinLogging
import java.io.File

class JukeboxVerticle : AbstractVerticle() {

  val logger = KotlinLogging.logger {  }

  var currentMode = State.PAUSED
  var playList = ArrayDeque<String>()

  override fun start() {
    logger.info { "Jukebox Verticle deployed" }
    val eventBus = vertx.eventBus()
    eventBus.consumer<Void>("jukebox.play") { play() }
    eventBus.consumer<Void>("jukebox.pause") { pause() }
    eventBus.consumer("jukebox.schedule", ::schedule)
    eventBus.consumer<Void>("jukebox.list", ::list)

    vertx.createHttpServer()
      .requestHandler { httpHandler(it) }
      .listen(8080)

  }

  private fun httpHandler(request: HttpServerRequest) {
    if("/" == request.path()) {
      openAudioStream(request)
      return
    }

    if(request.path().startsWith("/download/")) {
      val sanitizedPath = request.path().substring(10).replace("/", "")
      logger.info { "Try to download $sanitizedPath" }
      download(sanitizedPath, request)
      return
    }

    request.response().setStatusCode(404).end()
  }

  private fun download(path: String, request: HttpServerRequest) {
    val file = "tracks/$path"

    if(!vertx.fileSystem().existsBlocking(file)) {
      request.response().setStatusCode(404).end()
      return
    }

    val opts = OpenOptions().setRead(true)
    vertx.fileSystem().open(file, opts) {
      if(it.succeeded()) {
        downloadFile(it.result(), request)
      }else {
        request.response().setStatusCode(500).end()
      }
    }
  }

  private fun downloadFile(file: AsyncFile, request: HttpServerRequest) {
    val response = request.response()
    response.setStatusCode(200)
      .putHeader("Content-Type", "audio/mpeg")
      .setChunked(true)

    file.pipeTo(response)

    file.endHandler { response.end() }
  }

  private val streamers = mutableSetOf<HttpServerResponse>()

  private fun openAudioStream(request: HttpServerRequest) {
    val response = request.response()
      .putHeader("Content-Type", "audio/mpeg")
      .setChunked(true)

    streamers.add(response)

    response.endHandler {
      streamers.remove(response)
    }
  }

  private fun play() {
    currentMode = State.PLAYING
  }

  private fun pause() {
    currentMode = State.PAUSED
  }

  private fun schedule(request: Message<JsonObject>) {
    val file = request.body().getString("file")
    if (playList.isEmpty() && currentMode == State.PAUSED) {
      currentMode = State.PLAYING
    }
    playList.addLast(file)
  }

  private fun list(request: Message<*>) {
    vertx.fileSystem().readDir("tracks", "*mp3$") {
      if (it.succeeded()) {
        val files = it.result()
          .stream()
          .map { path -> File(path) }
          .map { file -> file.name }
          .toList()
        val json = json {
          obj(
            "files" to JsonArray(files)
          )
        }

        request.reply(json)
      } else {
        request.fail(500, it.cause().message)
      }
    }
  }


}
