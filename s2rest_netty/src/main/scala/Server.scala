package com.kakao.s2graph.rest.netty

import java.util.concurrent.Executors

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.rest.RestCaller
import com.kakao.s2graph.core.utils.logger
import com.typesafe.config.ConfigFactory
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http._
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import io.netty.util.CharsetUtil
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Success, Try}

class S2RestHandler(s2rest: RestCaller)(implicit ec: ExecutionContext) extends SimpleChannelInboundHandler[FullHttpRequest] with JSONParser {
  val ApplicationJson = "application/json"

  val Ok = HttpResponseStatus.OK
  val Close = ChannelFutureListener.CLOSE
  val BadRequest = HttpResponseStatus.BAD_REQUEST
  val BadGateway = HttpResponseStatus.BAD_GATEWAY
  val NotFound = HttpResponseStatus.NOT_FOUND
  val InternalServerError = HttpResponseStatus.INTERNAL_SERVER_ERROR

  def badRoute(ctx: ChannelHandlerContext) =
    simpleResponse(ctx, BadGateway, byteBufOpt = None, channelFutureListenerOpt = Option(Close))

  def simpleResponse(ctx: ChannelHandlerContext,
                     httpResponseStatus: HttpResponseStatus,
                     byteBufOpt: Option[ByteBuf] = None,
                     headers: Seq[(String, String)] = Nil,
                     channelFutureListenerOpt: Option[ChannelFutureListener] = None): Unit = {
    val res: FullHttpResponse = byteBufOpt match {
      case None => new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, httpResponseStatus)
      case Some(byteBuf) =>
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, httpResponseStatus, byteBuf)
    }

    headers.foreach { case (k, v) => res.headers().set(k, v) }
    val channelFuture = ctx.writeAndFlush(res)

    channelFutureListenerOpt match {
      case None =>
      case Some(listener) => channelFuture.addListener(listener)
    }
  }


  def toResponse(ctx: ChannelHandlerContext, req: FullHttpRequest, jsonQuery: JsValue, future: Future[JsValue], startedAt: Long) = {
    future onComplete {
      case Success(resJson) =>
        import HttpHeaders._
        val duration = System.currentTimeMillis() - startedAt
        val isKeepAlive = HttpHeaders.isKeepAlive(req)
        val buf: ByteBuf = Unpooled.copiedBuffer(resJson.toString, CharsetUtil.UTF_8)
        val (headers, listenerOpt) =
          if (isKeepAlive) (Seq(Names.CONTENT_TYPE -> ApplicationJson, Names.CONTENT_LENGTH -> buf.readableBytes().toString, Names.CONNECTION -> HttpHeaders.Values.KEEP_ALIVE), None)
          else (Seq(Names.CONTENT_TYPE -> ApplicationJson, Names.CONTENT_LENGTH -> buf.readableBytes().toString), Option(Close))
        //NOTE: logging size of result should move to s2core.
        //        logger.info(resJson.size.toString)

        val log = s"${req.getMethod} ${req.getUri} took ${duration} ms 200 ${s2rest.calcSize(resJson)} ${jsonQuery}"
        logger.info(log)

        simpleResponse(ctx, Ok, byteBufOpt = Option(buf), channelFutureListenerOpt = listenerOpt, headers = headers)
      case Failure(ex) => simpleResponse(ctx, InternalServerError, byteBufOpt = None, channelFutureListenerOpt = Option(Close))
    }
  }


  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    val uri = req.getUri

    req.getMethod match {
      case HttpMethod.GET =>
        uri match {
          case "/health_check.html" =>
            if (NettyServer.isHealthy) {
              val healthCheckMsg = Unpooled.copiedBuffer(NettyServer.deployInfo, CharsetUtil.UTF_8)
              simpleResponse(ctx, Ok, byteBufOpt = Option(healthCheckMsg), channelFutureListenerOpt = Option(Close))
            } else {
              simpleResponse(ctx, NotFound, channelFutureListenerOpt = Option(Close))
            }

          case s if s.startsWith("/graphs/getEdge/") =>
            // src, tgt, label, dir
            val Array(srcId, tgtId, labelName, direction) = s.split("/").takeRight(4)
            val params = Json.arr(Json.obj("label" -> labelName, "direction" -> direction, "from" -> srcId, "to" -> tgtId))
            val startedAt = System.currentTimeMillis()
            val future = s2rest.checkEdgesInner(params)
            toResponse(ctx, req, params, future, startedAt)
          case _ => badRoute(ctx)
        }

      case HttpMethod.PUT =>
        if (uri.startsWith("/health_check/")) {
          val newHealthCheck = uri.split("/").last.toBoolean
          NettyServer.isHealthy = newHealthCheck
          val newHealthCheckMsg = Unpooled.copiedBuffer(NettyServer.isHealthy.toString, CharsetUtil.UTF_8)
          simpleResponse(ctx, Ok, byteBufOpt = Option(newHealthCheckMsg), channelFutureListenerOpt = Option(Close))
        } else badRoute(ctx)

      case HttpMethod.POST =>
        val jsonString = req.content.toString(CharsetUtil.UTF_8)
        val jsQuery = Json.parse(jsonString)

        //TODO: result_size
        val startedAt = System.currentTimeMillis()

        val future =
          if (uri.startsWith("/graphs/experiment")) {
            val Array(accessToken, experimentName, uuid) = uri.split("/").takeRight(3)
            s2rest.experiment(jsQuery, accessToken, experimentName, uuid)
          } else {
            s2rest.uriMatch(uri, jsQuery)
          }

        toResponse(ctx, req, jsQuery, future, startedAt)

      case _ =>
        simpleResponse(ctx, BadRequest, byteBufOpt = None, channelFutureListenerOpt = Option(Close))
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    logger.error(s"exception on query.", cause)
    simpleResponse(ctx, BadRequest, byteBufOpt = None, channelFutureListenerOpt = Option(Close))
  }
}

// Simple http server
object NettyServer extends App {

  def updateHealthCheck(healthCheck: Boolean): Boolean = {
    this.isHealthy = healthCheck
    this.isHealthy
  }

  def getBoolean(key: String, defaultValue: Boolean): Boolean =
    if (config.hasPath(key)) config.getBoolean(key) else defaultValue

  /** should be same with Boostrap.onStart on play */

  // app status code
  var isHealthy = false
  var deployInfo = ""

  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  val ec = ExecutionContext.fromExecutor(threadPool)

  val config = ConfigFactory.load()
  val port = Try(config.getInt("http.port")).recover { case _ => 9000 }.get

  // init s2graph with config
  val s2graph = new Graph(config)(ec)
  val rest = new RestCaller(s2graph)(ec)

  val defaultHealthOn = getBoolean("app.health.on", true)
  deployInfo = Try(Source.fromFile("./release_info").mkString("")).recover { case _ => "release info not found\n" }.get

  isHealthy = defaultHealthOn
  logger.info(s"starts with num of thread: $numOfThread, ${threadPool.getClass.getSimpleName}")

  // Configure the server.
  val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)
  val workerGroup: EventLoopGroup = new NioEventLoopGroup()

  try {
    val b: ServerBootstrap = new ServerBootstrap()
    b.option(ChannelOption.SO_BACKLOG, Int.box(2048))

    b.group(bossGroup, workerGroup).channel(classOf[NioServerSocketChannel])
      .handler(new LoggingHandler(LogLevel.INFO))
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel) {
          val p = ch.pipeline()
          p.addLast(new HttpServerCodec())
          p.addLast(new HttpObjectAggregator(65536))
          p.addLast(new S2RestHandler(rest)(ec))
        }
      })

    logger.info(s"Listening for HTTP on /0.0.0.0:$port")
    val ch: Channel = b.bind(port).sync().channel()
    ch.closeFuture().sync()

  } finally {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
    s2graph.shutdown()
  }
}

