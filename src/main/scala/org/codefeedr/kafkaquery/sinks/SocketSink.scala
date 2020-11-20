package org.codefeedr.kafkaquery.sinks

import java.io.IOException
import java.net.{ServerSocket, Socket}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}
import org.apache.flink.util.SerializableObject
import org.codefeedr.kafkaquery.sinks.SocketSink.{lock, serverSocket, socket}

class SocketSink[T](port: Int) extends RichSinkFunction[T] {

  override def open(parameters: Configuration): Unit = {
    lock synchronized {
      if (serverSocket == null) {
        serverSocket = new ServerSocket(port)
        println(
          s"Writing query output to available sockets on port ${serverSocket.getLocalPort}..."
        )
      }

      if (socket == null) {
        socket = serverSocket.accept()
        println(
          s"""Client ${socket.getInetAddress}:${socket.getPort} connected."""
        )
      }
    }
  }

  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    val valueBytes = (value.toString + "\n").getBytes

    lock synchronized {
      try {
        socket.getOutputStream.write(valueBytes)
      } catch {
        case _: IOException =>
          socket.close()
          println(
            s"""Client ${socket.getInetAddress}:${socket.getPort} disconnected."""
          )
      }
    }
  }

}

object SocketSink {
  private var serverSocket: ServerSocket = _
  private var socket: Socket = _
  private val lock = new SerializableObject

  def getSocket: Socket = socket
  def setSocket(newSocket: Socket): Unit = socket = newSocket
}
