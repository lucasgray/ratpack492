import static ratpack.groovy.Groovy.ratpack
import ratpack.stream.Streams
import ratpack.websocket.WebSockets

import com.ni.api.streamsforall.api.impl.StreamModule
import com.ni.api.streamsforall.api.impl.StreamService

ratpack {

	bindings { add new StreamModule() }

	handlers { StreamService streamService ->

		post("registerStream") {

			def sleeptime = context.getRequest().getBody().getText()

			def stream = streamService.makeStream(
				UUID.randomUUID().toString(),
				false, 
				Long.parseLong(sleeptime))

			context.getResponse().getHeaders().add("Access-Control-Allow-Origin", "*")

			context.getResponse().sendStream(Streams.publish(stream))

		}

//		get("registerWebsocketStream") {
//
//			def filter = context.getRequest().getBody().getText()
//
//			def stream = streamService.makeStream(UUID.randomUUID().toString(), filter, true)
//
//			context.getResponse().getHeaders().add("Access-Control-Allow-Origin", "*")
//
//			WebSockets websockets = new WebSockets(){}
//			websockets.websocketBroadcast(context, Streams.publish(stream))
//		}
	}
}
