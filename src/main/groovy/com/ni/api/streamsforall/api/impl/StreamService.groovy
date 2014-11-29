package com.ni.api.streamsforall.api.impl

import groovy.stream.Stream
import groovy.transform.Canonical
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import io.netty.buffer.Unpooled

import java.util.Map.Entry
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

@ToString
@Canonical
@Slf4j
class StreamService {

	ConcurrentMap<String,BlockingQueue<JsonNode>> blockingQueues

	ConcurrentMap<String,ExecutorService> executors
	ConcurrentMap<String,Set<Thread>> threads

	Thread backgroundThread = new Thread() {
		void run() {

			while(true) {
				if (blockingQueues != null && blockingQueues.size() !=0) {
					blockingQueues.entrySet().each { Entry<String,BlockingQueue<JsonNode>> entry ->

						log.info("Consumer ${entry.getKey()} is at ${entry.getValue().size()}")

						if (entry.getValue().size() >= 1000) {
							log.info("Consumer ${entry.getKey()} is at capacity!")

							ExecutorService es = executors.get(entry.getKey())
							BlockingQueue<JsonNode> lbq = entry.getValue()

							threads.get(entry.getKey()).each {it.shutdown = true } 
							
							try {
								es.shutdownNow()
							} catch (Exception e) {
								log.info("couldnt shut down")
							}
							
							log.info("clearing lbq")
							lbq.clear()

							blockingQueues.remove(entry.getKey())
							executors.remove(entry.getKey())
						}

						sleep 5000
					}
				}
			}
		}
	}

	StreamService() {
		blockingQueues = new ConcurrentHashMap<String,BlockingQueue<JsonNode>>()
		executors = new ConcurrentHashMap<String,ExecutorService>()
		threads = new ConcurrentHashMap<String,Set<Thread>>()

		backgroundThread.start()
	}

	Stream makeStream(String clientId, boolean isWebsocket, long sleeptime) {

		log.info("beginning to make stream")

		BlockingQueue<JsonNode> lbq = new LinkedBlockingQueue<JsonNode>(5000)
		ExecutorService es = Executors.newFixedThreadPool(8)

		Set<Thread> mythreads = startExecutorThreads(es, lbq, sleeptime)

		blockingQueues.put(clientId, lbq)
		executors.put(clientId, es)
		threads.put(clientId, mythreads)

		Stream.from {  lbq  }.map {
			def i = it.take()
			i
		}.map {
			if (!isWebsocket)
				Unpooled.copiedBuffer(it.toString().bytes)
			else
				it.toString()
		}
	}

	Set<Thread> startExecutorThreads(ExecutorService es, LinkedBlockingQueue<JsonNode> lbq, long sleepval) {

		def threads = []
		
		8.times {

			def t = new Thread() {

				boolean shutdown = false
				
						void run() {

							ObjectMapper om = new ObjectMapper()

							while(!shutdown) {

								String json = """{"ni":{"authored_date":"2014-11-28T14:53:00","author":"marsiorio","language":"it","source":"www.nuovarassegna.it","title":"Scontri tra tifosi: Lazio-Perugia, 5 deferiti","type":"WORDPRESSORG_POST","tld":"it","url":"http://www.nuovarassegna.it/sport/scontri-tra-tifosi-lazio-perugia-5-deferiti","fulldomain":"www.nuovarassegna.it","content":"BELLUNO - Cinque ultrà laziali sono stati deferiti all'autorità giudiziaria, e saranno raggiunti da daspo, dalla Questura di Belluno per gli incidenti del 26 luglio scorso ad Auronzo di Cadore per l'incontro Lazio-Perugia. Un gruppo di ultrà si era portato a ridosso della recinzione dei tifosi del Perugia e alcuni avevano cominciato a lanciare pietre e altri oggetti, oltre a abbattere la rete di protezione. Solo l'intervento di Polizia e Carabinieri aveva impedito il contatto tra le tifoserie. This entry passed through the Full-Text RSS service - if this is your content and you're reading it on someone else's site, please read the FAQ at fivefilters.org/content-only/faq.php#publishers. Vai all'articolo originale Via: Corriere dello sport","thread_id":"article:wordpress.com:71602831:32553","provider":"GNIP","domain":"nuovarassegna.it","id":"e5f9f76d4606fbdcbbc5a863e08201ea83d26630","author_id":"69745c20f7bbb7484d0c7b3e67c893ea","timestamp":"2014-11-28T17:02:51"},"raw":{"wordpressorgpost_entry":{"verb":"post","id":"tag:gnip.wordpress.com:2012:blog/71602831/post/32553","postedTime":"2014-11-28T17:02:34.000Z","displayName":"Scontri tra tifosi: Lazio-Perugia, 5 deferiti","provider":{"objectType":"service","displayName":"WordPress","link":"http://im.wordpress.com:8008/posts.json"},"actor":{"objectType":"person","displayName":"marsiorio","id":"person:wordpress.com:67367191","wpEmailMd5":"69745c20f7bbb7484d0c7b3e67c893ea","link":"http://gravatar.com/marsiorio"},"target":{"objectType":"blog","displayName":"Nuova Rassegna","link":"http://www.nuovarassegna.it/","wpBlogId":71602831,"feed":"http://www.nuovarassegna.it/feed/","summary":"Tutte le notizie dell&#039;ultima ora"},"object":{"objectType":"article","displayName":"Scontri tra tifosi: Lazio-Perugia, 5 deferiti","link":"http://www.nuovarassegna.it/sport/scontri-tra-tifosi-lazio-perugia-5-deferiti","wpPostId":32553,"summary":"BELLUNO &#8211; Cinque ultr&#224; laziali sono stati deferiti all&#8217;autorit&#224; giudiziaria, e saranno raggiunti da daspo, dalla Questura di Belluno per gli incidenti del 26 luglio scorso ad Auronzo di Cadore per l&#8217;incontro Lazio-Perugia. Un gruppo di ultr&#224; si era portato &#8230","content":"<strong>BELLUNO</strong> - Cinque ultrà laziali sono stati deferiti all'autorità giudiziaria, e saranno raggiunti da daspo, dalla Questura di Belluno per gli incidenti del 26 luglio scorso ad Auronzo di Cadore per l'incontro Lazio-Perugia. Un gruppo di ultrà si era portato a ridosso della recinzione dei tifosi del Perugia e alcuni avevano cominciato a lanciare pietre e altri oggetti, oltre a abbattere la rete di protezione. Solo l'intervento di Polizia e Carabinieri aveva impedito il contatto tra le tifoserie.<p><em>This entry passed through the Full-Text RSS service - if this is your content and you're reading it on someone else's site, please read the FAQ at ","updatedTime":"2014-11-28T14:53:00Z","postedTime":"2014-11-28T14:53:00Z","wpBlogId":71602831,"id":"article:wordpress.com:71602831:32553","tags":[{"objectType":"category","displayName":"sport","link":"http://it.wordpress.com/tag/sport"}]},"gnip":{"urls":[{"url":"http://fivefilters.org/content-only/faq.php#publishers","expanded_url":"http://fivefilters.org/content-only/faq.php#publishers","expanded_status":200},{"url":"http://corrieredellosport.feedsportal.com/c/34176/f/619144/s/40efac3c/sc/13/l/0L0Scorrieredellosport0Bit0Ccalcio0Cserie0Ia0Clazio0C20A140C110C280E3860A0A60CScontri0Ktra0Ktifosi0J3A0KLazio0EPerugia0J2C0K50Kdeferiti0K/story01.htm","expanded_url":"http://www.corrieredellosport.it/calcio/serie_a/lazio/2014/11/28-386006/Scontri%20tra%20tifosi:%20Lazio-Perugia,%205%20deferiti%20","expanded_status":200}],"language":{"value":"it"}}}},"classify_results":{"dataview_results":["7779"],"sentiment":["0"],"is_large":["false"],"size":["747"],"elasticsearch_size":["2369"],"language":["it"],"dataview":["7779"],"elasticsearch_size_is_large":["false"]}}"""
								JsonNode node = om.readTree(json)
								lbq.put(node)

								try {
									sleep(sleepval)
								} catch (InterruptedException e) {
									log.info("caught interrupted exception ", e)
									shutdown = true
								}
							}
						}
					}
			
			es.submit(t)
			threads << t
		}
		threads
	}
}

