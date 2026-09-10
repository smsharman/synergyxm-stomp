(ns synergyxm.stomp-test
  "Frame codec and URL rules as pure tests; everything connection-shaped
   against a tiny in-process WebSocket server (Java-WebSocket, test scope)
   that speaks just enough STOMP: it answers CONNECT with CONNECTED, records
   every frame, and pushes MESSAGE frames on demand.

   The live test at the bottom is skipped unless SYNERGYXM_LIVE_BASE_URL and
   SYNERGYXM_LIVE_API_KEY are set."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [jsonista.core :as json]
            [synergyxm.stomp :as stomp])
  (:import [java.net InetSocketAddress URI]
           [java.net.http HttpClient HttpClient$Version HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
           [java.nio.charset StandardCharsets]
           [java.util Base64]
           [java.util.concurrent LinkedBlockingQueue]
           [org.java_websocket WebSocket]
           [org.java_websocket.server WebSocketServer]))

(def ^:private NUL "\u0000")

;; =============================================================================
;; Frames
;; =============================================================================

(deftest encode-frame-shape
  (is (= (str "DISCONNECT\n\n" NUL) (stomp/encode-frame "DISCONNECT")))
  (is (= (str "SEND\ndestination:/queue/a\ncontent-length:2\n\nhi" NUL)
         (stomp/encode-frame "SEND" {"destination" "/queue/a"} "hi")))
  (testing "no content-length for an empty body"
    (is (not (str/includes? (stomp/encode-frame "SUBSCRIBE" {"id" "sub-1"}) "content-length"))))
  (testing "content-length counts bytes, not characters"
    (is (str/includes? (stomp/encode-frame "SEND" {} "é") "content-length:2")))
  (testing "nil-valued headers are dropped"
    (is (= (str "SEND\na:1\n\n" NUL) (stomp/encode-frame "SEND" {"a" "1" "b" nil}))))
  (testing "keyword header names are used as-is"
    (is (str/includes? (stomp/encode-frame "SEND" {:destination "/x"}) "destination:/x"))))

(deftest header-escaping
  (is (str/includes? (stomp/encode-frame "SEND" {"a:b" "x\\y\nz\r:"}) "a\\cb:x\\\\y\\nz\\r\\c"))
  (testing "CONNECT and CONNECTED are not escaped (spec)"
    (is (str/includes? (stomp/encode-frame "CONNECT" {"passcode" "a:b\\c"}) "passcode:a:b\\c"))
    (is (str/includes? (stomp/encode-frame "CONNECTED" {"server" "a:b"}) "server:a:b"))))

(deftest decode-frame-basics
  (let [f (stomp/decode-frame (stomp/encode-frame "MESSAGE" {"subscription" "sub-1"} "hello"))]
    (is (= "MESSAGE" (:command f)))
    (is (= "sub-1" (get-in f [:headers "subscription"])))
    (is (= "hello" (:body f))))
  (testing "the trailing NUL is optional"
    (is (= "hello" (:body (stomp/decode-frame "MESSAGE\nid:1\n\nhello")))))
  (testing "an empty body"
    (is (= "" (:body (stomp/decode-frame (stomp/encode-frame "RECEIPT" {"receipt-id" "1"}))))))
  (testing "a header line without a colon is ignored"
    (is (= {"id" "1"} (:headers (stomp/decode-frame "MESSAGE\ngarbage\nid:1\n\n")))))
  (testing "CONNECTED header values are not unescaped"
    (is (= "a\\cb" (get-in (stomp/decode-frame "CONNECTED\nserver:a\\cb\n\n") [:headers "server"])))))

(deftest decode-frame-heartbeats-are-nil
  (is (nil? (stomp/decode-frame "\n")))
  (is (nil? (stomp/decode-frame "\r\n")))
  (is (nil? (stomp/decode-frame "")))
  (is (nil? (stomp/decode-frame NUL)))
  (testing "leading heart-beats before a real frame are stripped"
    (is (= "MESSAGE" (:command (stomp/decode-frame (str "\n\n" (stomp/encode-frame "MESSAGE" {"id" "1"}))))))))

(deftest decode-frame-content-length
  (testing "content-length bounds the body"
    (is (= "ab" (:body (stomp/decode-frame "MESSAGE\ncontent-length:2\n\nabcd")))))
  (testing "content-length counts bytes"
    (is (= "é" (:body (stomp/decode-frame (stomp/encode-frame "MESSAGE" {} "é")))))))

(deftest decode-frame-duplicate-headers
  (is (= "first" (get-in (stomp/decode-frame "MESSAGE\nid:first\nid:second\n\n") [:headers "id"]))))

(deftest decode-frame-crlf
  (let [f (stomp/decode-frame (str "MESSAGE\r\nid:1\r\ncontent-length:5\r\n\r\nhello" NUL))]
    (is (= "MESSAGE" (:command f)))
    (is (= {"id" "1" "content-length" "5"} (:headers f)))
    (is (= "hello" (:body f))))
  (testing "CRLF without content-length"
    (is (= "hello" (:body (stomp/decode-frame (str "MESSAGE\r\nid:1\r\n\r\nhello" NUL)))))))

(deftest round-trips
  (doseq [[command headers body]
          [["CONNECT" {"host" "jobs" "passcode" "to:ken"} nil]
           ["SUBSCRIBE" {"id" "sub-1" "destination" "/amq/queue/q"} nil]
           ["SEND" {"destination" "/exchange/e/rk" "user-id" "n-1"} "{\"a\": 1}"]
           ["ACK" {"id" "ack-1"} nil]
           ["NACK" {"id" "ack-1" "requeue" "false"} nil]
           ["MESSAGE" {"weird" "a:b\\c\nd\re"} "body"]]]
    (let [f (stomp/decode-frame (stomp/encode-frame command headers body))]
      (is (= command (:command f)))
      (is (= (or body "") (:body f)))
      (doseq [[k v] headers] (is (= v (get-in f [:headers k])) (str command " header " k))))))

(deftest escaped-backslash-is-not-a-newline
  (testing "a header value of backslash+n must not decode to a newline"
    (let [value (str \\ "n")
          f     (stomp/decode-frame (stomp/encode-frame "MESSAGE" {"k" value}))]
      (is (= value (get-in f [:headers "k"]))))))

;; =============================================================================
;; ws-url
;; =============================================================================

(deftest ws-url-rules
  (testing "override wins"
    (is (= "ws://override/ws" (stomp/ws-url {:host "example.com" :ws-url "ws://a:15674/ws"} "ws://override/ws"))))
  (testing "then the advertised ws-url"
    (is (= "ws://a:15674/ws" (stomp/ws-url {:host "example.com" :ws-url "ws://a:15674/ws"})))
    (is (= "ws://a:15674/ws" (stomp/ws-url {:host "example.com" :ws-url "ws://a:15674/ws"} ""))))
  (testing "derived: ws for loopback and private hosts, wss otherwise"
    (is (= "ws://192.168.1.241:15674/ws" (stomp/ws-url {:host "192.168.1.241" :port 5672})))
    (is (= "ws://localhost:15674/ws" (stomp/ws-url {:host "localhost"})))
    (is (= "ws://127.0.0.1:15674/ws" (stomp/ws-url {:host "127.0.0.1"})))
    (is (= "ws://10.0.0.5:15674/ws" (stomp/ws-url {:host "10.0.0.5"})))
    (is (= "wss://broker.synergyxm.com:15674/ws" (stomp/ws-url {:host "broker.synergyxm.com"}))))
  (testing "default host"
    (is (= "ws://localhost:15674/ws" (stomp/ws-url {}))))
  (testing "tls? forces the scheme"
    (is (= "wss://localhost:15674/ws" (stomp/ws-url {:host "localhost"} nil true)))
    (is (= "ws://broker.synergyxm.com:15674/ws" (stomp/ws-url {:host "broker.synergyxm.com"} nil false)))))

;; =============================================================================
;; A STOMP-speaking test server
;; =============================================================================

(defn- start-server!
  "A WebSocket server that answers CONNECT with CONNECTED and records frames.
   Returns {:server :port :frames :sockets :closed}."
  []
  (let [frames  (atom [])
        sockets (atom [])
        closed  (atom [])
        started (promise)
        server  (proxy [WebSocketServer] [(InetSocketAddress. "127.0.0.1" 0)]
                  (onStart [] (deliver started true))
                  (onOpen [conn _handshake] (swap! sockets conj conn))
                  (onClose [_conn code _reason remote] (swap! closed conj {:code code :remote remote}))
                  (onError [_conn _e] nil)
                  (onMessage [conn message]
                    (when (string? message)
                      (doseq [part (remove str/blank? (str/split message (re-pattern NUL)))]
                        (when-let [frame (stomp/decode-frame part)]
                          (swap! frames conj frame)
                          (when (= "CONNECT" (:command frame))
                            (.send ^WebSocket conn
                                   (stomp/encode-frame "CONNECTED" {"version" "1.2"
                                                                    "heart-beat" "10000,10000"}))))))))]
    (.setReuseAddr ^WebSocketServer server true)
    (.start ^WebSocketServer server)
    (assert (deref started 10000 false) "test WebSocket server did not start")
    {:server server :port (.getPort ^WebSocketServer server)
     :frames frames :sockets sockets :closed closed}))

(defn- stop-server! [{:keys [^WebSocketServer server]}]
  (try (.stop server 1000) (catch Throwable _ nil)))

(defn- url [srv] (str "ws://127.0.0.1:" (:port srv) "/ws"))

(defn- frames-of [srv command] (filterv #(= command (:command %)) @(:frames srv)))

(defn- wait-for
  "Poll `f` for up to `ms` (5 s); returns its truthy value or nil."
  ([f] (wait-for f 5000))
  ([f ms]
   (let [deadline (+ (System/currentTimeMillis) ms)]
     (loop []
       (or (f)
           (when (< (System/currentTimeMillis) deadline)
             (Thread/sleep 10)
             (recur)))))))

(defn- push-message!
  "Send a MESSAGE frame from the server to the newest client socket."
  [srv headers body]
  (.send ^WebSocket (last @(:sockets srv)) (stomp/encode-frame "MESSAGE" headers body)))

(defmacro with-server [[sym] & body]
  `(let [~sym (start-server!)]
     (try ~@body (finally (stop-server! ~sym)))))

(defn- connect! [srv opts]
  (stomp/connect (merge {:url (url srv) :vhost "jobs" :token "tok-1"
                         :heartbeat-ms 60000 :connect-timeout 10}
                        opts)))

;; =============================================================================
;; Connection
;; =============================================================================

(deftest connect-sends-a-connect-frame
  (with-server [srv]
    (let [conn (connect! srv {})]
      (try
        (is (stomp/connected? conn))
        (is (= {"accept-version" "1.2,1.1" "host" "jobs" "login" ""
                "passcode" "tok-1" "heart-beat" "60000,60000"}
               (:headers (first (frames-of srv "CONNECT")))))
        (finally (stomp/shutdown! conn))))))

(deftest connect-rejects-on-an-error-frame
  (let [started (promise)
        server  (proxy [WebSocketServer] [(InetSocketAddress. "127.0.0.1" 0)]
                  (onStart [] (deliver started true))
                  (onOpen [_ _] nil)
                  (onClose [_ _ _ _] nil)
                  (onError [_ _] nil)
                  (onMessage [conn message]
                    (when (string? message)
                      (.send ^WebSocket conn
                             (stomp/encode-frame "ERROR" {"message" "not_authorised"} "bad passcode")))))]
    (.start ^WebSocketServer server)
    (assert (deref started 10000 false))
    (try
      (let [e (try (stomp/connect {:url (str "ws://127.0.0.1:" (.getPort ^WebSocketServer server) "/ws")
                                   :vhost "jobs" :token "nope" :connect-timeout 10})
                   nil
                   (catch clojure.lang.ExceptionInfo e e))]
        (is (some? e) "connect must throw on ERROR")
        (is (= :stomp-error (:type (ex-data e))))
        (is (str/includes? (ex-message e) "not_authorised"))
        (is (str/includes? (ex-message e) "bad passcode")))
      (finally (.stop ^WebSocketServer server 1000)))))

(deftest subscribe-sends-the-right-frame
  (with-server [srv]
    (let [conn (connect! srv {})]
      (try
        (let [sub-id (stomp/subscribe! conn "jobs.site.node" (fn [_]))]
          (is (= "sub-1" sub-id))
          (is (wait-for #(seq (frames-of srv "SUBSCRIBE"))))
          (is (= {"id" "sub-1" "destination" "/amq/queue/jobs.site.node"
                  "ack" "client-individual" "prefetch-count" "1"}
                 (:headers (first (frames-of srv "SUBSCRIBE"))))))
        (finally (stomp/shutdown! conn))))))

(deftest handler-receives-the-message-and-acks
  (with-server [srv]
    (let [conn (connect! srv {})
          got  (atom nil)]
      (try
        (let [sub-id (stomp/subscribe! conn "q" (fn [m] (reset! got m) ((:ack! m))))]
          (wait-for #(seq (frames-of srv "SUBSCRIBE")))
          (push-message! srv {"subscription" sub-id "ack" "ack-7" "message-id" "m-1"} "{\"a\":1}")
          (is (wait-for #(some? @got)))
          (is (= "{\"a\":1}" (:body @got)))
          (is (= "ack-7" (get-in @got [:headers "ack"])))
          (is (fn? (:nack! @got)))
          (is (wait-for #(seq (frames-of srv "ACK"))))
          (is (= {"id" "ack-7"} (:headers (first (frames-of srv "ACK"))))))
        (finally (stomp/shutdown! conn))))))

(deftest handler-nack-carries-requeue-false
  (with-server [srv]
    (let [conn (connect! srv {})]
      (try
        (let [sub-id (stomp/subscribe! conn "q" (fn [m] ((:nack! m))))]
          (wait-for #(seq (frames-of srv "SUBSCRIBE")))
          (push-message! srv {"subscription" sub-id "ack" "ack-9"} "x")
          (is (wait-for #(seq (frames-of srv "NACK"))))
          (is (= {"id" "ack-9" "requeue" "false"} (:headers (first (frames-of srv "NACK"))))))
        (finally (stomp/shutdown! conn))))))

(deftest ack-id-falls-back-to-message-id
  (with-server [srv]
    (let [conn (connect! srv {})]
      (try
        (let [sub-id (stomp/subscribe! conn "q" (fn [m] ((:ack! m))))]
          (wait-for #(seq (frames-of srv "SUBSCRIBE")))
          (push-message! srv {"subscription" sub-id "message-id" "m-42"} "x")
          (is (wait-for #(seq (frames-of srv "ACK"))))
          (is (= {"id" "m-42"} (:headers (first (frames-of srv "ACK"))))))
        (finally (stomp/shutdown! conn))))))

(deftest a-throwing-handler-does-not-kill-the-connection
  (with-server [srv]
    (let [conn (connect! srv {})
          seen (atom 0)]
      (try
        (let [sub-id (stomp/subscribe! conn "q" (fn [_] (swap! seen inc) (throw (ex-info "boom" {}))))]
          (wait-for #(seq (frames-of srv "SUBSCRIBE")))
          (push-message! srv {"subscription" sub-id "ack" "a1"} "one")
          (is (wait-for #(= 1 @seen)))
          (push-message! srv {"subscription" sub-id "ack" "a2"} "two")
          (is (wait-for #(= 2 @seen)))
          (is (stomp/connected? conn)))
        (finally (stomp/shutdown! conn))))))

(deftest error-frames-reach-on-error
  (with-server [srv]
    (let [errors (atom [])
          conn   (connect! srv {:on-error #(swap! errors conj %)})]
      (try
        (.send ^WebSocket (last @(:sockets srv))
               (stomp/encode-frame "ERROR" {"message" "access_refused"} "queue gone"))
        (is (wait-for #(seq @errors)))
        (is (str/includes? (first @errors) "access_refused"))
        (finally (stomp/shutdown! conn))))))

(deftest publish-builds-the-destination
  (with-server [srv]
    (let [conn (connect! srv {})]
      (try
        (stomp/publish! conn "job-events" "job.site.node.CAMERA_JOB_RECEIVED" "{\"e\":1}" {:user-id "node-uuid"})
        (is (wait-for #(seq (frames-of srv "SEND"))))
        (let [f (first (frames-of srv "SEND"))]
          (is (= "/exchange/job-events/job.site.node.CAMERA_JOB_RECEIVED" (get-in f [:headers "destination"])))
          (is (= "application/json" (get-in f [:headers "content-type"])))
          (is (= "true" (get-in f [:headers "persistent"])))
          (is (= "node-uuid" (get-in f [:headers "user-id"])))
          (is (= "7" (get-in f [:headers "content-length"])))
          (is (nil? (get-in f [:headers "message-id"])) "the broker rejects message-id on SEND")
          (is (= "{\"e\":1}" (:body f))))
        (finally (stomp/shutdown! conn))))))

(deftest publish-without-persistence-or-user-id
  (with-server [srv]
    (let [conn (connect! srv {})]
      (try
        (stomp/publish! conn "e" "rk" "x" {:persistent? false :content-type "text/plain"})
        (is (wait-for #(seq (frames-of srv "SEND"))))
        (let [f (first (frames-of srv "SEND"))]
          (is (nil? (get-in f [:headers "persistent"])))
          (is (nil? (get-in f [:headers "user-id"])))
          (is (= "text/plain" (get-in f [:headers "content-type"]))))
        (finally (stomp/shutdown! conn))))))

(deftest reconnect-resubscribes-with-the-new-token
  (with-server [srv]
    (let [conn (connect! srv {})
          got  (atom nil)]
      (try
        (let [sub-id (stomp/subscribe! conn "jobs.q" (fn [m] (reset! got m) ((:ack! m))))]
          (is (wait-for #(seq (frames-of srv "SUBSCRIBE"))))
          (stomp/reconnect! conn "tok-2")
          (is (stomp/connected? conn))
          (is (wait-for #(= 2 (count (frames-of srv "CONNECT")))))
          (is (= "tok-2" (get-in (second (frames-of srv "CONNECT")) [:headers "passcode"])))
          (is (wait-for #(= 2 (count (frames-of srv "SUBSCRIBE")))))
          (let [f (second (frames-of srv "SUBSCRIBE"))]
            (is (= sub-id (get-in f [:headers "id"])) "the same subscription id is reused")
            (is (= "/amq/queue/jobs.q" (get-in f [:headers "destination"]))))
          (testing "and messages keep flowing over the new socket"
            (push-message! srv {"subscription" sub-id "ack" "ack-1"} "after")
            (is (wait-for #(some? @got)))
            (is (= "after" (:body @got)))))
        (finally (stomp/shutdown! conn))))))

(deftest close-does-not-invoke-on-closed
  (with-server [srv]
    (let [closes (atom [])
          conn   (connect! srv {:on-closed #(swap! closes conj %)})]
      (stomp/close! conn)
      (is (wait-for #(seq (frames-of srv "DISCONNECT"))) "close! sends DISCONNECT")
      (is (false? (stomp/connected? conn)))
      (Thread/sleep 300)
      (is (empty? @closes) "a deliberate close is not a drop")
      (testing "close! is idempotent"
        (stomp/close! conn)
        (Thread/sleep 100)
        (is (= 1 (count (frames-of srv "DISCONNECT")))))
      (stomp/shutdown! conn))))

(deftest an-unexpected-close-invokes-on-closed-once
  (with-server [srv]
    (let [closes (atom [])
          conn   (connect! srv {:on-closed #(swap! closes conj %)})]
      (try
        (is (wait-for #(seq @(:sockets srv))))
        (.close ^WebSocket (last @(:sockets srv)))
        (is (wait-for #(seq @closes)) "the drop must be reported")
        (Thread/sleep 300)
        (is (= 1 (count @closes)) "exactly once")
        (is (false? (stomp/connected? conn)))
        (finally (stomp/shutdown! conn))))))

(deftest a-superseded-socket-closing-does-not-invoke-on-closed
  (with-server [srv]
    (let [closes (atom [])
          conn   (connect! srv {:on-closed #(swap! closes conj %)})]
      (try
        (stomp/subscribe! conn "q" (fn [_]))
        (is (wait-for #(seq (frames-of srv "SUBSCRIBE"))))
        (let [old (last @(:sockets srv))]
          (stomp/reconnect! conn)
          (is (wait-for #(= 2 (count @(:sockets srv)))))
          (is (not (identical? old (last @(:sockets srv)))))
          (.close ^WebSocket old)
          (Thread/sleep 400)
          (is (empty? @closes) "the superseded socket must not report a drop")
          (is (stomp/connected? conn)))
        (finally (stomp/shutdown! conn))))))

(deftest heartbeats-are-sent
  (with-server [srv]
    (let [conn (connect! srv {:heartbeat-ms 50})]
      (try
        (is (= "50,50" (get-in (first (frames-of srv "CONNECT")) [:headers "heart-beat"])))
        ;; heart-beats decode to nil, so they must never show up as frames
        (Thread/sleep 300)
        (is (empty? (remove #(= "CONNECT" (:command %)) @(:frames srv))))
        (is (stomp/connected? conn))
        (finally (stomp/shutdown! conn))))))

(deftest run-with-refresh-reconnects-with-a-refreshed-token
  (with-server [srv]
    (let [drops  (LinkedBlockingQueue.)
          conn   (connect! srv {:on-closed #(.offer drops %)})
          needs? (atom true)
          after  (atom 0)
          stop   (promise)]
      (try
        (stomp/subscribe! conn "q" (fn [_]))
        (is (wait-for #(seq (frames-of srv "SUBSCRIBE"))))
        (let [worker (future (stomp/run-with-refresh!
                              conn {:drops drops :check-ms 50 :stop-promise stop
                                    :needs-refresh?   (fn [] @needs?)
                                    :refresh!         (fn [] (reset! needs? false) "tok-refreshed")
                                    :after-reconnect! (fn [] (swap! after inc))}))]
          (is (wait-for #(= 2 (count (frames-of srv "CONNECT")))) "it should reconnect")
          (is (= "tok-refreshed" (get-in (second (frames-of srv "CONNECT")) [:headers "passcode"])))
          (is (wait-for #(= 2 (count (frames-of srv "SUBSCRIBE")))) "and re-subscribe")
          (is (wait-for #(= 1 @after)) "after-reconnect! is called")
          (Thread/sleep 200)
          (is (= 1 @after) "and only once, since needs-refresh? is now false")
          (deliver stop true)
          (is (not= ::timeout (deref worker 5000 ::timeout)) "the loop stops on the stop-promise"))
        (finally (stomp/shutdown! conn))))))

(deftest run-with-refresh-reconnects-after-a-drop
  (with-server [srv]
    (let [drops (LinkedBlockingQueue.)
          conn  (connect! srv {:on-closed #(.offer drops %)})
          after (atom 0)
          stop  (promise)]
      (try
        (stomp/subscribe! conn "q" (fn [_]))
        (is (wait-for #(seq (frames-of srv "SUBSCRIBE"))))
        (let [worker (future (stomp/run-with-refresh!
                              conn {:drops drops :check-ms 50 :stop-promise stop
                                    :needs-refresh?   (fn [] false)
                                    :after-reconnect! (fn [] (swap! after inc))}))]
          (.close ^WebSocket (last @(:sockets srv)))
          (is (wait-for #(= 2 (count (frames-of srv "CONNECT"))) 15000) "it should reconnect after the drop")
          (is (wait-for #(= 2 (count (frames-of srv "SUBSCRIBE"))) 5000))
          (is (wait-for #(= 1 @after)))
          (is (= "tok-1" (get-in (second (frames-of srv "CONNECT")) [:headers "passcode"])))
          (deliver stop true)
          (is (not= ::timeout (deref worker 5000 ::timeout))))
        (finally (stomp/shutdown! conn))))))

;; =============================================================================
;; Live (skipped unless the env vars are set)
;; =============================================================================

(def ^:private live-base-url (System/getenv "SYNERGYXM_LIVE_BASE_URL"))
(def ^:private live-api-key (System/getenv "SYNERGYXM_LIVE_API_KEY"))
(def ^:private live-ws-url (System/getenv "SYNERGYXM_LIVE_WS_URL"))
(def ^:private live-mgmt-url (System/getenv "SYNERGYXM_LIVE_MGMT_URL"))
(def ^:private live? (boolean (and (not-empty live-base-url) (not-empty live-api-key))))

(defn- http-post [url body headers]
  (let [client (-> (HttpClient/newBuilder) (.version HttpClient$Version/HTTP_1_1) .build)
        req    (reduce (fn [b [k v]] (.header b k v))
                       (-> (HttpRequest/newBuilder (URI/create url))
                           (.header "content-type" "application/json")
                           (.POST (HttpRequest$BodyPublishers/ofString body)))
                       headers)
        resp   (.send client (.build req) (HttpResponse$BodyHandlers/ofString))]
    (when-not (<= 200 (.statusCode resp) 299)
      (throw (ex-info (str "HTTP " (.statusCode resp) ": " (.body resp)) {:status (.statusCode resp)})))
    (json/read-value (.body resp) json/keyword-keys-object-mapper)))

(defn- auth-machine []
  (http-post (str (str/replace live-base-url #"/$" "") "/auth/machine")
             (json/write-value-as-string {:api-key live-api-key})
             {}))

(defn- mgmt-publish! [vhost exchange routing-key payload]
  (let [uri         (URI/create live-mgmt-url)
        [user pass] (str/split (or (.getUserInfo uri) "guest:guest") #":")
        root        (str (.getScheme uri) "://" (.getHost uri) (when (pos? (.getPort uri)) (str ":" (.getPort uri))))
        auth        (.encodeToString (Base64/getEncoder) (.getBytes (str user ":" pass) StandardCharsets/UTF_8))]
    (http-post (str root "/api/exchanges/" vhost "/" exchange "/publish")
               (json/write-value-as-string {:properties {:delivery_mode 2 :content_type "application/json"}
                                            :routing_key routing-key
                                            :payload payload
                                            :payload_encoding "string"})
               {"authorization" (str "Basic " auth)})))

(deftest ^:live live-broker-round-trip
  (if-not live?
    (is true "skipped: set SYNERGYXM_LIVE_BASE_URL and SYNERGYXM_LIVE_API_KEY")
    (let [session (auth-machine)
          broker  (:broker session)
          queue   (:job-queue broker)
          conn    (stomp/connect {:url (stomp/ws-url broker live-ws-url)
                                  :vhost (or (:vhost broker) "jobs")
                                  :token (:access-token session)})
          got     (atom nil)
          test-id (str (random-uuid))]
      (try
        (is (stomp/connected? conn))
        (stomp/subscribe! conn queue
                          (fn [m]
                            (let [payload (json/read-value (:body m) json/keyword-keys-object-mapper)]
                              (when (= test-id (:test-id payload)) (reset! got payload))
                              ((:ack! m)))))
        (testing "publishing an event"
          (stomp/publish! conn (or (:events-exchange broker) "job-events")
                          (str "job." (subs queue (count "jobs.")) ".CAMERA_JOB_RECEIVED")
                          (json/write-value-as-string {:event "CAMERA_JOB_RECEIVED"
                                                       :node (:node-uuid session)
                                                       :test-id test-id})
                          {:user-id (:node-uuid session)})
          (Thread/sleep 500)
          (is (stomp/connected? conn) "an illegal publish would have dropped the connection"))
        (when (not-empty live-mgmt-url)
          (testing "an injected dispatch is delivered and acked"
            (let [out (mgmt-publish! (or (:vhost broker) "jobs") "jobs"
                                     (str "job." (subs queue (count "jobs.")) ".TEST_DISPATCH")
                                     (json/write-value-as-string {:job-type "TEST_DISPATCH" :test-id test-id}))]
              (is (true? (:routed out)) (str "not routed to " queue)))
            (is (wait-for #(some? @got) 15000) "the dispatch should reach the handler")))
        (finally (stomp/shutdown! conn))))))
