(ns synergyxm.stomp
  "STOMP 1.2 over WebSocket for SynergyXM workers, against RabbitMQ's
   `rabbitmq_web_stomp` plugin. Uses the JDK's `java.net.http.WebSocket`, so
   there is no extra dependency.

   Shape, for a worker main:

     (let [conn (stomp/connect {:url (stomp/ws-url broker) :vhost vhost
                                :token token :on-closed (fn [reason] ...)})]
       (stomp/subscribe! conn job-queue
         (fn [{:keys [body ack! nack!]}]
           (try (handle (json/read-value body)) (ack!)
                (catch Exception e (nack!)))))
       (stomp/publish! conn \"job-events\" rk body {:user-id node-uuid}))

   Token refresh is a reconnect (STOMP has no update-secret): `reconnect!`
   closes, connects with the new token and re-subscribes every active
   subscription. `run-with-refresh!` wraps that in the loop the three
   Clojure workers share: refresh before expiry, reconnect with backoff
   when the socket drops.

   Mapping to AMQP: CONNECT login \"\" / passcode = node JWT / host = vhost;
   SUBSCRIBE /amq/queue/<queue> ack:client-individual prefetch-count:1;
   ACK/NACK(requeue:false) with the MESSAGE frame's `ack` header;
   SEND /exchange/<exchange>/<routing-key> with user-id, content-type,
   persistent (no message-id — the broker rejects it)."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [java.net URI]
           [java.net.http HttpClient WebSocket WebSocket$Listener]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.time Duration]
           [java.util.concurrent CompletableFuture CompletionStage TimeUnit
            Executors ScheduledExecutorService LinkedBlockingQueue TimeoutException]))

(def default-ws-port 15674)
(def default-ws-path "/ws")

;; =============================================================================
;; Frames
;; =============================================================================

(defn- escape [^String s raw?]
  (if raw? s
      (-> s (str/replace "\\" "\\\\") (str/replace "\r" "\\r")
          (str/replace "\n" "\\n") (str/replace ":" "\\c"))))

(defn- unescape [^String s raw?]
  (if (or raw? (not (str/includes? s "\\"))) s
      (-> s (str/replace "\\r" "\r") (str/replace "\\n" "\n")
          (str/replace "\\c" ":") (str/replace "\\\\" "\\"))))

(defn encode-frame
  "Serialise a frame to a String (the WebSocket text payload). Adds
   content-length for a non-empty body."
  ([command] (encode-frame command {} nil))
  ([command headers] (encode-frame command headers nil))
  ([command headers ^String body]
   (let [raw?    (contains? #{"CONNECT" "CONNECTED"} command)
         body    (or body "")
         blen    (count (.getBytes body StandardCharsets/UTF_8))
         headers (cond-> (into {} (remove (comp nil? val)) headers)
                   (pos? blen) (assoc "content-length" (str blen)))]
     (str command "\n"
          (str/join "\n" (map (fn [[k v]] (str (escape (name k) raw?) ":" (escape (str v) raw?))) headers))
          "\n\n" body "\u0000"))))

(defn decode-frame
  "Parse one complete frame (a text payload without the trailing NUL, or
   with it). Returns {:command :headers :body} or nil for a heart-beat."
  [^String s]
  (let [s (str/replace s #"^[\r\n]+" "")
        s (if (str/ends-with? s "\u0000") (subs s 0 (dec (count s))) s)]
    (when-not (str/blank? s)
      (let [sep      (or (str/index-of s "\n\n") (count s))
            head     (str/replace (subs s 0 sep) "\r\n" "\n")
            body     (if (< (+ sep 2) (count s)) (subs s (+ sep 2)) "")
            [command & lines] (str/split-lines head)
            command  (str/trim command)
            raw?     (contains? #{"CONNECT" "CONNECTED"} command)
            headers  (reduce (fn [m line]
                               (if-let [i (str/index-of line ":")]
                                 (let [k (unescape (subs line 0 i) raw?)]
                                   (if (contains? m k) m (assoc m k (unescape (subs line (inc i)) raw?))))
                                 m))
                             {} lines)
            body     (if-let [cl (get headers "content-length")]
                       (let [n (Long/parseLong cl)
                             bytes (.getBytes body StandardCharsets/UTF_8)]
                         (String. bytes 0 (min n (alength bytes)) StandardCharsets/UTF_8))
                       body)]
        {:command command :headers headers :body body}))))

;; =============================================================================
;; URL
;; =============================================================================

(defn ws-url
  "The WebSocket URL for a broker map from /auth/machine: `override` wins
   (SYNERGYXM_BROKER_WS_URL in worker.conf), then the advertised :ws-url,
   else ws(s)://<host>:15674/ws (ws for loopback/private hosts, wss otherwise;
   `tls?` forces it)."
  ([broker] (ws-url broker nil nil))
  ([broker override] (ws-url broker override nil))
  ([broker override tls?]
   (or (not-empty override)
       (not-empty (:ws-url broker))
       (let [host (or (:host broker) "localhost")
             tls? (if (nil? tls?)
                    (not (or (contains? #{"localhost" "127.0.0.1" "::1"} host)
                             (some #(str/starts-with? host %) ["10." "192.168." "172."])))
                    tls?)]
         (str (if tls? "wss" "ws") "://" host ":" default-ws-port default-ws-path)))))

;; =============================================================================
;; Connection
;; =============================================================================

(defn- send-text! [conn ^String text]
  (let [^WebSocket ws (:ws @(:state conn))]
    (when (nil? ws) (throw (ex-info "not connected" {:type :not-connected})))
    (locking (:lock conn)
      (.get ^CompletableFuture (.sendText ws text true) 30 TimeUnit/SECONDS))))

(defn- dispatch! [conn frame]
  (let [{:keys [command headers body]} frame
        {:keys [subs on-error]} @(:state conn)]
    (case command
      "MESSAGE"
      (let [sub-id (get headers "subscription")
            ack-id (or (get headers "ack") (get headers "message-id"))
            handler (get-in subs [sub-id :handler])]
        (if handler
          (try
            (handler {:body body :headers headers
                      :ack!  (fn [] (send-text! conn (encode-frame "ACK" {"id" ack-id})))
                      :nack! (fn [] (send-text! conn (encode-frame "NACK" {"id" ack-id "requeue" "false"})))})
            (catch Throwable t (log/error t "STOMP message handler threw")))
          (log/warn "MESSAGE for unknown subscription" sub-id)))
      "ERROR"
      (let [msg (str "STOMP error: " (get headers "message") " " body)]
        (log/error msg)
        (when on-error (on-error msg)))
      "RECEIPT" nil
      (log/debug "unhandled STOMP frame" command))))

(defn- listener [conn]
  (let [buf (StringBuilder.)]
    (reify WebSocket$Listener
      (onOpen [_ ws] (.request ws 1))
      (onText [_ ws data last?]
        (.append buf ^CharSequence data)
        (when last?
          (let [text (str buf)]
            (.setLength buf 0)
            (doseq [part (remove str/blank? (str/split text #"\u0000"))]
              (when-let [frame (decode-frame part)]
                (if (contains? #{"CONNECTED" "ERROR"} (:command frame))
                  (let [^CompletableFuture p (:connected-promise @(:state conn))]
                    (if (and p (not (.isDone p)))
                      (.complete p frame)
                      (dispatch! conn frame)))
                  (dispatch! conn frame))))))
        (.request ws 1)
        nil)
      (onBinary [this ws data last?]
        (let [^ByteBuffer data data
              bytes (byte-array (.remaining data))]
          (.get data bytes)
          (.onText ^WebSocket$Listener this ws (String. bytes StandardCharsets/UTF_8) last?)))
      (onPing [_ ws msg] (.sendPong ws msg) (.request ws 1) nil)
      (onPong [_ ws _] (.request ws 1) nil)
      (onClose [_ ws code reason]
        ;; Only the *current* socket may report a drop: a superseded socket
        ;; (after reconnect!) closes late and must not trigger another one.
        (let [{:keys [closing? on-closed] current :ws} @(:state conn)]
          (when (identical? ws current)
            (swap! (:state conn) assoc :connected? false)
            (when (and on-closed (not closing?)) (on-closed (str "closed " code " " reason)))))
        nil)
      (onError [_ ws t]
        (let [{:keys [closing? on-closed connected-promise] current :ws} @(:state conn)]
          (when-let [^CompletableFuture p connected-promise]
            (when-not (.isDone p) (.completeExceptionally p t)))
          (when (identical? ws current)
            (swap! (:state conn) assoc :connected? false)
            (when (and on-closed (not closing?)) (on-closed (str "error " (.getMessage ^Throwable t))))))
        nil))))

(defn connected? [conn] (boolean (:connected? @(:state conn))))

(defn- open-socket! [conn]
  (let [{:keys [url vhost token heartbeat-ms connect-timeout]} @(:state conn)
        promise (CompletableFuture.)
        _       (swap! (:state conn) assoc :connected-promise promise :closing? false)
        client  (-> (HttpClient/newBuilder) (.connectTimeout (Duration/ofSeconds connect-timeout)) .build)
        ws      (-> (.newWebSocketBuilder client)
                    (.subprotocols "v12.stomp" (into-array String ["v11.stomp"]))
                    (.connectTimeout (Duration/ofSeconds connect-timeout))
                    (.buildAsync (URI/create url) (listener conn))
                    (.get connect-timeout TimeUnit/SECONDS))]
    (swap! (:state conn) assoc :ws ws)
    (send-text! conn (encode-frame "CONNECT" {"accept-version" "1.2,1.1"
                                              "host" vhost
                                              "login" ""
                                              "passcode" token
                                              "heart-beat" (str heartbeat-ms "," heartbeat-ms)}))
    (let [frame (try (.get promise connect-timeout TimeUnit/SECONDS)
                     (catch TimeoutException _ (throw (ex-info "timed out waiting for CONNECTED" {:type :timeout}))))]
      (when (= "ERROR" (:command frame))
        (throw (ex-info (str "STOMP error: " (get-in frame [:headers "message"]) " " (:body frame))
                        {:type :stomp-error :frame frame})))
      (swap! (:state conn) assoc :connected? true)
      ;; heart-beats: one newline every heartbeat-ms
      (let [^ScheduledExecutorService exec (:exec @(:state conn))]
        (swap! (:state conn) assoc :hb-task
               (.scheduleAtFixedRate exec
                                     (fn [] (try (when (connected? conn) (send-text! conn "\n"))
                                                 (catch Throwable _ nil)))
                                     heartbeat-ms heartbeat-ms TimeUnit/MILLISECONDS)))
      (log/info "STOMP connected to" url "(vhost" vhost ")")
      conn)))

(defn- resubscribe-all! [conn]
  (doseq [[sub-id {:keys [queue]}] (:subs @(:state conn))]
    (send-text! conn (encode-frame "SUBSCRIBE" {"id" sub-id
                                                "destination" (str "/amq/queue/" queue)
                                                "ack" "client-individual"
                                                "prefetch-count" "1"}))))

(defn connect
  "Open a connection. opts: :url (ws:// or wss://), :vhost, :token,
   optional :heartbeat-ms (10000), :connect-timeout seconds (20),
   :on-closed (fn [reason]) called once when the socket drops unexpectedly,
   :on-error (fn [message]) for ERROR frames."
  [{:keys [url vhost token heartbeat-ms connect-timeout on-closed on-error]}]
  (let [conn {:state (atom {:url url :vhost vhost :token token
                            :heartbeat-ms (or heartbeat-ms 10000)
                            :connect-timeout (or connect-timeout 20)
                            :on-closed on-closed :on-error on-error
                            :subs {} :sub-seq 0 :connected? false :closing? false
                            :exec (Executors/newSingleThreadScheduledExecutor)})
              :lock (Object.)}]
    (open-socket! conn)))

(defn subscribe!
  "Consume `queue` (prefetch 1, client-individual acks). `handler` receives
   {:body string :headers map :ack! (fn []) :nack! (fn [])} and must call
   exactly one of ack!/nack!. Returns the subscription id."
  [conn queue handler]
  (let [sub-id (str "sub-" (:sub-seq (swap! (:state conn) update :sub-seq inc)))]
    (swap! (:state conn) assoc-in [:subs sub-id] {:queue queue :handler handler})
    (send-text! conn (encode-frame "SUBSCRIBE" {"id" sub-id
                                                "destination" (str "/amq/queue/" queue)
                                                "ack" "client-individual"
                                                "prefetch-count" "1"}))
    sub-id))

(defn publish!
  "SEND `body` (a String) to /exchange/<exchange>/<routing-key>. opts:
   :user-id, :content-type (application/json), :persistent? (true)."
  [conn exchange routing-key ^String body & [{:keys [user-id content-type persistent?]
                                               :or {content-type "application/json" persistent? true}}]]
  (send-text! conn (encode-frame "SEND" (cond-> {"destination" (str "/exchange/" exchange "/" routing-key)
                                                 "content-type" content-type}
                                          persistent? (assoc "persistent" "true")
                                          user-id (assoc "user-id" user-id))
                                 body)))

(defn close!
  "Close cleanly. Idempotent."
  [conn]
  (let [{:keys [^WebSocket ws hb-task]} @(:state conn)]
    (swap! (:state conn) assoc :closing? true :connected? false)
    (when hb-task (.cancel ^java.util.concurrent.ScheduledFuture hb-task false))
    (when ws
      (try (send-text! conn (encode-frame "DISCONNECT")) (catch Throwable _ nil))
      (swap! (:state conn) assoc :ws nil)
      (try (.get ^CompletableFuture (.sendClose ws WebSocket/NORMAL_CLOSURE "bye") 5 TimeUnit/SECONDS)
           (catch Throwable _ (try (.abort ws) (catch Throwable _ nil)))))
    conn))

(defn reconnect!
  "Close and connect again, with `token` if given, re-subscribing every
   active subscription."
  [conn & [token]]
  (close! conn)
  (when token (swap! (:state conn) assoc :token token))
  (open-socket! conn)
  (resubscribe-all! conn)
  (log/info "STOMP reconnected" (if token "with a refreshed token" ""))
  conn)

(defn shutdown!
  "close! plus stop the internal executor. The connection is dead afterwards."
  [conn]
  (close! conn)
  (.shutdownNow ^ScheduledExecutorService (:exec @(:state conn)))
  nil)

;; =============================================================================
;; The worker loop
;; =============================================================================

(defn run-with-refresh!
  "Keep `conn` alive until `stop-promise` is delivered: every `check-ms`
   (30000) call `(needs-refresh?)`, and when true `(refresh!)` → new token,
   then reconnect with it; when the socket drops (via :on-closed, which the
   caller must wire to `(deliver-drop! reason)` returned from this fn... see
   below) reconnect with exponential backoff (2 s .. 60 s).

   Usage:
     (let [drops (LinkedBlockingQueue.)
           conn  (stomp/connect {... :on-closed #(.offer drops %)})]
       (stomp/subscribe! conn queue handler)
       (stomp/run-with-refresh! conn {:drops drops :needs-refresh? f :refresh! g
                                      :after-reconnect! (fn [] (update-status! \"online\"))}))
   Blocks the calling thread."
  [conn {:keys [^LinkedBlockingQueue drops needs-refresh? refresh! after-reconnect! check-ms stop-promise]
         :or {check-ms 30000}}]
  (let [stop-promise (or stop-promise (promise))
        backoff (atom 2000)]
    (loop []
      (when-not (realized? stop-promise)
        (let [reason (.poll drops check-ms TimeUnit/MILLISECONDS)]
          (cond
            reason
            (do (log/warn "broker connection dropped:" reason "— reconnecting in" @backoff "ms")
                (Thread/sleep ^long @backoff)
                (swap! backoff #(min 60000 (* 2 %)))
                (try
                  (let [token (when (and needs-refresh? (needs-refresh?)) (refresh!))]
                    (reconnect! conn token)
                    (reset! backoff 2000)
                    (when after-reconnect! (after-reconnect!)))
                  (catch Throwable t
                    (log/error t "reconnect failed")
                    (.offer drops (str "reconnect failed: " (.getMessage t))))))

            (and needs-refresh? (needs-refresh?))
            (try
              (let [token (refresh!)]
                (reconnect! conn token)
                (when after-reconnect! (after-reconnect!)))
              (catch Throwable t
                (log/error t "token refresh / reconnect failed")
                (.offer drops (str "refresh failed: " (.getMessage t))))))
          (recur))))))
