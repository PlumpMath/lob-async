(ns lob.async.bridge
  (:require [lob.link :as link]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as asyncp]
            [com.stuartsierra.component :as component]))

(comment
  (def sub-in (link/subscribe! task-in n-cpu
                               (fn [m]
                                 (clojure.pprint/pprint m)
                                 (msg/ack! m)))))

(defn close! [chan]
  (when chan
    (async/close! chan)
    nil))

(defn unsubscribe! [sub]
  (when sub
    (link/unsubscribe! sub)
    nil))

(defn in? [direction]
  (#{:in :both} direction))

(defn out? [direction]
  (#{:out :both} direction))

(defrecord Bridge [publication size chan-in chan-out sub direction]
  asyncp/ReadPort
  (take! [port fn1-handler]
    (asyncp/take! chan-in fn1-handler))
  asyncp/WritePort
  (put! [port val fn1-handler]
    (asyncp/put! chan-out val fn1-handler))
  component/Lifecycle
  (start [this]
    (if chan-in
      this
      (let [chan-in (when (in? direction) (async/chan size))
            chan-out (when (out? direction) (async/chan size))
            sub (when chan-in
                  (link/subscribe! publication size
                                   (fn [m]
                                     (async/put! chan-in m))))]
        (when chan-out
          (async/go-loop []
            (when-let [msg (async/<! chan-out)]
              (try
                (link/send! publication msg)
                (catch Exception e
                  (println e)))
              (recur))))
        (-> this
            (assoc :chan-in chan-in)
            (assoc :chan-out chan-out)
            (assoc :sub sub)))))
  (stop [this]
    (-> this
        (update :sub unsubscribe!)
        (update :chan-in close!)
        (update :chan-out close!))))


(defn bridge [size direction]
  (map->Bridge {:size size
                :direction direction}))
