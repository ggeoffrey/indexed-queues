(ns indexed-queue.in-memory
  (:require [indexed-queue.atom :as a]
            [indexed-queue.config :as config]
            [indexed-queue.core :as core]
            [indexed-queue.cache :as cache]
            [manifold.stream :as s]))

;;;;;;;;;;;;;;;;;;;
;; INDEXED QUEUE ;;
;;;;;;;;;;;;;;;;;;;

(defrecord IndexedTopic [queue index]
  core/IQueue
  (publish! [this m]
    (core/publish! queue m))
  (consume! [this f]
    (core/consume! queue f))
  (consume! [this key f]
    (core/consume! queue key f))

  core/IMap
  ;; (put! [this k v] (publish! this v))
  (get! [this k]
    (core/get! index k))
  (get! [this k default]
    (core/get! index k default))
  (delete! [this k]
    (core/delete! index k)
    this)

  core/IObservable
  (subscribe! [this k f] (core/subscribe! index k f))
  (unsubscribe! [this k]
    (core/unsubscribe! index k)
    (core/unsubscribe! queue k)
    this))

(defn index-events [index key e]
  {:pre [(some? (key e))]}
  (core/put! index (key e) e))

(defn indexed-topic [params]
  (let [q     (s/stream)
        index (if-let [ttl (config/ttl params)]
                (cache/observable-cache {:ttl ttl})
                (a/observable-atom {}))]
    (core/consume! q (partial index-events index (config/keyfn params)))
    (map->IndexedTopic {:queue q
                        :index index})))

;;;;;;;;;;;;;
;; COMMENT ;;
;;;;;;;;;;;;;

(comment
  (let [q (indexed-topic {:index {:key :id
                                  :ttl 2000}})]
    (tap> "----")
    (core/consume! q (fn [m] (tap> [:message m])))
    (core/subscribe! q :test tap>)
    (core/publish! q {:id 1})
    (Thread/sleep 1000)
    (core/publish! q {:id 2})
    (Thread/sleep 1000)
    (core/delete! q 1)
    (Thread/sleep 1000)
    (tap> {2 (core/get! q 2)})
    ;; (core/unsubscribe! q :test)
    ))
