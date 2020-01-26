(ns indexed-queue.distributed
  (:require [chazel.core :as hz]
            [indexed-queue.atom :as a]
            [indexed-queue.config :as config]
            [indexed-queue.core :as core]
            [indexed-queue.in-memory :as m])
  (:import com.hazelcast.topic.impl.reliable.ReliableTopicProxy))

(defrecord DistributedQueue [queue listeners]
  core/IQueue
  (publish! [this m]
    (core/publish! queue m))
  (consume!
    [this key f]
    (core/subscribe! this key f))

  core/IObservable
  (subscribe! [this key f]
    (let [listener (hz/add-message-listener queue f)]
      (when key
        (swap! listeners assoc key listener))
      listener))
  (unsubscribe! [this key]
    (when-let [id (get @listeners key)]
      (hz/remove-message-listener queue id)
      (swap! listeners dissoc key))))

(defn distributed-queue [name _]
  (map->DistributedQueue {:queue     (hz/hz-reliable-topic name)
                          :listeners (atom {})}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DISTRIBUTED INDEXED QUEUE ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol core/IQueue
  com.hazelcast.topic.impl.reliable.ReliableTopicProxy
  (publish! [this m] (hz/publish this m))
  (consume! [this f] (hz/add-message-listener this f)))

(defn distributed-indexed-topic [name params]
  (let [q     (distributed-queue name nil)
        index (a/observable-atom {})]
    (core/subscribe! q :indexer (partial m/index-events index (config/keyfn params)))
    (m/map->IndexedTopic {:queue q
                          :index index})))

(comment
  (add-tap prn)
  (tap> "----")
  (let [q (distributed-indexed-topic "_" {:index {:key :id}})]
    (core/consume! q :consumer (fn [m] (tap> [:message m])))
    (core/subscribe! q :watcher tap>)
    (core/publish! q {:id 1})
    (Thread/sleep 1000)
    (core/publish! q {:id 1, :updated? true})
    (Thread/sleep 1000)
    (core/delete! q 1)
    (core/unsubscribe! q :consumer)
    (core/unsubscribe! q :watcher)))

;; (.destroy (hz/hz-reliable-topic "_"))
