(ns indexed-queue.core
  (:require [manifold.bus :as b]
            [manifold.stream :as s])
  (:import manifold.bus.IEventBus
           manifold.stream.default.Stream))

(defprotocol IMap
  (put! [this k v])
  (get! [this k] [this k default])
  (delete! [this k]))

(defprotocol ICache
  (get-or-else! [this k default]))

(defprotocol IQueue
  (publish! [this m])
  (consume! [this f] [this key f]))

(defprotocol IObservable
  (subscribe! [this key f])
  (unsubscribe! [this key]))

(extend-protocol IMap
  clojure.lang.PersistentArrayMap
  (put! [m k v] (assoc m k v))
  (get!
    ([m k] (get m k))
    ([m k default] (get m k default)))
  (delete! [m k] (dissoc m k))

  clojure.lang.PersistentHashMap
  (put! [m k v] (assoc m k v))
  (get!
    ([m k] (get m k))
    ([m k default] (get m k default)))
  (delete! [m k] (dissoc m k))

  clojure.lang.Atom
  (put! [m k v]
    (swap! m put! k v)
    m)
  (get!
    ([m k] (get! @m k))
    ([m k default] (get! @m k default)))
  (delete! [m k]
    (swap! m delete! k)
    m))

(extend-protocol ICache
  clojure.lang.Atom
  (get-or-else! [this k default]
    (or (get! this k)
        (do
          (put! this k default)
          (get! this k)))))

(extend-protocol IQueue
  manifold.stream.default.Stream
  (publish! [this m]
    (s/put! this m))
  (consume!
    ([this f]
     (s/consume f this))
    ([this _ f]
     (s/consume f this)))

  manifold.bus.IEventBus
  (publish!
    ([this m]
     (b/publish! this "default" m))
    ([this topic m]
     (b/publish! this topic m)))
  (consume!
    ([this f]
     (consume! this "default" f))
    ([this topic f]
     (let [stream (b/subscribe this topic)]
       (consume! stream f)
       stream)))

  manifold.stream.SourceProxy
  (consume!
    ([this f]
     (s/consume f this))
    ([this _ f]
     (s/consume f this))))
