(ns indexed-queue.cache
  (:require [clojure.core.cache :as cache]
            [clojure.core.cache.wrapped :as c]
            [indexed-queue.core :as core]
            [indexed-queue.atom :as a]))

(defrecord ObservableCache [cache]
  clojure.lang.IDeref
  (deref [this]
    (deref cache))

  core/IMap
  (put! [this k v]
    (c/lookup-or-miss cache k (constantly v))
    this)
  (get! [this k] (c/lookup cache k))
  (get! [this k default] (c/lookup cache k default))
  (delete! [this k]
    (c/evict cache k)
    this)

  core/IObservable
  (subscribe! [this key f]
    (core/subscribe! cache key f)
    this)
  (unsubscribe! [this key]
    (core/unsubscribe! cache key)
    this))

(defn observable-cache [{:keys [ttl]
                         :or   {ttl 2000}}]
  (a/observable-atom (map->ObservableCache {:cache (c/ttl-cache-factory {} :ttl ttl)})))
