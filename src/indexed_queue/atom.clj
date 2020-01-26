(ns indexed-queue.atom
  (:require [indexed-queue.core :as core]
            [manifold.bus :as b]
            [manifold.stream :as s]))

(defn- atom-watcher [_ _ _ val f]
  (f val))

(defrecord ObservableAtom [state queue listeners]
  clojure.lang.IDeref
  (deref [this]
    (deref state))

  core/IMap
  (put! [this k v]
    (let [action (if (core/get! this k) :update :create)]
      (core/put! state k v)
      (core/publish! queue [action (core/get! state k)]))
    this)
  (get! [this k] (core/get! state k))
  (get! [this k default] (core/get! state k default))
  (delete! [this k]
    (let [value (core/get! state k)]
      (core/delete! state k)
      (when value
        (core/publish! queue [:delete value])))
    this)

  core/ICache
  (get-or-else! [this k default]
    (or (core/get! this k)
        (do
          (core/put! this k default)
          (core/get! this k))))

  core/IObservable
  (subscribe! [this key f]
    (core/put! listeners key (core/consume! queue f))
    this)
  (unsubscribe! [this key]
    (when-let [listener (core/get! listeners key)]
      (s/close! listener)
      (core/delete! listeners key))
    this))

(defn observable-atom [init]
  (map->ObservableAtom {:state     (atom init)
                        :queue     (b/event-bus)
                        :listeners (atom {})}))

(ns-unmap *ns* 'map->ObservableAtom)

(defmethod print-method ObservableAtom [v ^java.io.Writer w]
  (.write w (str "<<ObservableAtom " (pr-str (:state v)) ">>")))
