(ns indexed-queue.config
  (:require [clojure.spec.alpha :as s]))

(s/def ::ttl nat-int?)
(s/def ::key fn?)

(s/def ::distributed? boolean?)

(s/def ::index (s/keys :req-un [::key]
                       :opt-un [::ttl]))

(s/def ::queue (s/keys :opt-un [::distributed?]))

(s/def ::name string?)
(s/def ::params (s/keys :opt-un [::queue ::index]))
(s/def ::message map?)

(defn index [params] (:index params))
(defn keyfn [params]
  (:key (index params)))

(defn ttl [params]
  (:ttl (index params)))
