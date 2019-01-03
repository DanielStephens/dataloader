(ns dataloader.core
  (:require [clojure.core :refer :all]))

(defn reset-vals!
  "this suffers from the ABA problem however until clojure 10 not sure what the alternative is,
  perhaps using ref instead of atoms?"
  [atm new-value]
  (let [old-value @atm]
    (if (compare-and-set! atm old-value new-value)
      (do
        (.notifyWatches atm old-value new-value)
        [old-value new-value])
      (recur atm new-value))))

(defprotocol DataLoader
  (load-data [this key] "returns a promise for the data that will be loaded for the single key `key`.")
  (load-many [this keys] "returns a promise for the data that will be loaded for the vector of keys `keys`.")
  (clear [this key] "clears the cached result for the key `key` if one exists, otherwise does nothing.")
  (clear-all [this] "clears all cached results.")
  (prime [this key value] "caches a specific value `value` with the key `key`.")
  (execute [this] "runs the underlying batch function to load data and resolves any unresolved promises."))

(deftype BasicDataLoader
  [batch-load-fn resolve-fn reject-fn queue should-batch cache should-cache cache-key-fn]
  DataLoader
  (load-data [this key]
    (when
      (nil? key)
      (throw (ex-info "The load function must be called with a value"
                      {:input key})))
    (let [value (some-> cache deref (get key))]
      (if (and should-cache value)
        value
        (let [p (promise)]
          (when should-cache (swap! cache assoc (cache-key-fn key) p))
          (swap! queue conj [key p])
          (when-not should-batch (execute this))
          p))))

  (load-many [this keys]
    (when-not
      (vector? keys)
      (throw (ex-info "The load-many function must be called with a vector of keys."
                      {:input keys})))
    (let [promises (map (partial load-data this) keys)]
      (future
        (doall (map deref promises)))))

  (clear [_ key]
    (swap! cache dissoc (cache-key-fn key)))

  (clear-all [_]
    (reset! cache {}))

  (prime [_ key value]
    (when-not should-cache
      (throw (ex-info "The prime function only makes sense when caching is in use"
                      {:should-cache false})))
    (let [k (cache-key-fn key)
          v (-> cache deref (get k))]
      (when (nil? v)
        (let [p (future value)]
          (swap! cache assoc k p)
          (if (instance? Exception value)
            (reject-fn value)
            (resolve-fn value))))))

  (execute [this]
    (let [q (first (reset-vals! queue []))
          keys (apply vector (map first q))
          promises (apply vector (map second q))
          batch-ref (if (not-empty keys) (batch-load-fn keys) (future []))
          _ (when-not
              (instance? clojure.lang.IDeref batch-ref)
              (throw (ex-info (str "data-loader must be constructed with a function which accepts "
                                   "a vector of keys and returns a IDeref of a vector of values, "
                                   "but the function did not return a IDeref.")
                              {:function batch-load-fn :keys keys :output batch-ref})))
          batch @batch-ref
          _ (when-not
              (vector? batch)
              (do
                (println batch)
                (throw (ex-info (str "data-loader must be constructed with a function which accepts "
                                    "a vector of keys and returns a IDeref of a vector of values, "
                                    "but the function returned an IDeref which did not contain a vector of values.")
                               {:function batch-load-fn :keys keys :batch batch}))))
          _ (when-not
              (= (count keys) (count batch))
              (throw (ex-info (str "data-loader must be constructed with a function which accepts "
                                   "a vector of keys and returns a IDeref of a vector of values, "
                                   "but the function returned an IDeref with a vector of values of the wrong size.")
                              {:function batch-load-fn :keys keys :batch batch})))]
      (doseq [n (range (count keys))
            :let [value (nth batch n)
                  promise (nth promises n)]]
        (do
          (deliver promise value)
          (if (instance? Exception value)
            (do
              (clear this (nth keys n))
              (reject-fn value))
            (resolve-fn value))))
      promises)))

(defn data-loader
  ([batch-load-function {:keys [resolve-fn
                                reject-fn
                                should-batch
                                should-cache
                                cache-key-fn]
                         :or {resolve-fn (fn [_] nil)
                              reject-fn (fn [_] nil)
                              should-batch false
                              should-cache false
                              cache-key-fn identity}}]
   (->BasicDataLoader batch-load-function
                      resolve-fn
                      reject-fn
                      (atom [])
                      should-batch
                      (atom {})
                      should-cache
                      cache-key-fn))
  ([batch-load-function] (data-loader batch-load-function {})))

(def a (atom {:hits 0 :A 1 :B 2}))
(defn batch-load-fn [keys]
  (let [aa @a]
    (swap! a (fn [{hits :hits :as m}] (assoc m :hits (inc hits))))
    (future (apply vector (map (fn [k] (let [v (get aa k)]
                                         (cond
                                           (= :invalid v) (Exception. (str "Other issue [" k "]"))
                                           (nil? v) (Exception. (str "No such element [" k "]"))
                                           :else v))) keys)))))
(def dl (data-loader batch-load-fn {:should-batch true  :should-cache true}))