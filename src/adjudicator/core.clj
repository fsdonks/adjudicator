(ns adjudicator.core
  (:gen-class)
  (:import [javax.swing JFrame JFileChooser])
  (:require [clojure.java [io :as io]]
            [clojure [string :as str]]))

;; Header for Reconciliation file. Excel does not correctly write header to a single line
;; when exporting as tab delimited text file.
;; *the first 14 lines of a tab delimited reconciliation file are header information
(def header
  (map keyword  
       ["branch"
        "oos_src" "oos_name" "oos_str" "reason" ;; oos -- out of scope 
        "rel_src" "rel_name" "relation" "rel_q" "rel_str" "rel_inf" ;; rel -- related
        "fd_src" "fd_name" "fd_str" ;; fd -- 5 digit
        "ks_src" "ks_name" "ks_str" ;; ks -- K-surrogat
        "delete" ;; Yes, No, or empty
        "rrep_src" "rep_qc1" "rep_qc2" "rep_qc3" ;; rrep -- related replace
        "crep_src" "crep_qc1" "crep_qc2" "crep_qc3"])) ;; crep - custom replace

;; Index is a map where keys: fields from header  vals: index number of field in record
;; ie [:branch 0] is the first key value pair 
(def index (zipmap header (range (count header))))

;; Reads all lines from a file into a coll (not lazy) 
(defn read-lines [filename]
  (with-open [r (io/reader filename)]
    (doall (line-seq r)))) ;; forces all lines to be read before closing

;; Given line from reconciliation file, builda a map with all the fields in header
(defn line->map [line]
  (let [v (str/split line #"\t")]
    (zipmap (keys index) (map #(nth (concat v (apply concat
       ;; adds empty values for empty fields in the record                                 
       (for [x (range (- (count index) (count v)))]
            [""]))) (get index %)) (keys index)))))

;; returns a set of scrs to remove from the demand record
(defn delete-srcs [ms]
  (into #{} (filter #(= "Y" (str (first (:delete %)))) ms))) ;; only delete out of scope src
  ;(reduce into ;; also delete related srcs, change to previous line if not the intent
  ;        (for [m ms :let [d (str (first (:delete m)))] :when (= "Y" d)]
  ;          (into #{} (map #(get m %) [:oos_src :rel_src :fd_src :ks_src])))))

;; returns a map of srcs to replace
;; where the keys are the src to be replaced
;; and the vals are the replacement srcs
;(defn replace-srcs [ms]
;  (apply conj
;         (for [m ms :let [d (str (first (:delete m)))] :when (not= "Y" d)]
;           {(:oos_src m) (first (filter #(not= "" %) [(:rrep_src m) (:crep_src m)]))})))

(defn replace-srcs [ms]
  (into #{}
   (for [m ms :let [d (str (first (:delete m)))] :when (not= "Y" d)] (:oos_src m))))
  
  
;; Given map of record, returns the src that sould replace the current oos src
(defn rep-src [m]
  (first (filter #(not= "" %) (map #(get m %) [:rrep_src :crep_src]))))

;; Given map of record, returns the src title that should replace the current oos src title
(defn rep-name [m]
  (first (filter #(not= "" %) (map #(get m %) [:rel_name :fd_name :ks_name]))))

;; Returns the replacemnet str for the oos src; NEVER USED 
;(defn rep-str [m]
;  (first (filter #(not= "" %) (map #(get m %) [:rel_str :fd_str :ks_str]))))

;; Returns the replacement quantity for the oos src; NEVER USED
;(defn rep-q [m]
;  (reduce + (map read-string (filter #(not= "" %) (map #(get m %)
;                 [:rep_qc1 :rep_qc2 :rep_qc3 :crep_qc1 :crep_qc2 :crep_qc3])))))


;; Breaks the line into vector by splitting with "\t" delimiter
(defn tsv->line [line] (str/split line #"\t"))

;; Given the filepath for the reconciliation file, demand file, and output file
;; Build the list of srcs to delete from the demand file using the change file
;; If the record is not on the delete list, it is written to the outfile
;; Otherwise it is ignored and never written to the new file
(defn delete-demand [changefile demandfile outfile]
  (with-open [r (io/reader demandfile) w (io/writer outfile)] ;; lazily reads demand file
    (let [lines (line-seq r) h (tsv->line (first lines))
          i (zipmap (map keyword h) (range (count h)))
          dlist (into #{} (map #(:oos_src %)
                               (delete-srcs (map line->map (drop 14 (read-lines changefile))))))]
      ;;                                    first 14 files are header info
      (doseq [d (map tsv->line lines)]  ;:when #(not (contains? dlist (nth % (:src i))))]
        (if (not (contains? dlist (nth d (:SRC i))))
          (doseq [v d] (.write w (str v "\t"))))
        (when (not (contains? dlist (nth d (:SRC i))))
          (.write w "\r\n"))))))

;; Returns a record with the new src value
(defn replace-src [rec i newsrc]
  (assoc rec (get i "SRC") newsrc)) 

(defn src->m [ms src]
  (first (filter #(= src (:oos_src %)) ms)))

;; For each src in the change file to be replaced, replaces the oos src with the supplied replacement
(defn replace-supply [changefile supplyfile outfile]
  (with-open [r (io/reader supplyfile) w (io/writer outfile)]
    (let [lines (line-seq r) h (tsv->line (first lines))
          i (zipmap h (range (count h)))
          ms (map line->map (drop 14 (read-lines changefile)))
          rsrcs (replace-srcs ms)]
      (doseq [d (map tsv->line lines)
              :let [rec (if (contains? rsrcs (nth d (get i "SRC")))
                          (replace-src d i (rep-src (src->m ms (nth d (get i "SRC")))))
                          d)]]
        (doseq [v rec] (.write w (str v "\t")))
        (.write w "\r\n")))))


;; Deletes srcs from demand file and replaces srcs in supply file
;; according to the information given in the change file
;; Changes to both files are done in parallel 
;; Returns nil
(defn reconciliation [changefile sfile sout dfile dout]
  (doall 
   (pmap #(%) [#(replace-supply changefile sfile sout) #(delete-demand changefile dfile dout)]))
  nil)

;; Opens a file select menu and returns selected file names
(defn choose-file [title & dir-only]
  (let [f (javax.swing.JFrame.)
        c (javax.swing.JFileChooser.)]
    (.add f c)
    (.setDialogTitle c title)
    (when dir-only (.setFileSelectionMode c JFileChooser/DIRECTORIES_ONLY))
    (.setMultiSelectionEnabled c true)
    (let [x (.showOpenDialog c f)]
      (if (zero? x)
        (map #(.getPath ^java.io.File %) (.getSelectedFiles c))
        (println "No file selected.")))))

;; Calls reconciliation from using files selected from File Select Menu
(defn ->reconciliation []
  (let [changefile (first (choose-file "Reconciliation file"))
        demandfile (first (choose-file "Demand File"))
        supplyfile (first (choose-file "Supply File"))
        output (first (choose-file "Output directory" true))
        demandout (str output "/formatted-demand.txt")
        supplyout (str output "/formatted-supply.txt")]
    (reconciliation changefile supplyfile supplyout demandfile demandout)
    (println "Done.")))

(defn -main [& args]
  (->reconciliation)
  (java.lang.System/exit 0))
