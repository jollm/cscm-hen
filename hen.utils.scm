;; Copyright (C) 2011 by Joseph Gay
;;
;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation; either version 3 of the License, or
;; (at your option) any later version.
;;
;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with this program; if not, write to the Free Software
;; Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
;;
;; Author: Joseph Gay, <gilleylen [at] gmail [dot] com>

;; hen.utils.scm

;; Helper procedures.

(define (->string+ #!rest args)
  (fold-right string-append "" (map ->string args)))

(define (string-trim-last string)
  (string-trim-right string char-set:whitespace (sub1 (string-length string))))

(define (second-match regex s)
  (if* (string-match regex s) (if (not (null? (cdr it))) (second it) #f)
       #f))

;; this parses beanstalk's yaml, nothing generic intended
(define (parse-yaml-alist yaml)
  (plist->alist (flatten (cdr (filter-map (lambda (s) (string-split s ": ")) (string-split yaml "\n"))))))

(define (parse-yaml-list yaml)
  (cdr (map (cut string-drop <> 2) (string-split yaml "\n"))))
