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

;; A beanstalk client.
;; beanstalkd is http://kr.github.com/beanstalkd/

(module hen

(with-hen
 hen-put
 hen-reserve
 hen-use
 hen-delete
 hen-release
 hen-bury
 hen-touch
 hen-watch
 hen-ignore
 hen-peek
 hen-peek-ready
 hen-peek-delayed
 hen-peek-buried
 hen-kick
 hen-stats-job
 hen-stats-tube
 hen-stats
 hen-list-tubes
 hen-list-tube-used
 hen-list-tubes-watched
 hen-quit
 hen-pause-tube
 hen-in
 hen-out)

(import scheme chicken)

(use chicken data-structures extras lambda+ list-utils miscmacros regex srfi-1 srfi-13 srfi-14 tcp6)

(import-for-syntax chicken)

(include "hen.utils")

(define hen-in)
(define hen-out)

(define priority-default 2147483648)

(define-syntax with-hen
  (syntax-rules ()
    [(with-beanstalk tcp-connection body1 body2 ...)
     (let-values ([(i o) tcp-connection])
       (fluid-let ([hen-in i] [hen-out o])
         (let ([res (begin body1 body2 ...)])
           (hen-quit) res)))]))

(define+ (hen-put #!rest args #!key (tcp-in hen-in) (tcp-out hen-out))
  (apply (case-lambda
           ([data] (hen-put priority-default 0 36000 data #:tcp-in tcp-in #:tcp-out tcp-out))
           ([pri data] (hen-put pri 0 36000 data #:tcp-in tcp-in #:tcp-out tcp-out))
           ([pri delay ttr data]
            (write-line (string-append (->string+ "put " pri " " delay " " ttr " " (string-length data)) "\r")
                        tcp-out)
            (write-line (string-append data "\r") tcp-out)
            (read-line tcp-in))) args))

(define (read-job-data res tcp-in)
  (if* (string-match "^(?:RESERVED|FOUND) (\\d+) (\\d+)$" res)
       `((id . ,(second it))
         (data . ,(string-trim-last (read-string (+ 2 (string->number (third it))) tcp-in))))
       res))

(define (hen-reserve #!optional (timeout #f) #!key (tcp-in hen-in) (tcp-out hen-out))
  (fluid-let ([tcp-write-timeout (and timeout (* 1000 timeout))])
    (write-line (string-append (if timeout (->string+ "reserve-with-timeout " timeout)
                                   "reserve") "\r")
                tcp-out))
  (read-job-data (read-line tcp-in) tcp-in))

(define-syntax define-hen-command
  (lambda (x r c)
    (let-values ([(%write-line %define %read-line %map %->string %list
                   %string-append %string-intersperse %cons %symbol->string %let %if %begin)
                  (apply values (map r '(write-line define read-line map ->string list
                                         string-append string-intersperse cons symbol->string let if begin)))])
      (let* ([name (cadr x)]
             [required (take-while (lambda (e) (not (equal? '#!optional e)))  (caddr x))]
             [optional (drop-while (lambda (e) (not (equal? '#!optional e)))  (caddr x))]
             [all-args (append required (if (null? optional) '() (map car (cdr optional))))]
             [has-read-extra? (not (null? (cdddr x)))]
             [read-extra (if has-read-extra? (cdddr x) '())])
        `(,%define (,(symbol-append 'hen- name)
                    ,@required ,@optional
                    #!key (tcp-in hen-in) (tcp-out hen-out))
                   (,%write-line (,%string-append (,%string-intersperse
                                                   (,%cons (,%symbol->string ',name)
                                                           (,%map ,%->string (,%list ,@all-args)))) "\r") tcp-out)
                   (,%let ([res (,%read-line tcp-in)])
                     (,%if ,has-read-extra?
                           (,%begin ,@read-extra)
                           res)))))))

(define-syntax define-hen-command-list
  (syntax-rules ()
    [(define-hen-list
       (command-1 (arg-1 ...) ...)
       (command-2 (arg-2 ...) ...) ...)
     (begin (define-hen-command command-1 (arg-1 ...) ...)
            (define-hen-command command-2  (arg-2 ...) ...)
      ...)]))

(define (read-stats res tcp-in #!optional (parser parse-yaml-alist))
  (if* (second-match "^OK (\\d+)$" res) (parser (read-string (string->number it) tcp-in))))

(define-hen-command-list
  [use (tube)]
  [delete (id)]
  [release (id #!optional (pri priority-default) (delay 0))]
  [bury (id #!optional (pri priority-default))]
  [touch (id)]
  [watch (tube)]
  [ignore (tube)]
  [peek (id) (read-job-data res tcp-in)]
  [peek-ready () (read-job-data res tcp-in)]
  [peek-delayed () (read-job-data res tcp-in)]
  [peek-buried () (read-job-data res tcp-in)]
  [kick (bound)]
  [stats-job (id) (read-stats res tcp-in)]
  [stats-tube (tube) (read-stats res tcp-in)]
  [stats () (read-stats res tcp-in)]
  [list-tubes () (read-stats res tcp-in)]
  [list-tube-used ()]
  [list-tubes-watched () (read-stats res tcp-in parse-yaml-list)]
  [quit ()]
  [pause-tube (tube delay)])

)
