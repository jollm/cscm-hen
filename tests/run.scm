
(import chicken scheme)
(use hen test)

(define command-list-0 (list hen-peek-ready
                             hen-peek-delayed
                             hen-peek-buried
                             hen-stats
                             hen-list-tube-used
                             hen-list-tubes
                             hen-list-tubes-watched
                             hen-quit))

(define command-list-1 (list hen-stats-job
                             hen-stats-tube
                             hen-kick
                             hen-peek
                             hen-ignore
                             hen-watch
                             hen-touch
                             hen-delete
                             hen-use
                             hen-put))

(define command-list-2 (list hen-pause-tube
                             hen-bury))

(define command-list-3 (list hen-release))

(test-begin "all commands with dummy data")

(test "zero param commands"
      '("OK 13" "OK 13" "OK 13" (("test" . "data")) "OK 13" (("test" . "data")) ("st:data") "OK 13")
      (map (lambda (p) (p #:tcp-in (open-input-string "OK 13\r\n---\ntest:data\r\n") #:tcp-out (open-output-string))) command-list-0))

(test "single param commands"
      '((("test" . "data")) (("test" . "data")) "OK 13" "OK 13" "OK 13" "OK 13" "OK 13" "OK 13" "OK 13" "OK 13")
      (map (lambda (p) (p "tube" #:tcp-in (open-input-string "OK 13\r\n---\ntest:data\r\n") #:tcp-out (open-output-string))) command-list-1))

(test "two param commands"
      '("OK 13" "OK 13")
      (map (lambda (p) (p 20 20 #:tcp-in (open-input-string "OK 13\r\n---\ntest:data\r\n") #:tcp-out (open-output-string))) command-list-2))

(test "three param commands"
      '("OK 13")
      (map (lambda (p) (p 20 20 20 #:tcp-in (open-input-string "OK 13\r\n---\ntest:data\r\n") #:tcp-out (open-output-string))) command-list-3))

(test "reserve"
      "OK 13"
      (hen-reserve 0 #:tcp-in (open-input-string "OK 13\r\n---\ntest:data\r\n") #:tcp-out (open-output-string)))

(test-end)

(test-exit)
