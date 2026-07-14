# urirun-connector-watchdog

`watch://` — wybudza urirun gdy pętla stoi: wykrywa zapętlenia/stagnację koru z logu,
diagnozuje rootcause (needs_input / no_executor / env / stalled), przerywa jałową pętlę
(`ticket→blocked` + notatka) i eskaluje do panelu / `human://`.
