# storm_basics
Apache Storm basic examples


## RollingCountBolt (storm-starter)
* Good explanation is provided on github location of this bolt (src/jvm/main/storm/starter/bolt/RollingCountBolt.java)
* 1st arg : length of sliding window in seconds
* 2nd arg : emit frequency in seconds
* declared output fields
  1. __"obj"__ : Object whose rolling count is maintained
  1. __"count"__ : Latest rolling count
  1. __"actualWindowLengthInSeconds"__ : Actual duration of rolling count
* Example output
  ```
  2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [pubicgirth, 810, 9]
  2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [oxinterfere, 843, 9]
  2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [signsunlucky, 848, 9]
  2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [immodestbrunnich, 893, 9]
  2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [rockperturb, 834, 9]
  2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [eggclopping, 850, 9]
  ```
  __NOTE :__ Window count above is logged as 9 even though it was set to 10. This is expected due to heavy load
