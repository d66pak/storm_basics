# storm_basics
Apache Storm basic examples


---
[Michael G. Noll - Implementing Real-Time Trending Topics][1]


### RollingCountBolt (storm-starter)
* Good explanation is provided on github location of this bolt (_src/jvm/main/storm/starter/bolt/RollingCountBolt.java_)
* Also read some explanation about this bolt in [Michael G. Noll's blog][1]
* 1st arg : length of sliding window in seconds
* 2nd arg : emit frequency in seconds
* declared output fields
  1. __"obj"__ : Object whose rolling count is maintained
  1. __"count"__ : Latest rolling count
  1. __"actualWindowLengthInSeconds"__ : Actual duration of rolling count
* Example output ...


   ```
   2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [pubicgirth, 810, 9]
   2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [oxinterfere, 843, 9]
   2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [signsunlucky, 848, 9]
   2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [immodestbrunnich, 893, 9]
   2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [rockperturb, 834, 9]
   2015-06-22T14:57:53.383+0900 b.s.d.task [INFO] Emitting: rolling_counter_bolt default [eggclopping, 850, 9]
   ```
  ...
  __NOTE :__ Window count above is logged as 9 even though it was set to 10. This is expected due to heavy load
* Constructor
  * Initializes _windowLengthInSeconds_ and _emitFrequencyInSeconds_
  * Initializes _SlidingWindowCounter_
* NthLastModifiedTimeTracker
  This is only required to find out if actual window length is less that given.
  Its only required for warning purpose.
  * Maintains _CircularFifoBuffer_ of given window size and initializes all the slots with current time in milli sec.
  * 

* This bolt uses __tick tuple__ to receive triggers at regular intervals
  ```java
  @Override
  public Map<String, Object> getComponentConfiguration() {
      Map<String, Object> conf = new HashMap<String, Object>();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
      return conf;
  }
  ```
  ...
  _emitFrequencyInSeconds_ is member variable.

---



[1]: http://www.michael-noll.com/blog/2013/01/18/implementing-real-time-trending-topics-in-storm/
