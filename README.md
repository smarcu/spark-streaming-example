# spark-streaming-example

An example of spark processing where a worker dynamically reads configuration from a file (/tmp/filters.cfg)

When the config files contains the word enabled, the processing applies a multiplication operation on values.

While running, update the file /tmp/filters.cfg (add or remove the enabled word) and see the processing 
adapting accordingly (values are *10 or not)



