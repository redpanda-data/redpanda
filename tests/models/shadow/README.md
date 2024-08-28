This is a P model for L0-only commit protocol, plus fetch. The architecture
covered by the model is depicted in the diagram below.

```
                                L0d Object                                   
                                    |                                        
                                    |                                        
                                    |                                        
                                    |                                        
                                    |                                        
                            +-------|------+                                 
                            |              |                                 
+------------+  Produce/Ack |              |                                 
|            | --------------              |   Placeholder append            
|   Client   |              |    Broker    ------------------------ Partition
|            | --------------              |                                 
+------------+     Fetch    |              |                                 
                            |              |                                 
                            +--------------+                                 
```

Notably absent from this model are:

* Reconciliation
* Leveling
* Recovery
* Garbage collection
* Failures
